# Chat — events and outbound send command

> Owner: @n1rmata

Living spec of the `chat` family: the inbound fact (`ChatMessageEvent`, host → platform) and the
outbound command (`SendChatMessageCommand`, platform → host).

**Counterpart specs (cross-repo):**

- `nx-gameservers/docs/specs/073-chat.md` — the platform side: ingest, storage, retention, read API,
  SSE stream, announcement-scheduler cutover.
- `nx-infra/docs/specs/031-sse-streaming.md` — the SSE transport canon the platform read side obeys.

## Problem

Platform-side moderation and anti-RMT need to see in-game chat. Every chat line used to stay inside
the game-server JVM — operators could not review what was said, the platform could not flag RMT /
gold-seller spam, scam offers or abusive language, and there was no durable per-character chat
history. The detection logic is a platform concern; the host only ships the raw fact.

The reverse direction is the second half of the same feature. Writing into game chat from outside is
today impossible except for one narrow path — `AnnounceNowCommand`, which broadcasts to the
announcement channel and nothing else. The mini app needs to post into clan chat as the player's own
character (including while that character is offline), and announcements need to reach a player's
private chat under an arbitrary display name ("System", "Дед Мороз").

Audience: platform-side consumers (moderation / anti-RMT, the mini app); host-side authors hooking
the chat handler path and registering the command handler.

## Requirements — inbound event

> Sibling feature carrying the wire dispatch plumbing:
> [`messaging`](008-messaging.md) — `MessagingTopics.events.<family>` topic addressing,
> `Nx-Server-Id` connection-scoped header, `Nx-Message-Type` per-record header, UUIDv7 idempotency.
> UNCHANGED by this slice.

**Must:**

- [done] R1. `nx-gs-adapter-api.kafka.events.chat.ChatMessageEvent` MUST ship as the single concrete
  event of the `chat` family (no abstract base). Final Java-8 POJO + hand-written builder +
  `equals`/`hashCode`/`toString`; constructor parameter names preserved (`-parameters`) for Gson /
  Jackson parameter-name deserialization. Fields:
  - `UUID eventId` — REQUIRED. UUIDv7; the upper 48 bits encode the message occurrence timestamp —
    consumers extract `occurredAt` via `UUIDv7.extractCreatedAt(eventId)` and dedupe on the id
    (at-least-once delivery). No separate `occurredAt` field. Null-checked in the constructor.
  - `long charId` — REQUIRED. Sender object id; also the partition key (8-byte big-endian) so one
    sender's messages stay in occurrence order on a single partition.

    > Naming: the platform canon is `characterId`, spelled out. This field keeps `charId` because
    > it is already released and in production — renaming a wire field breaks the rail for no
    > functional gain. New fields on this family follow the canon; the platform maps names at its
    > consumer boundary.

  - `@Nullable String charName` — sender display name; OPTIONAL.
  - `String channel` — REQUIRED. A `WellKnownChatChannels` code, or the raw string `UNKNOWN_<int>`
    for a build-specific channel this catalog does not yet name. Null-checked in the constructor.
  - `String text` — REQUIRED. Message body, already sanitized host-side. Null-checked in the
    constructor.
  - `@Nullable Long targetCharId` / `@Nullable String targetCharName` — whisper recipient.
    Populated ONLY on the `WHISPER` channel; both `null` on every other channel. `targetCharId` is
    `null` when the recipient is offline / unresolvable, while `targetCharName` may still carry the
    typed recipient name.
  - `@Nullable Map<String, String> metadata` — OPTIONAL open string→string map of build-agnostic
    attributes. Hosts MAY add arbitrary keys without an api release; consumers ignore unknown keys.
    `null` when absent; normalized to an unmodifiable copy when present.

- [done] R2. `nx-gs-adapter-api.kafka.events.chat.WellKnownChatChannels` MUST ship the canonical
  `UPPER_SNAKE_CASE` open-string channel vocabulary used as the `channel` value. A host maps its
  build-specific numeric chat type onto one of these codes; an exposed-but-unnamed channel is
  published as the raw string `UNKNOWN_<int>` (the platform still routes it but cannot aggregate it
  canonically). Adding a constant is a non-breaking minor-version change. Shipped codes:

  `GENERAL`, `SHOUT`, `WHISPER`, `PARTY`, `CLAN`, `ALLIANCE`, `TRADE`, `WORLD`, `HERO`, `GM`,
  `PETITION`, `PETITION_GM`, `ANNOUNCEMENT`, `CRITICAL_ANNOUNCEMENT`, `SCREEN_ANNOUNCEMENT`,
  `BATTLEFIELD`, `BOAT`, `FRIEND`, `MSN`, `PARTY_ROOM`, `COMMAND_CHANNEL`,
  `COMMAND_CHANNEL_COMMANDER`, `NPC_GENERAL`, `NPC_SHOUT`, `NPC_WHISPER`.

- [done] R3. `nx-gs-adapter-core.events.EventTypeRegistry` MUST register `ChatMessageEvent`: family
  `"chat"`, message-type `"ChatMessageEvent"`, partition-key extractor returning `charId` (8-byte
  big-endian). Dispatched through the existing generic `NxEvents.publish(Object)` path.

- [done] R4. The platform `/connect` response MUST advertise the chat topic: a `"chat"` entry in
  `ConnectResponse.messagingTopics.events`, resolving to `<tenant-slug> + ".gs.events.chat"`.

- [done] R6. The host (`bohpts-core`) MUST hook its chat-handler path and publish one
  `ChatMessageEvent` per player-typed message via the cached `NxEvents` facade. Sanitize `text`
  host-side; map the build's numeric chat type to a `WellKnownChatChannels` code (or
  `UNKNOWN_<int>`); set `targetCharId` / `targetCharName` only for whispers. Any uncaught `Throwable`
  in the publish path is caught and logged, never propagated to the game thread.

- [todo] R5. The platform MUST run a consumer over `<tenant>.gs.events.chat`. Partition key is the
  sender id; retention follows the platform-wide event-topic default — long-term moderation history
  is a consumer-side concern, not Kafka's. Design: `nx-gameservers/docs/specs/073-chat.md`.

- [todo] R7. On the `CLAN` and `ALLIANCE` channels the host SHOULD carry the speaker's clan id in
  `metadata` under the key `clanId`. The platform scopes clan-chat reads by it. Resolving the clan
  from the platform's own replica instead is wrong: the replica lags, so a message from a character
  who just left the clan lands in the wrong scope, while the host knows the clan at the moment of
  speaking. No api release is needed — `metadata` is the open map R1 provides for exactly this.

## Requirements — outbound command

- [todo] R8. `nx-gs-adapter-api.kafka.commands.chat.SendChatMessageCommand` MUST ship as the single
  generic "put this text into game chat" command, implementing `NxCommand<SendChatMessageResult>`.
  Per-command wire contract lives in [`009-commands/catalog.md`](009-commands/catalog.md); the
  design decisions behind its shape are here.

  **Sender is two independent things, and the contract keeps them apart.** `senderCharacterId` is who
  speaks legally — it drives the host's gates, the packet's `objectId` and the platform's attribution.
  `senderDisplayName` is what the client renders. A mini-app message carries both (character plus its
  own name with a suffix); a "Дед Мороз" announcement carries only the second. Collapsing them into a
  single field cannot express either the persona or the audit trail.

  **The display name is composed platform-side, in full.** The host writes the string as given —
  `CreatureSay` serializes the sender name with `writeS`, so an arbitrary string renders without any
  client change. Keeping composition on the platform means the suffix format changes without an
  adapter or game-core release. This relies on the standing rule that the adapter trusts the platform.

  **`audience` is an axis of its own**, orthogonal to `channel`: `CHARACTER` | `CLAN` | `ALL_ONLINE`.
  A whisper to one player and a whisper fanned out to everyone online are the same frame with
  different recipient lists. Without the axis, DM announcements would need a second command carrying a
  copy of every field.

  **`messageId` (UUIDv7) is supplied by the platform and becomes the `eventId` of the echo event**
  (R9). Re-issuing the command after a reply timeout therefore converges on one stored row instead of
  two. The host is expected to keep a bounded window of seen ids; without that window the field exists
  but the property does not.

  **Origin travels on the command, not by inference.** `source` is required and echoed into the
  event metadata: the host has no way to tell which platform surface issued a message, and
  without the marker the stored corpus cannot separate platform traffic from what players typed
  in-game — which is the distinction every abuse query starts from.

  **Accepted channels are a whitelist that grows per slice.** The first slice accepts `CLAN` (mini-app clan chat) and `ANNOUNCEMENT` (the absorbed announcement path);
  anything outside the current whitelist is answered `VALIDATION_FAILED`. `CRITICAL_ANNOUNCEMENT` is
  deliberately absent — see R11.

- [todo] R9. The host handler MUST publish a `ChatMessageEvent` for every message it sends, reusing
  `messageId` as `eventId` and marking origin in `metadata`. Otherwise platform-originated messages
  never reach the chat table, the RMT corpus, or the live stream other clan members read, and chat
  ends up with two sources of truth.

- [todo] R10. The host handler MUST run the same gates as the native chat handler for the target
  channel — for `CLAN` that is `isChatBanned` + `Config.BAN_CHAT_CHANNELS`, the shadow-ban check, and
  the academy level floor. Skipping them turns the command into a chat-ban bypass. An offline speaker
  is resolved through the clan table rather than a live `Player`, and the clan broadcast reaches the
  online members.

## Absorbing `AnnounceNowCommand`

Announcements are not a separate subsystem: `Broadcast.announceToOnlinePlayers` builds
`new CreatureSay(0, 10, "", text)` and sends it to every online player — the same packet class the
chat handlers use. In command terms that is exactly `senderCharacterId: null`,
`senderDisplayName: ""`, `channel: ANNOUNCEMENT`, `audience: ALL_ONLINE`. So `SendChatMessageCommand`
supersedes `AnnounceNowCommand` rather than living beside it.

- [todo] R11. The `critical` flag MUST NOT be carried over. It is visually near-worthless on the
  bohpts client, the front-end already always sends `critical: false`, and its only channel
  (`CreatureSay` type 18) is the one where the clickable-link token `[=url=]` renders literally.
  Dropping it removes the trap along with the flag. If a real need appears later, it comes back as its
  own change.

- [todo] R12. Text handling (`\n` split into physical lines, wrapping bare `http(s)://` URLs in the
  host's clickable-link token, trimming trailing punctuation out of the wrapper) MUST move into the
  new handler, and `AnnounceNowHandler` MUST be rewritten as a thin delegate over it. Then the two
  paths agree by construction rather than by reviewer attention.

**Rollout is constrained by the host's release cadence: the platform deploys at any time, the game
core's jar applies only at the morning restart.** Announcements must not break in the window where the
platform is new and the host is old.

`CommandStatus.UNSUPPORTED_COMMAND` already covers it — the dispatcher replies with it when no handler
is registered for the `Nx-Message-Type`. That is an explicit, fast, per-server negative.

> Considered and rejected: feature detection through `CommandsStats.registeredTypes`, which rides the
> heartbeat topic. No platform service consumes heartbeats at all, so this would mean standing up a
> consumer for a whole family to read one flag.

- [todo] R13. **Phase 1 (expand).** The platform's announcement scheduler sends the new command and,
  on an `UNSUPPORTED_COMMAND` reply, immediately re-sends the legacy `AnnounceNowCommand`. The
  fallback is counted by a metric. `AnnounceNowHandler` stays registered host-side.
- [todo] R14. **Phase 2 (contract).** Once the fallback metric reads zero across every server for
  several consecutive days — the trigger is the metric, not a date — the fallback, `AnnounceNowHandler`,
  `AnnounceNowCommand` and `AnnounceResult` are all removed. Naming the trigger is what makes the
  compatibility layer a phase instead of a permanent straddle.

## Topic & wire summary

| Item              | Value                                                    |
| ----------------- | -------------------------------------------------------- |
| Family            | `chat`                                                   |
| Event topic       | `<tenant>.gs.events.chat` (e.g. `bohpts.gs.events.chat`) |
| `Nx-Message-Type` | `ChatMessageEvent`                                       |
| Partition key     | `charId` (8-byte big-endian)                             |
| Idempotency       | `eventId` (UUIDv7), at-least-once delivery               |
| Command topic     | `<tenant>.gs.commands` (shared, routed by message type)  |
| `Nx-Message-Type` | `SendChatMessageCommand`                                 |
| Idempotency       | `messageId` (UUIDv7), at-most-once delivery              |

## Compatibility

The event half is purely additive and already released — `ChatMessageEvent` +
`WellKnownChatChannels` in `api/v0.67.0`, the registry binding in `core/v0.32.0`.

The command half is additive on release: a host built against an older api simply never registers the
handler, and the platform sees `UNSUPPORTED_COMMAND` — which is exactly the signal phase 1 relies on.
The removal in R14 is the only breaking step, and it is gated on the fallback metric.

## Non-goals

- **Adapter-side moderation.** The adapter ships the raw fact only.
- **NPC / system chat as a separate stream.** `NPC_*` and announcement channels ride the same family;
  consumers filter by `channel`.
- **Chat editing / deletion semantics.** Events are append-only facts; there is no retraction message.
- **Delivery to offline recipients.** A whisper needs a live `Player` to receive the packet; an
  offline inbox with catch-up delivery is a platform-side design, not a wire concern.

## Links

- Sibling reference (host-push publisher pattern + registry binding):
  [`docs/specs/011-events-online-snapshot.md`](011-events-online-snapshot.md)
- Wire dispatch plumbing: [`docs/specs/008-messaging.md`](008-messaging.md)
- Command runtime + handler authoring: [`docs/specs/009-commands/spec.md`](009-commands/spec.md),
  [`guide.md`](009-commands/guide.md), [`catalog.md`](009-commands/catalog.md)
