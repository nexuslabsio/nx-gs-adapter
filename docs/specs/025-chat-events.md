# Chat — events

> Owner: @n1rmata

Living spec of the `chat` family's inbound fact (`ChatMessageEvent`, host → platform). The outbound
command (`SendChatMessageCommand`, R8–R15) lives in
[`009-commands/send-chat-message.md`](009-commands/send-chat-message.md).

**Counterpart specs (cross-repo):**

- `nx-gameservers/docs/specs/073-chat.md` — the platform side: ingest, storage, retention, read API,
  SSE stream, announcement-scheduler cutover.
- `nx-infra/docs/specs/031-sse-streaming.md` — the SSE transport canon the platform read side obeys.

## Problem

Platform-side moderation and anti-RMT need to see in-game chat. Every chat line used to stay inside
the game-server JVM — operators could not review what was said, the platform could not flag RMT /
gold-seller spam, scam offers or abusive language, and there was no durable per-character chat
history. The detection logic is a platform concern; the host only ships the raw fact.

Audience: platform-side consumers (moderation / anti-RMT); host-side authors hooking the chat handler
path.

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

- [done] R5. The platform MUST run a consumer over `<tenant>.gs.events.chat`. Partition key is the
  sender id; retention follows the platform-wide event-topic default — long-term moderation history
  is a consumer-side concern, not Kafka's. Design: `nx-gameservers/docs/specs/073-chat.md`.

- [done] R7. On the `CLAN` and `ALLIANCE` channels the host SHOULD carry the speaker's clan id in
  `metadata` under the key `clanId`. The platform scopes clan-chat reads by it. Resolving the clan
  from the platform's own replica instead is wrong: the replica lags, so a message from a character
  who just left the clan lands in the wrong scope, while the host knows the clan at the moment of
  speaking. No api release is needed — `metadata` is the open map R1 provides for exactly this.

## Topic & wire summary

| Item              | Value                                                    |
| ----------------- | -------------------------------------------------------- |
| Family            | `chat`                                                   |
| Event topic       | `<tenant>.gs.events.chat` (e.g. `bohpts.gs.events.chat`) |
| `Nx-Message-Type` | `ChatMessageEvent`                                       |
| Partition key     | `charId` (8-byte big-endian)                             |
| Idempotency       | `eventId` (UUIDv7), at-least-once delivery               |

## Compatibility

Purely additive and already released — `ChatMessageEvent` + `WellKnownChatChannels` in
`api/v0.67.0`, the registry binding in `core/v0.32.0`.

## Non-goals

- **Adapter-side moderation.** The adapter ships the raw fact only.
- **NPC / system chat as a separate stream.** `NPC_*` and announcement channels ride the same family;
  consumers filter by `channel`.
- **Chat editing / deletion semantics.** Events are append-only facts; there is no retraction message.

## Links

- Sibling reference (host-push publisher pattern + registry binding):
  [`docs/specs/011-events-online-snapshot.md`](011-events-online-snapshot.md)
- Wire dispatch plumbing: [`docs/specs/008-messaging.md`](008-messaging.md)
- Outbound command: [`009-commands/send-chat-message.md`](009-commands/send-chat-message.md)
