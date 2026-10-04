# Chat — events

> Owner: @n1rmata

Living spec of the `chat` family's inbound fact (`ChatMessageEvent`, host → platform). The outbound
command (`SendChatMessageCommand`, R8–R15 and R22–R27) lives in
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

  - `@Nullable String charName` — clean sender character name (no offline postfix); OPTIONAL.
  - `@Nullable String senderDisplayName` — `[planned]` name as the game client renders it, e.g.
    `Vasya*`; see R16.
  - `String channel` — REQUIRED. A `WellKnownChatChannels` code, or the raw string `UNKNOWN_<int>`
    for a build-specific channel this catalog does not yet name. Null-checked in the constructor.
  - `String text` — REQUIRED. Message body, already sanitized host-side. Null-checked in the
    constructor.
  - `@Nullable Long targetCharId` / `@Nullable String targetCharName` — whisper recipient.
    Populated ONLY on the `WHISPER` channel; both `null` on every other channel. `targetCharId` is
    `null` only when the recipient cannot be resolved at all; an offline addressee is resolved by
    name (R20), and `targetCharName` carries the typed recipient name.
  - `@Nullable Map<String, String> metadata` — OPTIONAL open string→string map of build-agnostic
    attributes. Hosts MAY add arbitrary keys without an api release; consumers ignore unknown keys.
    `null` when absent; normalized to an unmodifiable copy when present.
  - `@Nullable List<Long> recipientCharacterIds` — `[planned]`, see R17.
  - `@Nullable List<ChatItemSnapshot> items` — `[planned]`, see R18.

  The three `[planned]` fields are additive: a host built before them omits them (they read as
  `null`), and consumers MUST tolerate their absence.

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

- [done] R6. The host MUST hook its chat-handler path and publish one
  `ChatMessageEvent` per player-typed message via the cached `NxEvents` facade — and, `[planned]`
  (R19), only after the channel handler actually delivered it. Sanitize `text`
  host-side; map the build's numeric chat type to a `WellKnownChatChannels` code (or
  `UNKNOWN_<int>`); set `targetCharId` / `targetCharName` only for whispers. Any uncaught `Throwable`
  in the publish path is caught and logged, never propagated to the game thread.

- [done] R5. The platform MUST run a consumer over `<tenant>.gs.events.chat`. Partition key is the
  sender id; retention follows the platform-wide event-topic default — long-term moderation history
  is a consumer-side concern, not Kafka's. Design: `nx-gameservers/docs/specs/073-chat.md`.

- [done] R7. On the `CLAN` and `ALLIANCE` channels the host SHOULD carry the speaker's clan id in
  `metadata` under the key `clanId` (the alliance id travels under its own key, R21). The platform scopes clan-chat reads by it. Resolving the clan
  from the platform's own replica instead is wrong: the replica lags, so a message from a character
  who just left the clan lands in the wrong scope, while the host knows the clan at the moment of
  speaking. No api release is needed — `metadata` is the open map R1 provides for exactly this.

- [planned] R16. `ChatMessageEvent.senderDisplayName` MUST carry the name exactly as the game client
  renders it. The host appends `*` (no space) when the speaking character is not in game (offline or
  offline-trader mode); `charName` stays the clean character name. The postfix format is owned by the
  host, not the platform: it is also a player-facing protocol (an in-game whisper to `Vasya*` routes
  to the offline character, R20), so it must not depend on a platform deploy. `null` from hosts that
  predate the field; consumers fall back to `charName`.

- [planned] R17. `ChatMessageEvent.recipientCharacterIds` MUST be filled only for `GENERAL` and
  `PARTY` (`null` elsewhere): the characters that actually received the packet, plus the speaker.
  Party has no stable id and the `GENERAL` audience is positional, so the recipient list is the only
  way to scope those channels on the read side. Clan / alliance scoping keeps using metadata ids.

- [planned] R18. `ChatMessageEvent.items` MUST hold an immutable `ChatItemSnapshot` for every item link
  present in `text`, one per objectId, taken at send time: the item's later state (enchant, augment,
  owner) cannot be reconstructed through chat. New wire class
  `nx-gs-adapter-api.kafka.events.chat.ChatItemSnapshot` (Java-8 POJO, same conventions as R1):
  - `long itemObjectId`, `int itemTemplateId`, `int enchantLevel`
  - `Map<String, Integer> attributes` — elemental type (`FIRE` / `WATER` / `WIND` / `EARTH` / `HOLY` /
    `DARK`) to power; empty when none, never `null`
  - `@Nullable ItemAugmentationDbDto augmentation` = `{int option1Id; @Nullable Integer option2Id}` — the
    item-sync class itself (`kafka.sync.db.item`), not a chat copy: one domain concept, one wire shape

  No ensoul: not supported on this build, and a special ability is a separate item template.
  Augmentation option localization is out of scope (a future platform option catalog, nx-gamedata
  `TODO §6`).

  **`text` stays in the game's native form in both directions.** Item links are
  tokens (`\b\tType=1 \tID=<objectId> ...\b`).
  Normalizing them to any platform format is the platform's job: the DTO format must not depend on
  host deploys. The objectId on the wire is fine.

- [planned] R19. **Publish semantics.** The host MUST publish the event only after the channel handler
  actually delivered the message; a message the handler rejected (ban, level floor, block list, ...)
  produces no event. A contract, not an implementation detail: the stored corpus then equals what
  players could have read.

- [planned] R20. `WHISPER`: `targetCharId` MUST be set also when the addressee is offline; the host
  resolves by name, including the `Name*` form (R16).

- [planned] R21. `ChatMetadataKeys.ALLIANCE_ID = "allianceId"` MUST carry the speaker's alliance id as a
  decimal string on the `CLAN` and `ALLIANCE` channels, same rationale as R7. Naming follows the
  platform canon (full words: alliance, character); the shipped `charId` / `targetCharId` /
  `targetCharName` are NOT renamed.

- [planned] R21a. `ChatMetadataKeys.SHADOWED = "shadowed"` (`"true"`) MUST mark a message the host
  delivered only to the speaker (shadow-ban, broadcast filter). Such a message is still published
  (R19 counts it as delivered — it is exactly the spam the corpus exists for); `recipientCharacterIds`
  is `[speaker]` on `GENERAL` / `PARTY`. Consumers store it but show it only to the speaker.

> Host-internal, not wire: the host keeps a bounded snapshot cache (objectId -> item info, TTL 6h,
> re-link refreshes) so in-game players can open links sent by offline characters.

## Topic & wire summary

| Item              | Value                                      |
| ----------------- | ------------------------------------------ |
| Family            | `chat`                                     |
| Event topic       | `<tenant>.gs.events.chat`                  |
| `Nx-Message-Type` | `ChatMessageEvent`                         |
| Partition key     | `charId` (8-byte big-endian)               |
| Idempotency       | `eventId` (UUIDv7), at-least-once delivery |

## Compatibility

Purely additive and already released — `ChatMessageEvent` + `WellKnownChatChannels` in
`api/v0.67.0`, the registry binding in `core/v0.32.0`. R16-R21 are additive on top of that: new
nullable event fields, one new wire class, one new metadata key; old hosts omit them and consumers
tolerate absence. The one behavioral change is R19 (publish after delivery).

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
