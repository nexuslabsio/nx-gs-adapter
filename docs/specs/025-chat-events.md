# Events — Chat message

> Owner: @n1rmata

## Problem

Platform-side moderation and anti-RMT need to see in-game chat. Today every chat
line stays inside the game-server JVM — operators cannot review what was said,
the platform cannot flag RMT / gold-seller spam, scam offers, or abusive
language, and there is no durable per-character chat history. The detection
logic (spam patterns, RMT heuristics, profanity) is a platform concern; the
host only needs to ship the raw fact.

The existing event families (`premiumpurchase`, `serveronline`, `character`,
`raid`, …) already prove the host-push wire pattern: per-family Kafka topic,
UUIDv7 `eventId`, `Nx-Message-Type` header dispatch, `NxEvents.publish(...)`
SPI, game-loop-safe bounded-queue enqueue. Chat is the next family to ride that
rail — one event per player-typed message, no pattern matching adapter-side.

Audience: platform-side consumers (moderation / anti-RMT analysis); host-side
authors hooking the chat-handler path.

## Requirements

> Sibling feature carrying the wire dispatch plumbing:
> - [`messaging`](008-messaging/spec.md) — `MessagingTopics.events.<family>` topic
    > addressing, `Nx-Server-Id` connection-scoped header, `Nx-Message-Type`
    > per-record header, UUIDv7 idempotency. UNCHANGED by this slice.

**Must:**

- [done] R1. `nx-gs-adapter-api.kafka.events.chat.ChatMessageEvent` MUST ship as
  the single concrete event of the `chat` family (no abstract base). Final
  Java-8 POJO + hand-written builder + `equals`/`hashCode`/`toString`;
  constructor parameter names preserved (`-parameters`) for Gson / Jackson
  parameter-name deserialization. Fields:
    - `UUID eventId` — REQUIRED. UUIDv7; the upper 48 bits encode the message
      occurrence timestamp — consumers extract `occurredAt` via
      `UUIDv7.extractCreatedAt(eventId)` and dedupe on the id
      (at-least-once delivery). No separate `occurredAt` field. Null-checked in
      the constructor.
    - `long charId` — REQUIRED. Sender object id; also the partition key
      (8-byte big-endian) so one sender's messages stay in occurrence order on a
      single partition.
    - `@Nullable String charName` — sender display name; OPTIONAL.
    - `String channel` — REQUIRED. A `WellKnownChatChannels` code, or the raw
      string `UNKNOWN_<int>` for a build-specific channel this catalog does not
      yet name. Null-checked in the constructor.
    - `String text` — REQUIRED. Message body, already sanitized host-side.
      Null-checked in the constructor.
    - `@Nullable Long targetCharId` / `@Nullable String targetCharName` —
      whisper recipient. Populated ONLY on the `WHISPER` channel; both `null` on
      every other channel. `targetCharId` is `null` when the recipient is
      offline / unresolvable, while `targetCharName` may still carry the typed
      recipient name.
    - `@Nullable Map<String, String> metadata` — OPTIONAL open string→string map
      of build-agnostic attributes (e.g. `rawType` — the build's numeric chat
      type, room id). Hosts MAY add arbitrary keys without an api release;
      consumers ignore unknown keys. `null` when absent; normalized to an
      unmodifiable copy when present.

- [done] R2. `nx-gs-adapter-api.kafka.events.chat.WellKnownChatChannels` MUST
  ship the canonical `UPPER_SNAKE_CASE` open-string channel vocabulary used as
  the `channel` value. A host maps its build-specific numeric chat type onto one
  of these codes; an exposed-but-unnamed channel is published as the raw string
  `UNKNOWN_<int>` (the platform still routes it but cannot aggregate it
  canonically). Adding a constant is a non-breaking minor-version change.
  Shipped codes:

  `GENERAL`, `SHOUT`, `WHISPER`, `PARTY`, `CLAN`, `ALLIANCE`, `TRADE`, `WORLD`,
  `HERO`, `GM`, `PETITION`, `PETITION_GM`, `ANNOUNCEMENT`,
  `CRITICAL_ANNOUNCEMENT`, `SCREEN_ANNOUNCEMENT`, `BATTLEFIELD`, `BOAT`,
  `FRIEND`, `MSN`, `PARTY_ROOM`, `COMMAND_CHANNEL`, `COMMAND_CHANNEL_COMMANDER`,
  `NPC_GENERAL`, `NPC_SHOUT`, `NPC_WHISPER`.

- [done] R3. `nx-gs-adapter-core.events.EventTypeRegistry` MUST register
  `ChatMessageEvent`: family `"chat"`, message-type `"ChatMessageEvent"`,
  partition-key extractor returning `charId` (8-byte big-endian). Dispatched
  through the existing generic `NxEvents.publish(Object)` path — appending the
  one `register(...)` call is the only core change.

- [todo] R4. The platform `/connect` response MUST advertise the chat topic:
  add a `"chat"` entry to `ConnectResponse.messagingTopics.events`, resolving to
  `<tenant-slug> + ".gs.events.chat"`. The adapter reads its wire address from
  this field — no other config change adapter-side.

- [todo] R5. The platform MUST provision the per-tenant Kafka topic
  `<tenant>.gs.events.chat` and run a consumer. Partition key is `charId`;
  retention follows the platform-wide event-topic default (long-term moderation
  history is a consumer-side concern, not Kafka's).

- [todo] R6. The host (bohpts-core) MUST hook its chat-handler path and publish
  one `ChatMessageEvent` per player-typed message via the cached `NxEvents`
  facade. Sanitize `text` host-side; map the build's numeric chat type to a
  `WellKnownChatChannels` code (or `UNKNOWN_<int>`); set `targetCharId` /
  `targetCharName` only for whispers. Any uncaught `Throwable` in the publish
  path is caught and logged, never propagated to the game thread.

## Topic & wire summary

| Item              | Value                                                    |
|-------------------|----------------------------------------------------------|
| Family            | `chat`                                                   |
| Topic             | `<tenant>.gs.events.chat` (e.g. `bohpts.gs.events.chat`) |
| `Nx-Message-Type` | `ChatMessageEvent`                                       |
| Partition key     | `charId` (8-byte big-endian)                             |
| Idempotency       | `eventId` (UUIDv7), at-least-once delivery               |

## Compatibility

Purely additive. `ChatMessageEvent` + `WellKnownChatChannels` are new types in
`nx-gs-adapter-api` (released as `api/v0.67.0`); the registry binding ships in
`nx-gs-adapter-core` (`core/v0.32.0`). No existing wire shape changes. A host
on an older api jar simply never publishes the family; the platform sees no
`chat` topic traffic until the host is rebuilt against the new api + the
`/connect` response advertises the topic.

## Non-goals

- **Adapter-side moderation.** The adapter ships the raw fact only — spam / RMT /
  profanity detection is entirely platform-side.
- **NPC / system chat as a separate stream.** `NPC_*` and announcement channels
  ride the same family; consumers filter by `channel`.
- **Chat editing / deletion semantics.** Events are append-only facts; there is
  no retraction message.

## Links

- Sibling reference (host-push publisher pattern + registry binding):
  [`docs/specs/011-events-online-snapshot/spec.md`](011-events-online-snapshot/spec.md)
- Wire dispatch plumbing: [`docs/specs/008-messaging/spec.md`](008-messaging/spec.md)
