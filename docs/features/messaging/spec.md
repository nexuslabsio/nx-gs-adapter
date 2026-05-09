# Messaging — Events Outbound + Commands Inbound

## Problem

The L2 game-server core needs a structured, dev-friendly way to (a) signal the platform about
discrete in-game facts (premium purchases, character lifecycle, clan events, periodic server
snapshots) and (b) receive structured commands from the platform's web side (rename, kick,
mail, currency-pay, character-transfer, …). The legacy `bohpts-core / l2e.gameserver.infrastructure.rabbitMq`
implementation couples handlers directly to game-server internals (`GameObjectsStorage`,
`MailManager`, `CharacterDAO`, …), maintains 58 hand-rolled DTOs without domain grouping,
auto-acks before handler execution (drops messages on handler failure), runs handlers on the
RabbitMQ consumer thread (no game-loop hop, races against game state mutations), and carries
no tenant / server identity in the envelope.

This slice introduces two surfaces in `nx-gs-adapter`:

- **Outbound events** — adapter-core capability `NxEvents` for in-game-fact fanout to per-family
  Kafka topics. Phase 1 ships `events.premiumpurchase` end-to-end with `PremiumPurchaseEvent` modelling
  combined item+service purchases with per-line multi-currency payments. Topic slots for
  `events.character` / `events.clan` / `events.server` are provisioned in the wire contract
  but the concrete subtypes are deferred.
- **Inbound commands (architectural sketch only)** — the wire shape (`<tenant>.gs.commands.<domain>`
  per-business-domain topics, `Nx-Message-Type` + `Nx-Correlation-Id` headers, `CommandResultEvent`
  reply via the events stream) and the high-level SPI hook are committed in Javadoc placeholders;
  the runtime consumer + dispatch implementation is Phase 2.

Audience: bohpts-core (and future per-tenant) integration code that wants to publish premium
events without touching Kafka directly; platform-side consumers of `gs.events.*`; future
command-handler authors across `char` / `clan` / `mail` / `account` domains.

## Requirements

> **Sibling features carry the SPI plumbing and topic delivery contract:**
> - Tier-1 SPI (`AdapterModule` + ServiceLoader) lives in
    > [`adapter-modules`](../adapter-modules/spec.md). Phase 1 messaging is **not**
    > a discovered Tier-1 module — it lives inside `nx-gs-adapter-core` as a built-in
    > capability surfaced through `ConnectContext`.
> - Existing `Nx-Server-Id` header stamping (raw 16-byte UUID on every record post-`/connect`)
    > from [`per-server-sync`](../per-server-sync/spec.md) is reused unchanged.
>
> All requirements below assume `adapter-bootstrap` topic-delivery contract is extended
> with the new `messagingTopics` field in this slice.

**Must:**

- [todo] R1. `nx-gs-adapter-api.rest.ConnectResponse` MUST carry a new optional top-level
  field `messagingTopics: MessagingTopics`. `null` (field absent on the wire) is normalized
  to an empty bundle (every namespace resolves to an empty map). `SyncTopics` is left
  untouched — `db` / `runtime` / `dp` namespaces remain a separate concern.
    - SC1. The new field is **additive** — older adapter versions that don't know about
      `messagingTopics` parse the response without error; the field is silently ignored
      by Gson when no setter exists.

- [todo] R2. `nx-gs-adapter-api.rest.MessagingTopics` POJO MUST expose:
    - `Map<String,String> getEvents()` — event-family → fully-qualified Kafka topic.
      Phase 1 keys: `"premiumpurchase"`. Phase-2 reserved keys: `"character"`, `"clan"`,
      `"server"` (no enforcement; the platform may add or omit any key).
    - `Map<String,String> getCommands()` — command-domain → fully-qualified Kafka topic.
      Phase 1 — empty map (architectural placeholder). Phase-2 reserved keys: `"char"`,
      `"clan"`, `"mail"`, `"account"`.
    - Defensive copies on construction, unmodifiable views on getters, `null` normalized
      to empty map at the getter (matches `SyncTopics` precedent).
    - Hand-written `Builder` + `toBuilder()`, `equals` / `hashCode` / `toString`.

- [todo] R3. `nx-gs-adapter-api` SHIPS the event-family contract:
    - Package `app.l2nx.gs.adapter.api.kafka.events` — root for all event families.
    - Package `app.l2nx.gs.adapter.api.kafka.events.premiumpurchase` — Phase-1 family:
        - `abstract class PremiumPurchaseEvent` — common base (no fields; pure marker for the
          per-family typed publish method `NxEvents.publishPremiumPurchase(PremiumPurchaseEvent)`).
          Future subtypes `PremiumRefundEvent`, `PremiumGiftReceivedEvent` are added
          here without changing `NxEvents`.
        - `final class PremiumPurchaseEvent extends PremiumPurchaseEvent` — fields per R3.1.
        - `final class PurchaseItem` — fields per R3.2.
        - `final class PurchaseService` — fields per R3.3.
        - `final class Payment` — fields per R3.4.
        - `final class WellKnownServices` — string constants per R3.5.

    - **R3.1** `PremiumPurchaseEvent` field set:
        - `UUID eventId` — REQUIRED. **MUST be UUIDv7.** Wire timestamp is encoded
          in the upper 48 bits (extractable via `UUIDv7.extractCreatedAt`); no separate
          `occurredAt` field.
        - `long characterId` — REQUIRED.
        - `@Nullable String characterName` — optional.
        - `@Nullable String accountName` — optional.
        - `@Nullable List<PurchaseItem> items` — getter normalizes `null` → `emptyList()`.
        - `@Nullable List<PurchaseService> services` — getter normalizes `null` → `emptyList()`.
        - **Soft invariant:** `items.size() + services.size() >= 1`. Producers MUST NOT emit
          an event with both empty; the wire schema permits it (consumer-side validation
          logs and dedupes on `eventId` rather than rejecting). No Java-level constructor
          enforcement — keeps the POJO Gson-friendly.

    - **R3.2** `PurchaseItem` field set:
        - `long itemId` — REQUIRED.
        - `long qty` — REQUIRED.
        - `@Nullable Map<String,String> params` — host-specific opaque metadata
          (enchant level, attribute element, soul-stone slots, …). Getter normalizes
          `null` → empty map.
        - `List<Payment> payments` — REQUIRED, `1..N`. Multi-currency lines (e.g.
          Giant Codex Mastery: 20 Coin-of-Luck + 10M Adena) are first-class.

    - **R3.3** `PurchaseService` field set:
        - `String code` — REQUIRED. Canonical service code; see `WellKnownServices`.
        - `@Nullable Map<String,String> params` — optional structured args
          (e.g. `rename`: `{"old": "X", "new": "Y"}`; `name_color_change`:
          `{"rgb": "0xFFCC00"}`).
        - `List<Payment> payments` — REQUIRED, `1..N`.

    - **R3.4** `Payment` field set:
        - `long currencyItemId` — REQUIRED. Raw L2 item ID (e.g. `4037` for Coin of Luck,
          `57` for Adena). Platform maps id → human name via its own catalog.
        - `long qty` — REQUIRED.

    - **R3.5** `WellKnownServices` constants (string-typed, open enum — hosts MAY use
      additional non-standard codes, platform treats unknown codes as opaque):
      `noblesse`, `subclass`, `sex_change`, `name_change`, `name_color_change`,
      `title_color_change`, `hero_temporary`, `clan_lvl_up`, `clan_skill_buy`,
      `clan_rep_buy`, `clan_fame_buy`, `clan_create_penalty_remove`,
      `clan_join_penalty_remove`, `clan_invite_penalty_remove`, `ally_penalty_remove`,
      `level_up`, `level_down`, `karma_recover`, `pk_recover`, `vitality_recover`,
      `augmentation`, `olympiad_pts_buy`, `soul_cloak_transfer`. Catalog is curated
      from bohpts community-board listeners + multisell custom shop XMLs and is
      L2-canonical — covers L2J / Lucera / Essence forks without per-fork divergence.
      Adding new codes is non-breaking; `nx-gs-adapter-api` minor-bumps the constants list.

    - All POJOs Java-8, `final` fields, hand-written `Builder` + `toBuilder()`,
      JSpecify `@Nullable`, `equals`/`hashCode`/`toString`. Constructor parameter
      names preserved (`-parameters`) for Gson without `@JsonProperty`.

- [todo] R4. `nx-gs-commons` MUST ship `app.l2nx.gs.commons.UUIDv7`:
    - `static UUID generate()` — generates a UUIDv7 per RFC 9562: top 48 bits =
      `Instant.now().toEpochMilli()`, version nibble `0x7`, 12 random bits, variant `0b10`,
      62 random bits. Includes a per-thread monotonic counter that increments when two
      calls land on the same epoch ms — guarantees strictly-increasing IDs within a single
      JVM at sub-millisecond granularity.
    - `static Instant extractCreatedAt(UUID uuid)` — returns the embedded timestamp.
      Throws `IllegalArgumentException` for non-v7 input.
    - `static UUID fromString(String value)` — tolerant variant: `null` / blank → `null`,
      otherwise delegates to `UUID.fromString`. **Java 8 compat:** uses
      `value.trim().isEmpty()` (not `isBlank()`).
    - **Pure JDK, no third-party dep.** Hand-rolled (~50 LOC). Diverges from
      `nx-libs/common` UUIDv7 (which uses `com.fasterxml.uuid` and Java 11 `String.isBlank`)
      because the `:nx-gs-commons` charter is Java-8 + JSpecify-only deps.
    - Public to consumers of `:nx-gs-commons` (adapter-core uses it; tenant providers
      and bohpts hooks may use it for their own correlation-id needs).

- [todo] R5. `nx-gs-adapter-api.kafka.NxHeaders` MUST expose a new constant:
    - `String NX_MESSAGE_TYPE = "Nx-Message-Type"` — UTF-8 string header, value =
      simple class name of the concrete event type (e.g. `"PremiumPurchaseEvent"`).
      Auto-stamped by adapter-core on every event publish; platform consumers
      switch on this header for deserialization without peeking into the JSON.
    - `Nx-Server-Id` header (existing) — unchanged.

- [todo] R6. `nx-gs-adapter-api.spi.ConnectContext` MUST expose `NxEvents events()`
  accessor — symmetric to existing `getSyncTopics()`. Returned facade is non-null;
  when `messagingTopics.events` is empty / absent, the facade is a no-op
  implementation that logs DEBUG on every publish call ("events disabled").

- [todo] R7. `app.l2nx.gs.adapter.api.spi.NxEvents` interface (new SPI, package
  `spi` to align with existing tier-1 SPIs) MUST expose:
    - `void publishPremiumPurchase(PremiumPurchaseEvent event)` — Phase 1 family entrypoint.
      Future families add sibling methods (`publishCharacter`, `publishClan`,
      `publishServer`) — adding a method is a binary-compatible API expansion
      because tenant code calls these directly, doesn't implement them.
    - `void publish(NxEvent event)` is **NOT** in the contract — per-family
      typed methods give better discoverability and avoid a registry bottleneck
      at adapter-core.
    - Method semantics: enqueue + return. MUST NOT block longer than the time
      to push a record into a bounded `ArrayBlockingQueue`. MUST NOT throw —
      serialization / Kafka errors are surfaced through heartbeat counters
      and WARN logs, never up the call chain (game-loop safety).

- [todo] R8. `nx-gs-adapter-core` MUST implement an internal `EventsPublisher`:
    - One bounded `ArrayBlockingQueue<EventEnvelope>` shared across all event
      families. Default capacity `10000`. Configurable via `l2nx.events.queue-capacity`.
    - One daemon thread `nx-events-publisher` (SafeRunnable-wrapped) that drains
      the queue: for each envelope, resolve `(topic, messageType, partitionKey)`
      via a hardcoded type-registry, Gson-serialize, and call
      `kafkaProducer.send(record, callback)`.
    - Drop policy on full queue: `oldest` (default) — evicts the head to make
      room for the new envelope. Configurable via `l2nx.events.drop-policy`
      (`oldest` | `newest`).
    - Shutdown drain: `stop()` signals the publisher thread, drains up to
      `l2nx.events.shutdown-drain-timeout-ms` (default `5000`) before yielding.
      Remaining envelopes are logged as drop-on-shutdown counter.
    - Per-callback bookkeeping: increment `published-total` on ack-success,
      `failed-total` on ack-failure (Kafka producer error). Publisher thread
      itself never fails — `Throwable` from serialization or `send()` is caught,
      logged, counted as `failed-total`, loop continues.

- [todo] R9. The type registry inside adapter-core MUST resolve, per concrete
  event class:
    - **Topic** — looked up by family key in `messagingTopics.events`
      (`PremiumPurchaseEvent` → key `"premiumpurchase"`).
    - **MessageType header value** — concrete class simple name
      (`"PremiumPurchaseEvent"`).
    - **Partition key extractor** — function `T -> byte[]`. Phase-1 mappings:
        - `PremiumPurchaseEvent` → `LongSerializer.serialize(event.getCharacterId())`
          (8-byte big-endian, ordering per character).
    - Future families add registry entries; the registry is hardcoded in
      adapter-core (not pluggable) until at least 3 distinct families ship —
      no premature SPI.

- [todo] R10. The publisher MUST resolve missing topics gracefully:
    - Family in `messagingTopics.events` map missing → **family is disabled.**
      `NxEvents.publishX(...)` for that family becomes a no-op + DEBUG log
      ("events.<family> disabled — no topic configured"). Heartbeat surfaces
      `disabledFamilies: ["premiumpurchase", ...]` on the events module slot.
    - Topic present but Kafka producer not yet ready (i.e. `publishX` called
      before `onConnect` completes) → drop + DEBUG log. Phase 1 does NOT buffer
      pre-connect events; host code is expected to wire publish hooks behind
      `start()` lifecycle.

- [todo] R11. **Bohpts integration (host-side, not in this monorepo).** A new
  `BohptsPremiumPurchaseHook` MUST live in `bohpts-core/l2e.gameserver.l2nx`
  alongside existing `BohptsRuntimeStateProvider` / `BohptsDbSchemaProvider`.
  Wires into existing premium-purchase code paths:
    - Community-board service listeners (`CSBuyNoblesse`, `CSChangeNickName`,
      `CSChangeSex`, `CSChangeNickNameColor`, `CSChangeTitleColor`,
      `CSClanNameChange`, `HeroAnswerListener`, `ClanLevelAnswerListener`,
      `LevelAnswerListener`, `RecoveryPkAnswerListener`,
      `RecoveryKarmaAnswerListener`, `RecoveryVitalityAnswerListener`,
      `ReputationAnswerListener`, `AugmentationAnswerListener`, …) →
      `publishPremiumPurchase(PremiumPurchaseEvent.builder().services(...).build())`.
    - Multisell custom-shop callback (custom shop entry IDs `20005`/`20011`/`…`/
      `20204` — Coin-of-Luck-priced multisells per legacy datapack) →
      `publishPremiumPurchase(PremiumPurchaseEvent.builder().items(...).build())`.
    - Hook acquires the `NxEvents` facade once at adapter-bootstrap connect
      callback (host receives the connected `ConnectContext` via existing
      bohpts ↔ adapter wiring), caches it for the duration of the session.
    - **Out of scope of this monorepo** — the hook ships in the private
      `bohpts-core` repo. The spec lists it here so the integration point is
      discoverable; implementation lives under that repo's PR.

- [todo] R12. **Architectural sketch — commands inbound (NOT IMPLEMENTED in
  this slice).** Documented to commit the wire shape:
    - **Wire topology:** per-domain topic `<tenant>.gs.commands.<domain>`.
      Reserved domains: `char`, `clan`, `mail`, `account`. Adapter consumes one
      topic per domain it supports.
    - **Headers:** `Nx-Message-Type` (concrete command class simple name) +
      `Nx-Correlation-Id` (UUIDv7, platform-issued; embedded timestamp = command
      enqueue time on platform side).
    - **Reply path:** NOT Kafka-RPC. Adapter publishes
      `CommandResultEvent { correlationId, success, errorCode?, errorDetails?, payload? }`
      as a member of the events stream (likely `events.commands.replies`
      family — finalized in Phase 2). Platform correlates by `correlationId`.
    - **Result envelope (replaces legacy `ResponseV2`):** structured `errorCode`
      enum (`NOT_FOUND`, `INVALID_STATE`, `FORBIDDEN`, `RATE_LIMITED`,
      `UNAVAILABLE`, `VALIDATION_FAILED`, `INTERNAL_ERROR`), optional
      `errorDetails: Map<String,String>`, optional typed `payload: T`. Free-form
      `message` field is dropped — error context lives in structured details.
    - **SPI hook:** `ctx.commands().on(domain, CommandClass.class, handler)` —
      handler signature `CommandResult<R> handle(C command, CommandContext ctx)`.
      `CommandContext` exposes the `Executor gameThreadExecutor()` provided by
      the host on `onConnect` so handlers can hop onto the game loop when needed.
    - **Phase-1 placeholder:** `app.l2nx.gs.adapter.api.kafka.commands` package
      exists with a single `interface NxCommand {}` marker + Javadoc-stub
      describing the Phase-2 plan. Empty `commands: {}` shipped in
      `MessagingTopics`.

- [todo] R13. **Module versions (MVP slice ship):**
    - `nx-gs-adapter-api` = `0.13.0` — additive (new `MessagingTopics` field on
      `ConnectResponse`, new `kafka.events.*` and `kafka.commands` packages,
      new `NxHeaders.NX_MESSAGE_TYPE` + `NX_CORRELATION_ID` constants, new
      `NxEvents` SPI in `spi.*`, new `ConnectContext.events()` accessor, new
      `EventsStats` POJO + `events` slot on `ModuleStatus.Stats`). No deletions;
      clients on `0.12.x` continue to work against an `0.13.0` platform that
      omits `messagingTopics`.
    - `nx-gs-commons` = `0.2.0` — new `UUIDv7` utility class.
    - `nx-gs-kafka` = `0.4.0` — additive: new
      `NxProducer.sendBytesKeyRecord(record, callback)` +
      `NxKafka.sendBytesKeyRecord(record, callback)` so adapter-core can
      attach per-record headers ({@code Nx-Message-Type}) on byte-keyed
      events records. No removals.
    - `nx-gs-adapter-core` = `0.6.0` — new `EventsPublisher` + `NxEvents` impl,
      new `messagingTopics` parsing on `ConnectResponse`, heartbeat slot
      extension. Bumps API dep to `0.13.0`, commons to `0.2.0`, kafka to
      `0.4.0`.
    - `nx-gs-db-sync-core`, `nx-gs-runtime-sync-core` — no contract change;
      versions stay where they are.

**Should:**

- [todo] R14. Adapter-core SHOULD surface events-publisher health on the
  heartbeat. New built-in module slot `events` in
  `HeartbeatEvent.enabledModules` (built-in modules use `name="events"`,
  same shape as discovered `AdapterModule`s):
    - `state` ∈ `{ACTIVE, DEGRADED}` — `DEGRADED` when `failed-total` /
      `published-total` ratio exceeds an internal threshold (deferred to
      implementation; a starting threshold of 5% over the last minute is
      reasonable but not load-bearing in the spec).
    - `stats`:
        - `queue-depth: int` — current `ArrayBlockingQueue.size()`.
        - `queue-capacity: int` — configured cap.
        - `published-total: long` — running counter.
        - `dropped-total: long` — running counter (queue-full evictions +
          shutdown drops).
        - `failed-total: long` — Kafka send-callback failures.
        - `disabled-families: List<String>` — families with no topic configured.

**Could:**

- [todo] R15. Adapter-core COULD support **pre-connect buffering** of events
  emitted before `onConnect` completes. Default off; opt-in via
  `l2nx.events.pre-connect-buffer-capacity` (`>0` enables, capped). Comes when
  a real ops case demands it (e.g. host code emits a `CharacterLoggedInEvent`
  during JVM bootstrap before the platform handshake). Phase-1 host code
  (bohpts premium hook) wires publishes behind game-events that always fire
  post-`start()`, so this is unneeded.

**Non-goals:**

- **Schema registry / wire versioning beyond additive evolution.** Events are
  Gson-friendly POJOs with `serializeNulls=false`. Adding a nullable field is
  forward+backward compatible. Removing or retyping a field is a breaking
  api-major bump.
- **Tracing (`Nx-Trace-Id`).** Distributed tracing across publish ↔ consume is
  desirable but not justified at MVP scale.
- **Per-event-type config overrides** (per-type queue / drop policy / partitioner).
  One global queue, one drop policy. Differentiation comes when a real event type
  needs different SLA — likely after `events.server` ships and snapshots compete
  for queue space with purchase events.
- **Concrete `CharacterEvent` / `ClanEvent` / `ServerEvent` subtypes.** Topic
  slots are reserved in `messagingTopics.events`; the type hierarchy and
  `publishCharacter` / etc. methods land in follow-up slices when platform-side
  consumers exist.
- **Commands inbound runtime.** Wire shape is committed (R12); consumer thread,
  dispatch table, handler invocation, game-thread hop, reply publication —
  Phase 2.
- **Pre-connect publish buffering** (R15 lifted to Could).
- **Replay / dedup on the adapter side.** Platform consumer dedupes on `eventId`
  (UUIDv7); the adapter is at-least-once and does not maintain its own seen-set.
- **Synchronous publish API.** Caller blocks only long enough to enqueue. No
  `CompletableFuture<Void>` return — Java 8 friendly, L2-loop friendly, and the
  ack-callback bookkeeping in heartbeat is sufficient observability.
- **Per-tenant `NxEvents` impl override.** The publisher is a built-in piece of
  adapter-core; tenants don't get to swap it. If a tenant needs custom event
  routing (rare), they wrap `NxEvents` themselves at the call site.

### Edge cases

- **Both `items` and `services` empty on a `PremiumPurchaseEvent`** — wire-valid,
  semantically malformed. Producer side MUST NOT emit; if it does, consumer logs
  WARN and dedupes-by-`eventId` so the malformed envelope doesn't crash a batch.
- **Queue full during a burst** — drop-oldest evicts the head; head was likely
  a low-priority `ServerServerOnlineSnapshotEvent` from the previous tick. Premium
  events at typical L2 server cadence (≤ 100/min) never approach the 10k cap.
- **Clock skew producing UUIDv7 with past timestamp** — `extractCreatedAt`
  returns the literal embedded timestamp. Platform consumer is responsible for
  detecting "unrealistic" timestamps; adapter does not normalize.
- **Multiple JVMs producing events for the same character** — UUIDv7 monotonic
  counter is per-JVM; cross-JVM collision odds are 2⁻⁷⁴ per millisecond,
  acceptable for at-least-once.
- **Adapter shuts down mid-publish** — drain timeout (5s default) cleans the
  queue; remainder dropped + counted. Platform will see a publish gap.
- **Host calls `publishPremiumPurchase(null)`** — adapter-core treats `null` as no-op
  with WARN log (does not throw, never propagates to host thread).

## Open questions

- [resolved: Events module is built-in to adapter-core, not a discovered Tier-1
  `AdapterModule`. The capability is always present; absence of `messagingTopics`
  on the wire just disables publish at the per-family level. Mirrors how
  `HeartbeatEvent` is handled today — built-in, not a pluggable module.]
- [resolved: UUIDv7 lives in `nx-gs-commons` (Java-8 friendly), not in
  `nx-gs-adapter-api`. The api charter forbids any runtime deps and forbids
  Java-11 syntax; the existing `nx-libs/common.UUIDv7` uses both. Hand-rolled
  port keeps zero deps and Java-8 compat.]
- [resolved: One method per event family (`publishPremiumPurchase(PremiumPurchaseEvent)`) with
  abstract base superclass per family. Future subtypes within a family add
  zero new methods. Discoverability over a single generic `publish(NxEvent)`.]
- [resolved: `params` shape is `Map<String,String>` on both `PurchaseItem` and
  `PurchaseService`. Strict typing per service code (e.g. `RenameParams`,
  `ColorChangeParams`) is rejected — the platform side already has weakly-typed
  catalog handling for legacy reasons; pinning the wire to typed records would
  multiply DTO classes per service code. Map<String,String> is lossless for
  the use cases identified in the bohpts inventory (rename: `old`/`new`,
  color: `rgb`, level_up: `count`).]
- [resolved: Source of purchase (community-board / multisell / NPC / item-handler)
  is **NOT** carried in `PremiumPurchaseEvent`. Operator audit / tracing
  belongs in a separate `PurchaseAuditEvent` family if needed; the user-facing
  premium-purchase event keeps to "what was bought + paid" semantics.]
- [resolved: Item enchant level / attributes go into `PurchaseItem.params`,
  not first-class fields. Phase-1 premium SKUs in bohpts datapack are all
  unenchanted; if a future SKU sells an enchanted item, the host hook stamps
  `params.put("enchant", "10")` and the platform consumer reads from there.]
- [assumed: Heartbeat `events` slot uses module-name `"events"`. Collision
  with a future user-discovered `AdapterModule` named `"events"` is a packaging
  bug. Reserved name like the existing `db-sync` / `runtime-sync` are.]
- [assumed: Per-family Kafka producer config (acks / retries / linger.ms /
  compression) is the same as existing sync-event publishes. Not differentiated
  in Phase 1.]

## Links

- Sibling feature (Tier-1 SPI plumbing):
  [`docs/features/adapter-modules/spec.md`](../adapter-modules/spec.md)
- Sibling feature (topic delivery contract):
  [`docs/features/adapter-bootstrap/spec.md`](../adapter-bootstrap/spec.md)
- Sibling feature (`Nx-Server-Id` header stamping):
  [`docs/features/per-server-sync/spec.md`](../per-server-sync/spec.md)
- Legacy reference (RabbitMQ command surface, web side):
  `E:/bohpts/code/bohpts-web/backend/src/main/java/com/bohpts/messaging/`
- Legacy reference (RabbitMQ command surface, core side):
  `E:/projects/bohpts/bohpts-core/core/src/main/java/l2e/gameserver/infrastructure/rabbitMq/`
- Premium-purchase inventory source: bohpts community-board service listeners
  (`l2e.gameserver.handler.communityhandlers.impl.*`) + multisell custom shops
  (`bohpts-datapack/game/data/stats/npcs/multisell/custom/2000*.xml`).
