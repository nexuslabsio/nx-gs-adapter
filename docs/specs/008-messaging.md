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
>
> - Tier-1 SPI (`AdapterModule` + ServiceLoader) lives in > [`adapter-modules`](002-adapter-modules/spec.md). Phase 1 messaging is **not** > a discovered Tier-1 module — it lives inside `nx-gs-adapter-core` as a built-in > capability surfaced through `ConnectContext`.
> - Existing `Nx-Server-Id` header stamping (raw 16-byte UUID on every record post-`/connect`) > from [`per-server-sync`](007-per-server-sync.md) is reused unchanged.
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
  - One daemon thread `nx-events-publisher` (uncaught-handler installed,
    `Throwable` caught in the drain loop) that drains the queue: for each
    envelope, resolve `(topic, messageType, partitionKey)` via a hardcoded
    type-registry, Gson-serialize, and call `kafkaProducer.send(record, callback)`.
  - Drop policy on full queue: `newest` (default) — rejects the incoming
    envelope so queue order is preserved. `oldest` (evict head) remains
    configurable via `l2nx.events.drop-policy` (`oldest` | `newest`) but
    over-counts `dropped-total` under multi-producer contention because the
    displaced envelope is counted on the eviction path even when concurrent
    enqueuers race for the same slot.
  - Shutdown drain: `stop()` signals the publisher thread, drains up to
    `l2nx.events.shutdown-drain-timeout-ms` (default `5000`) before yielding.
    Remaining envelopes are logged as drop-on-shutdown counter. Coordinated
    with the JVM shutdown hook via a `CountDownLatch` so the app-initiated
    shutdown path and the hook caller don't double-close the producer.
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
  - **Reply path (as shipped):** a dedicated topic —
    `MessagingTopics.commandsRepliesTopic`, not an events family. The reply record is keyed by the
    big-endian most-significant bits of the correlation id and carries `Nx-Correlation-Id` (echoed)
    plus `Nx-Message-Type` (the reply type derived from the command class name). Platform correlates
    by `correlationId`.
  - **Result envelope (as shipped, replaces legacy `ResponseV2`):**
    `CommandResult { status: CommandStatus, payload?: R, problem?: CommandProblem }`. `CommandStatus`
    carries a `Tier` (OK / CLIENT_ERROR / SERVER_ERROR); error context rides `CommandProblem`
    (RFC 9457-subset: title + detail + extensions), which — unlike the originally planned
    `Map<String,String> errorDetails` — can carry numeric and list context. Full contract:
    [009-commands/spec.md](009-commands/spec.md).
  - **SPI hook (as shipped):** `ctx.commands().on(CommandClass.class, handler)` — no domain
    argument, since a single topic carries every command. Handler signature
    `CommandResult<R> handle(C command, CommandContext ctx)`. `CommandContext` exposes
    `host()` (a `HostExecutor` for game-thread hops) and `io()` (the adapter IO pool for
    blocking JDBC / HTTP).
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
- **Queue full during a burst** — default `newest` policy rejects the incoming
  envelope and increments `dropped-total`, preserving queue order. `oldest`
  (opt-in) evicts the head; under multi-producer contention the eviction path
  over-counts `dropped-total` because concurrent enqueuers race for the same
  slot. Premium events at typical L2 server cadence (≤ 100/min) never approach
  the 10k cap.
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
- [resolved: Per-family Kafka producer config is shared. Adapter-core wires
  at-least-once durability defaults into the single producer: `acks=all`,
  `enable.idempotence=true`, `max.in.flight.requests.per.connection=5`,
  `linger.ms=10`, `compression.type=gzip`, `retries=Integer.MAX_VALUE`,
  `delivery.timeout.ms=120000`. All overridable via user properties (config
  file or system properties). The at-least-once contract is enforced at the
  producer level, not merely claimed in docs.]

## Links

- Sibling feature (Tier-1 SPI plumbing):
  [`docs/specs/002-adapter-modules/spec.md`](002-adapter-modules/spec.md)
- Sibling feature (topic delivery contract):
  [`docs/specs/001-adapter-bootstrap.md`](001-adapter-bootstrap.md)
- Sibling feature (`Nx-Server-Id` header stamping):
  [`docs/specs/007-per-server-sync.md`](007-per-server-sync.md)
- Legacy reference (RabbitMQ command surface, web side):
  `E:/bohpts/code/bohpts-web/backend/src/main/java/com/bohpts/messaging/`
- Legacy reference (RabbitMQ command surface, core side):
  `E:/projects/bohpts/bohpts-core/core/src/main/java/l2e/gameserver/infrastructure/rabbitMq/`
- Premium-purchase inventory source: bohpts community-board service listeners
  (`l2e.gameserver.handler.communityhandlers.impl.*`) + multisell custom shops
  (`bohpts-datapack/game/data/stats/npcs/multisell/custom/2000*.xml`).

---

## Technical design

### Overview

Messaging is a built-in capability of `nx-gs-adapter-core` (not a discovered
`AdapterModule`). It exposes one bidirectional surface to host integration code:
`ConnectContext.events()` returns an `NxEvents` facade for outbound discrete-fact
fanout to per-family Kafka topics. The façade has stable identity across
reconnect cycles — an internal `AtomicReference` is swapped to the live publisher
on every reconnect, so host modules cache the reference once at `start()` and
never re-acquire.

Host hooks call typed publish methods (`publishPremiumPurchase`, …) which
enqueue into a shared bounded `ArrayBlockingQueue` on a single internal
`nx-events-publisher` daemon thread; the daemon Gson-serializes, stamps
`Nx-Server-Id` + `Nx-Message-Type` headers, derives a partition key from the
event payload via a hardcoded type-registry, and hands off to the single
`KafkaProducer<byte[], Object>` owned by `DefaultNxProducer` (string keys are
encoded to UTF-8 bytes at the boundary). Producer durability is wire-enforced
— `acks=all`, `enable.idempotence=true`, `max.in.flight.requests.per.connection=5`,
`linger.ms=10`, `compression.type=gzip`, `retries=Integer.MAX_VALUE`,
`delivery.timeout.ms=120000` — all overridable via user properties. Producer
close timeout is configurable via `KafkaConfig.Builder.producerCloseTimeout(Duration)`
(default `10s`).

Auto-stamped headers are reused from
[`per-server-sync`](007-per-server-sync.md). Topic addressing comes from a
new optional `messagingTopics: { events, commands }` field on `ConnectResponse`,
parallel to the existing `syncTopics` bundle. Inbound commands are realized in
the [`commands`](009-commands/spec.md) slice — a `ConsumerGroup` (single
`KafkaConsumer`, sync per-record commit after handler success, no commit on
handler exception → at-least-once redelivery) drives the dispatch surface.
The adapter also reuses a persistent `AdminClient` for the connect-flow Kafka
health check — created once at init, reused.

### Structure

- `nx-gs-adapter-api/src/main/java/app/l2nx/gs/adapter/api/rest/MessagingTopics.java`
  [planned] — new POJO; `events: Map<String,String>` + `commands: Map<String,String>`
- `nx-gs-adapter-api/src/main/java/app/l2nx/gs/adapter/api/rest/ConnectResponse.java`
  [planned] — extends with `@Nullable MessagingTopics messagingTopics` field
  (additive); existing fields untouched
- `nx-gs-adapter-api/src/main/java/app/l2nx/gs/adapter/api/spi/ConnectContext.java`
  [planned] — adds `NxEvents events()` accessor
- `nx-gs-adapter-api/src/main/java/app/l2nx/gs/adapter/api/spi/NxEvents.java`
  [planned] — new SPI; one method per event family
- `nx-gs-adapter-api/src/main/java/app/l2nx/gs/adapter/api/kafka/events/`
  [planned] — package root for event families
- `nx-gs-adapter-api/src/main/java/app/l2nx/gs/adapter/api/kafka/events/premium/`
  [planned] — Phase-1 family; `PremiumPurchaseEvent` (abstract base),
  `PremiumPurchaseEvent`, `PurchaseItem`, `PurchaseService`, `Payment`,
  `WellKnownServices`
- `nx-gs-adapter-api/src/main/java/app/l2nx/gs/adapter/api/kafka/commands/`
  [planned] — package placeholder; `NxCommand` marker interface + Javadoc-stub
- `nx-gs-adapter-api/src/main/java/app/l2nx/gs/adapter/api/kafka/NxHeaders.java`
  [planned] — adds `NX_MESSAGE_TYPE` constant
- `nx-gs-commons/src/main/java/app/l2nx/gs/commons/UUIDv7.java`
  [planned] — pure-JDK UUIDv7 generator + extractor
- `nx-gs-adapter-core/src/main/java/app/l2nx/gs/adapter/core/events/EventsPublisher.java`
  [planned] — bounded queue + daemon thread + drop policy
- `nx-gs-adapter-core/src/main/java/app/l2nx/gs/adapter/core/events/NxEventsImpl.java`
  [planned] — `NxEvents` SPI implementation; routes per-family methods into the
  shared queue
- `nx-gs-adapter-core/src/main/java/app/l2nx/gs/adapter/core/events/EventTypeRegistry.java`
  [planned] — hardcoded `Class<?> → (familyKey, messageType, partitionExtractor)`
  table
- `nx-gs-adapter-core/src/main/java/app/l2nx/gs/adapter/core/events/EventsModuleStatus.java`
  [planned] — heartbeat slot generator (counter + gauge snapshot)

Bohpts-side (in `bohpts-core` repo, not this monorepo):

- `bohpts-core/core/src/main/java/l2e/gameserver/l2nx/BohptsPremiumPurchaseHook.java`
  [planned] — wires CB service-listeners + multisell callbacks → `nxEvents.publishPremiumPurchase(...)`

### Key components

- **MessagingTopics** [planned] (implements R1, R2) — REST DTO bundling event-family
  and command-domain topic maps. Same shape conventions as `SyncTopics`: defensive
  copy + unmodifiable getter + null-normalization.

- **NxEvents** [planned] (implements R6, R7) — Tier-2-style SPI consumed by host
  integration code. Returned from `ConnectContext.events()`; never null. One
  method per event family — `publishPremiumPurchase(PremiumPurchaseEvent)`. Future families add
  sibling methods without breaking existing callers (binary-compatible).

- **NxEventsImpl** [planned] (implements R7) — adapter-core implementation.
  Stateless façade over `EventsPublisher` + `EventTypeRegistry`. All `publishX`
  methods funnel through `EventsPublisher.enqueue(envelope)`.

- **EventsPublisher** [planned] (implements R8, R10) — owns the bounded queue,
  daemon thread, drop policy, and Kafka producer call site. Lifecycle wired
  into adapter-core's existing `onConnect` / `start` / `stop` sequence:
  `onConnect` materializes registry + creates queue; `start` spawns daemon;
  `stop` signals + drains.

- **EventTypeRegistry** [planned] (implements R9) — `Map<Class<? extends NxEvent>,
EventTypeBinding>` populated at adapter-core startup. Each binding carries
  `familyKey` (used to look up the topic in `messagingTopics.events`),
  `messageTypeHeader` value, and `Function<Object, byte[]> partitionKeyExtractor`.
  Phase 1 has exactly one binding (`PremiumPurchaseEvent` → `"premiumpurchase"` /
  `"PremiumPurchaseEvent"` / `characterId-as-long-bytes`).

- **EventsModuleStatus** [planned] (implements R14) — adapter-core renders the
  `events` slot of `HeartbeatEvent.enabledModules` from the publisher's atomic
  counters + queue snapshot. Same shape as the existing built-in slot for
  heartbeat itself.

- **UUIDv7** [planned] (implements R4) — `app.l2nx.gs.commons.UUIDv7` in
  `:nx-gs-commons`. Public API: `generate()`, `extractCreatedAt(UUID)`,
  `fromString(String)`. Pure JDK, ~50 LOC. Java 8 syntax (no `String.isBlank`).

- **PremiumPurchaseEvent / PremiumPurchaseEvent** [planned] (implements R3) — abstract
  family base + Phase-1 concrete subtype. Subclassing pattern is `PremiumX
extends PremiumPurchaseEvent` so Future subtypes (`PremiumRefundEvent`,
  `PremiumGiftReceivedEvent`) plug in without changing `NxEvents`.

- **WellKnownServices** [planned] (implements R3.5) — string constants class,
  curated from bohpts community-board listener catalog + multisell custom shops.
  Hosts MAY use additional non-canonical codes; platform treats unknown codes
  as opaque.

- **BohptsPremiumPurchaseHook** [planned] (implements R11; ships in
  `bohpts-core` repo) — host-side glue. Acquires `NxEvents` once at adapter
  connect callback, wires into existing community-board service handlers and
  multisell callbacks. Listed here for discoverability; out of this monorepo's
  delivery scope.

### Data flows

End-to-end publish (premium purchase from a community-board buy-noblesse click):

1. Host code (`CSBuyNoblesse` listener) finishes the in-game state mutation
   (player's `noble` flag set, currency item charged).
2. Host hook (`BohptsPremiumPurchaseHook`) constructs
   `PremiumPurchaseEvent.builder()
 .eventId(UUIDv7.generate())
 .characterId(player.getObjectId())
 .characterName(player.getName())
 .accountName(player.getAccountName())
 .services(Collections.singletonList(
  PurchaseService.builder()
      .code(WellKnownServices.NOBLESSE)
      .payments(Collections.singletonList(
          Payment.builder().currencyItemId(4037).qty(50).build()))
      .build()))
 .build()`
   and calls `nxEvents.publishPremiumPurchase(event)`.
3. `NxEventsImpl` calls `EventsPublisher.enqueue(envelope)` where
   `envelope = (event, registryBinding)`. Returns immediately.
4. `nx-events-publisher` daemon picks up the envelope:
   - Resolves topic via `messagingTopics.events.get("premiumpurchase")`.
   - If topic null/missing → log DEBUG, increment `disabled-family-drops`,
     drop. (Tracked separately from `dropped-total` so operators can tell
     "platform didn't issue a topic" from "queue overflow".)
   - Otherwise → Gson-serialize event POJO to JSON bytes, with
     `serializeNulls=false` so unset fields are absent on the wire.
   - Build `ProducerRecord(topic, partitionKey, jsonBytes)` where
     `partitionKey = LongSerializer.serialize(event.getCharacterId())`.
   - Stamp headers: `Nx-Server-Id` (raw 16-byte UUID — already auto-stamped
     by the producer's static-headers config from `per-server-sync`),
     `Nx-Message-Type: PremiumPurchaseEvent`.
   - Call `kafkaProducer.send(record, callback)`.
5. Kafka producer ack callback:
   - Success → `published-total++`.
   - Failure → `failed-total++` + WARN log with throwable.
6. Heartbeat tick (independent cadence) reads atomic counters + queue size
   into `events` slot of `HeartbeatEvent.enabledModules`.

Queue-overflow path (default `newest` drop policy — drop incoming):

1. Caller calls `publishPremiumPurchase(event)`.
2. `EventsPublisher.enqueue` calls `queue.offer(envelope)` non-blocking.
3. If `offer` returns `false` → `dropped-total++`, log WARN once per second
   (rate-limited). The envelope is dropped on the caller side, queue order
   is preserved.
4. Daemon thread continues draining unaffected.

Queue-overflow path (opt-in `oldest` drop policy — evict head):

1. Caller calls `publishPremiumPurchase(event)`.
2. `EventsPublisher.enqueue` checks remaining capacity; on full → `queue.poll()`
   evicts the head, `dropped-total++`, then `queue.offer(envelope)`.
3. Under multi-producer contention `dropped-total` over-counts — the eviction
   path increments the counter even when two enqueuers race for the same slot
   and only one actually has its envelope dropped. Documented caveat; pick
   `newest` if accurate accounting matters.

Shutdown path:

1. `AdapterModule.stop()` (or adapter-core's main `stop`) invokes
   `EventsPublisher.stop()`.
2. `EventsPublisher` flips an `AtomicBoolean shuttingDown` and signals
   the daemon.
3. Daemon drains remaining envelopes for up to
   `l2nx.events.shutdown-drain-timeout-ms` (default 5000ms).
4. Any envelopes still queued at timeout are counted into
   `shutdown-drops-total` (separate counter — operators want to distinguish
   shutdown drops from runtime drops).
5. Kafka producer is NOT closed by `EventsPublisher` — the producer's
   lifecycle is owned by adapter-core's bootstrap, shared with sync modules.

### Data model

In-memory only; no DB tables, no persistence.

- **EventsPublisher.queue** [planned] — `ArrayBlockingQueue<EventEnvelope>`,
  default capacity 10000. Cleared on `stop()`; rebuilt on next `start()`.
- **EventsPublisher counters** [planned] — `AtomicLong publishedTotal`,
  `droppedTotal`, `failedTotal`, `disabledFamilyDropsTotal`,
  `shutdownDropsTotal`, plus `AtomicInteger queueDepthSnapshot` (refreshed at
  each heartbeat tick).
- **EventTypeRegistry** [planned] — `Map<Class<? extends NxEvent>, EventTypeBinding>`,
  populated at startup, immutable thereafter.

Wire DTOs (Kafka payloads):

- **PremiumPurchaseEvent** [planned] — `kafka.events.premiumpurchase.PremiumPurchaseEvent`
  in `nx-gs-adapter-api`. Field set per spec R3.1.
- Future families add their own DTOs in `kafka.events.<family>.*` packages.

### Integration points

- **`ConnectContext.events()`** [planned] — host integration code's entry
  point to publish. Called once at host's connect callback; result cached for
  the session.
- **`NxKafka` producer** [planned] — reused via `EventsPublisher`. No new Kafka
  init code; the single `KafkaProducer<byte[], Object>` built at adapter
  bootstrap (per `adapter-bootstrap` R6) is shared. Static headers
  (`Nx-Server-Id`) registered there auto-apply to events as well as sync
  records. The `commands` consumer surface shares producer access for reply
  publication and uses a separate `ConsumerGroup` (renamed from
  `NxConsumerGroup`) running with `enable.auto.commit=false` and sync per-record
  commit after handler success — on handler exception the commit is skipped
  and the record redelivers. Subscription is via
  `NxKafka.subscribe(topic, groupId, ...)` — `groupId` is an explicit required
  parameter rather than being implicit on the facade.
- **`HeartbeatEvent.enabledModules`** [planned] — surfaces
  `{name: "events", state, stats: {queue-depth, queue-capacity, published-total,
dropped-total, failed-total, disabled-families}}` per R14. Same envelope
  shape as `db-sync` / `runtime-sync` slots.
- **`messagingTopics.events`** [planned] — engine reads per-family topic from
  this map. Map shape and platform-side issuance are owned by
  `adapter-bootstrap`'s `ConnectResponse` extension in this slice.
- **bohpts community-board listeners + multisell callbacks** [planned] — host
  side hooks into existing premium code paths, listed in spec R11. Read-only
  observation point — never mutates game state, only fires the event.

### Decisions

- **Decision:** Built-in capability inside adapter-core, not a discovered
  Tier-1 `AdapterModule`.
  **Why:** Publishing events is cross-cutting — every future module (db-sync,
  runtime-sync, premium hooks) might want to publish. Forcing each one to
  ship its own ServiceLoader-discovered module would duplicate Kafka producer
  config, queue, drop policy, and headers. Built-in capability lets one
  `EventsPublisher` serve every caller. Mirrors how heartbeat is built-in,
  not a pluggable module. The user explicitly chose "no separate module
  yet" during brainstorm.

- **Decision:** Per-family typed publish method
  (`publishPremiumPurchase(PremiumPurchaseEvent)`), not generic `publish(NxEvent)`.
  **Why:** Discoverability — IDE autocomplete shows every family the host can
  publish to. Prevents typos in family names (which `publish(String family,
Object payload)` would allow). Adding a family is a binary-compatible API
  expansion (host code calls these, doesn't implement them). Within-family
  growth is free — `PremiumRefundEvent extends PremiumPurchaseEvent` requires zero
  changes to the SPI. Considered: generic `publish(NxEvent)` with a registry
  resolving topic from runtime type. Rejected because the type-to-family
  registry then becomes a string-key bottleneck and the discovery story
  worsens.

- **Decision:** Async with bounded queue + drop-newest by default, never throws.
  **Why:** Game-loop safety is the project's highest-priority invariant
  ("Never block game-server threads" — root CLAUDE.md). Synchronous publish
  exposes the caller to Kafka producer latency (network, broker GC, leader
  reelection); even a small `RuntimeException` propagating up the L2 game
  loop is a correctness disaster. Bounded queue isolates the caller from
  back-pressure. `newest` is the default because it preserves queue order
  and gives accurate `dropped-total` accounting; `oldest` remains opt-in
  for operators who would rather keep the freshest events at the cost of
  an over-counted dropped counter under multi-producer contention.
  Considered: `CompletableFuture<Void>` return — overkill for Java-8-bound
  L2 hosts, and async-with-callback doesn't compose better than the
  heartbeat-counter observability.

- **Decision:** Single `KafkaProducer<byte[], Object>` (not two — string-keyed
  - byte-keyed).
    **Why:** Halves `buffer.memory` (32 MiB instead of 64), halves broker
    connections, and runs one sender I/O thread instead of two. String keys
    encode to UTF-8 bytes at the publish boundary; cheap conversion, large
    resource win. The previous two-producer setup is gone.

- **Decision:** Producer durability defaults are wire-enforced.
  **Why:** The at-least-once contract used to be a documentation claim that
  callers had to keep in sync with their own producer properties. Adapter-core
  now defaults `acks=all`, `enable.idempotence=true`, `max.in.flight=5`,
  `linger.ms=10`, `compression=gzip`, `retries=MAX`,
  `delivery.timeout.ms=120000` at the producer level. User properties
  (config file or sysprops) override but the wire defaults are conservative.

- **Decision:** UUIDv7 lives in `:nx-gs-commons`, hand-rolled, no fasterxml.
  **Why:** `:nx-gs-adapter-api` charter is zero-runtime-deps; `:nx-gs-commons`
  charter is jspecify-only. The platform's `nx-libs/common.UUIDv7` uses
  `com.fasterxml.uuid:java-uuid-generator` AND Java-11 `String.isBlank` —
  pulling it as-is breaks both rules. Reimplementation is ~50 LOC and stays
  faithful to RFC 9562. Considered: relax commons charter to allow fasterxml.
  Rejected because the strict-deps rule is what makes commons safe to depend
  on across the open-core boundary.

- **Decision:** `eventId` is the timestamp; no separate `occurredAt` field.
  **Why:** UUIDv7 encodes the timestamp in the upper 48 bits, exact-millisecond
  precise, sortable as bytes. Carrying `occurredAt` as a long would duplicate
  the same information and risk drift (two fields, two ways to be wrong).
  Platform consumers extract via `UUIDv7.extractCreatedAt(eventId)`. Symmetric
  to how `nx-libs/common` already uses UUIDv7 across the platform.

- **Decision:** Per-line `payments` (on each `PurchaseItem` and
  `PurchaseService`), not a top-level `payments` on the event.
  **Why:** Real bohpts SKUs charge per-line (Giant Codex Mastery: 20 CoL +
  10M Adena for ONE item). A top-level `payments` would force consumers to
  apportion the cost across lines themselves. Per-line is honest about how
  pricing works in L2 and lets the platform compute totals trivially by
  summation.

- **Decision:** Per-family Kafka topic (`<tenant>.gs.events.premiumpurchase` etc.),
  not one shared `<tenant>.gs.events` topic with a `family` discriminator.
  **Why:** Different families want different retention, partitioning, and
  consumer-lag SLAs. `events.premiumpurchase` is high-value and long-retained;
  `events.server` (online snapshots, periodic) is low-value and short-retained.
  Separate topics let operators tune each independently without affecting
  the others. Within a family, multiple concrete event types share the topic
  and are discriminated by `Nx-Message-Type` header — cheap and forward-friendly.

- **Decision:** `Source` (multisell vs CB vs NPC vs item-handler) is NOT
  carried in `PremiumPurchaseEvent`.
  **Why:** Operator-audit / call-stack lineage is a different concern from
  user-facing premium purchase analytics. Embedding it in the same event
  pollutes the schema for the 95% of consumers who don't care. If audit
  emerges as a requirement, a parallel `PurchaseAuditEvent` in
  `events.audit` family covers it without bloating the user-facing event.
  Discussed during brainstorm; user explicitly chose to drop the field.

- **Decision:** `params: Map<String,String>` on both items and services —
  not typed records per service code.
  **Why:** Service codes have wildly varying parameter shapes
  (`rename`: old/new; `name_color_change`: rgb; `level_up`: count;
  `noblesse`: nothing). Typed records per code would multiply DTO classes
  by ~25; a generic map covers the use cases lossless. The platform's
  catalog already has weakly-typed parameter handling for L2 admin reasons,
  so weak typing on the wire matches the consumer's existing pattern.

- **Decision:** Commands shipped as architectural sketch only in Phase 1.
  **Why:** The user explicitly chose this scope — premium events are the
  first concrete need; the command surface is large (many domains) and
  benefits from a separate brainstorm pass once the events surface is
  battle-tested. Committing the wire shape now (topic naming, headers,
  reply-via-events-stream) prevents Phase 2 from re-relitigating the
  protocol layer; locking the runtime details (consumer threading,
  game-thread hop, dispatch table) is deferred until concrete commands
  are picked.

### Extension points

- **Add a new event family** — declare a new `<Family>Event` abstract class in
  `kafka.events.<family>` package, add a `publish<Family>(<Family>Event)`
  method to `NxEvents`, register the family-key in `MessagingTopics.events`
  on the platform side, hardcode the type→binding mapping in
  `EventTypeRegistry`. Phase 2 candidates: `character`, `clan`, `server`.

- **Add a new concrete event type within an existing family** — declare
  `Premium<Action>Event extends PremiumPurchaseEvent`, add registry binding in
  `EventTypeRegistry` (same family-key, distinct `messageType` header),
  no `NxEvents` SPI change required. Platform consumer switches on
  `Nx-Message-Type` header. Examples: `PremiumRefundEvent`,
  `PremiumGiftReceivedEvent`, `PremiumSubscriptionExpiredEvent`.

- **Add a new well-known service code** — append to `WellKnownServices`
  constants. Non-breaking; platform treats unknown codes as opaque (passes
  through to the platform's catalog mapper). Hosts using non-standard
  codes don't need a constant — code is an open string.

- **Per-family Kafka producer config override** [deferred, Could R-future]
  — when a family needs different acks/retries/linger.ms (e.g.
  `events.server` snapshot tolerates higher loss for lower latency), add
  per-family producer property prefix
  `l2nx.events.<family>.kafka.<property>` with fallback to the global
  Kafka config. Phase 1 uses the single shared producer.

- **Pre-connect buffering** [Could R15] — opt-in via
  `l2nx.events.pre-connect-buffer-capacity`. When set, `EventsPublisher`
  maintains a separate small queue used while `onConnect` is in flight;
  on connect-complete it drains the buffer into the main queue. Phase 1
  off because host hooks fire post-`start()`.

- **Commands inbound runtime** [Phase 2] — implement
`app.l2nx.gs.adapter.core.commands.CommandsConsumer` analogous to
`EventsPublisher`. Per-domain Kafka consumer thread, dispatch into a
registered `CommandHandler<C>` (registered by host code via
`ctx.commands().on(...)`), result publication via `EventsPublisher`
on the `events.commands.replies` family. Game-loop hop via host-supplied
`Executor`.
</content>
