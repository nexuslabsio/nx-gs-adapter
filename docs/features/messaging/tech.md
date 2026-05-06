# Messaging — tech

> Covers: spec.md

## Overview

Messaging is a built-in capability of `nx-gs-adapter-core` (not a discovered
`AdapterModule`). It exposes one bidirectional surface to host integration code:
`ConnectContext.events()` returns an `NxEvents` facade for outbound discrete-fact
fanout to per-family Kafka topics. Host hooks call typed publish methods
(`publishPremium`, …) which enqueue into a shared bounded `ArrayBlockingQueue`
on a single internal `nx-events-publisher` daemon thread; the daemon Gson-serializes,
stamps `Nx-Server-Id` + `Nx-Message-Type` headers, derives a partition key from
the event payload via a hardcoded type-registry, and hands off to the existing
`NxKafka` producer. Auto-stamped headers are reused from
[`per-server-sync`](../per-server-sync/spec.md). Topic addressing comes from a
new optional `messagingTopics: { events, commands }` field on `ConnectResponse`,
parallel to the existing `syncTopics` bundle. Inbound commands are an
architectural placeholder in Phase 1 — the wire shape and SPI are committed
in Javadoc + an empty `commands` map; the consumer + dispatch runtime is Phase 2.

## Structure

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
  [planned] — Phase-1 family; `PremiumEvent` (abstract base),
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
  [planned] — wires CB service-listeners + multisell callbacks → `nxEvents.publishPremium(...)`

## Key components

- **MessagingTopics** [planned] (implements R1, R2) — REST DTO bundling event-family
  and command-domain topic maps. Same shape conventions as `SyncTopics`: defensive
  copy + unmodifiable getter + null-normalization.

- **NxEvents** [planned] (implements R6, R7) — Tier-2-style SPI consumed by host
  integration code. Returned from `ConnectContext.events()`; never null. One
  method per event family — `publishPremium(PremiumEvent)`. Future families add
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
  Phase 1 has exactly one binding (`PremiumPurchaseEvent` → `"premium"` /
  `"PremiumPurchaseEvent"` / `characterId-as-long-bytes`).

- **EventsModuleStatus** [planned] (implements R14) — adapter-core renders the
  `events` slot of `HeartbeatEvent.enabledModules` from the publisher's atomic
  counters + queue snapshot. Same shape as the existing built-in slot for
  heartbeat itself.

- **UUIDv7** [planned] (implements R4) — `app.l2nx.gs.commons.UUIDv7` in
  `:nx-gs-commons`. Public API: `generate()`, `extractCreatedAt(UUID)`,
  `fromString(String)`. Pure JDK, ~50 LOC. Java 8 syntax (no `String.isBlank`).

- **PremiumEvent / PremiumPurchaseEvent** [planned] (implements R3) — abstract
  family base + Phase-1 concrete subtype. Subclassing pattern is `PremiumX
  extends PremiumEvent` so Future subtypes (`PremiumRefundEvent`,
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

## Data flows

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
   and calls `nxEvents.publishPremium(event)`.
3. `NxEventsImpl` calls `EventsPublisher.enqueue(envelope)` where
   `envelope = (event, registryBinding)`. Returns immediately.
4. `nx-events-publisher` daemon picks up the envelope:
    - Resolves topic via `messagingTopics.events.get("premium")`.
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

Queue-overflow path (drop-oldest):

1. Caller calls `publishPremium(event)`.
2. `EventsPublisher.enqueue` checks `queue.remainingCapacity() == 0`.
3. If so → `queue.poll()` evicts the head, `dropped-total++`, log WARN once
   per second (rate-limited), then `queue.offer(envelope)` (which now succeeds).
4. Daemon thread continues draining; the previously-evicted envelope is
   gone — it will not be retried, redelivered, or surfaced anywhere except
   the dropped counter.

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

## Data model

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

- **PremiumPurchaseEvent** [planned] — `kafka.events.premium.PremiumPurchaseEvent`
  in `nx-gs-adapter-api`. Field set per spec R3.1.
- Future families add their own DTOs in `kafka.events.<family>.*` packages.

## Integration points

- **`ConnectContext.events()`** [planned] — host integration code's entry
  point to publish. Called once at host's connect callback; result cached for
  the session.
- **`NxKafka` producer** [planned] — reused via `EventsPublisher`. No new Kafka
  init code; the producer instance built at adapter bootstrap (per
  `adapter-bootstrap` R6) is shared. Static headers (`Nx-Server-Id`) registered
  there auto-apply to events as well as sync records.
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

## Decisions

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
  (`publishPremium(PremiumEvent)`), not generic `publish(NxEvent)`.
  **Why:** Discoverability — IDE autocomplete shows every family the host can
  publish to. Prevents typos in family names (which `publish(String family,
  Object payload)` would allow). Adding a family is a binary-compatible API
  expansion (host code calls these, doesn't implement them). Within-family
  growth is free — `PremiumRefundEvent extends PremiumEvent` requires zero
  changes to the SPI. Considered: generic `publish(NxEvent)` with a registry
  resolving topic from runtime type. Rejected because the type-to-family
  registry then becomes a string-key bottleneck and the discovery story
  worsens.

- **Decision:** Async with bounded queue + drop-oldest, never throws.
  **Why:** Game-loop safety is the project's highest-priority invariant
  ("Never block game-server threads" — root CLAUDE.md). Synchronous publish
  exposes the caller to Kafka producer latency (network, broker GC, leader
  reelection); even a small `RuntimeException` propagating up the L2 game
  loop is a correctness disaster. Bounded queue isolates the caller from
  back-pressure; drop-oldest keeps the most recent (most relevant) events
  flowing when a burst exceeds capacity. Considered: `CompletableFuture<Void>`
  return — overkill for Java-8-bound L2 hosts, and async-with-callback
  doesn't compose better than the heartbeat-counter observability.

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

- **Decision:** Per-family Kafka topic (`<tenant>.gs.events.premium` etc.),
  not one shared `<tenant>.gs.events` topic with a `family` discriminator.
  **Why:** Different families want different retention, partitioning, and
  consumer-lag SLAs. `events.premium` is high-value and long-retained;
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

## Extension points

- **Add a new event family** — declare a new `<Family>Event` abstract class in
  `kafka.events.<family>` package, add a `publish<Family>(<Family>Event)`
  method to `NxEvents`, register the family-key in `MessagingTopics.events`
  on the platform side, hardcode the type→binding mapping in
  `EventTypeRegistry`. Phase 2 candidates: `character`, `clan`, `server`.

- **Add a new concrete event type within an existing family** — declare
  `Premium<Action>Event extends PremiumEvent`, add registry binding in
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
