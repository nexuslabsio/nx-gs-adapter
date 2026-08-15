# Adapter Modules

## Problem

`adapter-bootstrap` ships the connect-handshake / heartbeat / lifecycle skeleton, but
explicitly defers the pluggable-module mechanism to a follow-up slice (Non-goal:
"ServiceLoader-based discovery of `AdapterModule` — deferred to the next slice"). As a
result `HeartbeatEvent.enabledModules` is a placeholder (always empty list) and there is
no way to extend the adapter with sync / metrics / future capabilities.

This slice delivers that mechanism: a Tier-1 SPI (`AdapterModule`) discovered by
`nx-gs-adapter-core` after a successful platform handshake, plus a per-module lifecycle
state machine, plus the heartbeat enrichment that surfaces module health to the platform.
It is foundation for `db-sync` (first consumer), `dp-sync`, `metrics`, and any future
pluggable module type.

Audience: adapter-core authors (own the discovery + lifecycle code path); module authors
across `nx-gs-db-*` / `nx-gs-dp-*` / future sibling repos (consume the SPI); platform-side
heartbeat consumers (read enriched `enabledModules`).

## Requirements

**Must:**

- [done] R1. `nx-gs-adapter-api` MUST define `app.l2nx.gs.adapter.api.spi.AdapterModule` —
  Tier-1 SPI interface with five methods:
  - `String name()` — unique identifier (e.g. `"db-sync"`, `"dp-sync"`); surfaced in
    `enabledModules`
  - `void onConnect(ConnectContext ctx)` — called once after platform handshake +
    Kafka producer init success; module wires up ports, sets up resources
  - `void start()` — called once after every module's `onConnect` completes; module
    kicks off its own daemon work (schedulers, consumers, etc.)
  - `void stop()` — called on `NxAdapter.shutdown()` before `onDisconnect`; cancels
    schedulers, drains in-flight work; idempotent
  - `void onDisconnect()` — called after `stop`; releases resources (pools,
    connections, in-memory state)

- [done] R2. `nx-gs-adapter-api` MUST define
  `app.l2nx.gs.adapter.api.spi.ConnectContext` — immutable identity bundle passed to
  `AdapterModule.onConnect(ctx)`. **Phase 1** fields:
  - `UUID tenantId()`
  - `String tenantSlug()`
  - `UUID serverId()`
  - `String serverSlug()`
  - `String serverName()`
  - `String adapterVersion()`
    Java 8 POJO with hand-written builder; immutable once constructed.

  **Phase 2** extends with:
  - `Map<String, String> syncTopics()` — per-entity Kafka topic names delivered by
    the platform via `ConnectResponse.syncTopics` (see
    [`adapter-bootstrap` R16](../001-adapter-bootstrap.md)). Keyed by
    `entityName` (`"clan"`, `"character"`, …); value is the fully-qualified topic
    (`"bohpts.gs.sync.clans"`). Returns an immutable map view; modules treat it
    as read-only. Empty map (or absent field on legacy responses) is a valid
    value — modules decide their own response (`db-sync` transitions to
    `DISABLED`).
  - operator config access (read-only `AdapterConfig` view — same `l2nx.*` chain
    as adapter-bootstrap) and Kafka publish capability (narrow `EventPublisher`
    SAM — keeps `nx-gs-adapter-api` free of `nx-gs-kafka` dependency).
  - `NxEvents events()` and `NxCommands commands()` accessors for the
    built-in messaging surfaces. Both façades have **stable identity across
    reconnect cycles** — an internal `AtomicReference` is swapped to the
    live implementation on every reconnect, so module code acquired in
    `onConnect(ctx)` may continue using cached references after a
    disconnect/reconnect cycle without re-acquiring.
  - `Executor io()` — adapter-owned IO pool (`nx-io-N` daemon threads,
    sized by `l2nx.io.workers`, default `max(2, cores/2)`) for blocking
    JDBC / HTTP / FS calls from module code (non-handler). Symmetric with
    the handler-scoped `CommandContext.io()`.

  Phase 1 modules MUST NOT publish events or consult config — those capabilities
  ship intentionally later. `syncTopics` arrives in api/0.6.0 alongside the
  `EntityStats` types.

- [done] R3. `nx-gs-adapter-api` MUST define
  `app.l2nx.gs.adapter.api.kafka.ops.ModuleStatus` — value type for heartbeat enrichment:
  - `String name()` — matches `AdapterModule.name()`
  - `String state()` — one of `ACTIVE` | `DEGRADED` | `DISABLED` | `FAILED` (string,
    not enum, on the wire — keeps platform-side consumer decoupled from JVM enum
    ordinals)
  - `Stats stats()` — nested typed-slot bag for module-specific extras (never null;
    empty `Stats` when nothing to report)

  `ModuleStatus.Stats` (inner class — typed slots, JSON-friendly; modules populate
  only the slots they own):
  - **Phase 1**: `Optional<PoolStats> pool()` — pool stats from the module's
    underlying data source (db-sync surfaces `JdbcConnectionSource.stats()`).
  - **Phase 2**: `Optional<List<EntityStats>> entities()` — per-entity operational
    state for sync modules (one entry per synced entity: clan, character, item, …);
    populated by the CDC engine on every cycle (see
    [`cdc-engine` R10](../005-cdc-engine/spec.md)). Replaces the earlier placeholder
    `Optional<List<String>>` (names only) shape — the upgrade is driven by the
    cdc-engine slice. Entity-centric vocabulary (vs table-centric) reflects the
    adapter's domain: operators and platform consumers think in entities; the
    source table is an internal implementation detail of the schema provider.

  Plus three value types, all in `app.l2nx.gs.adapter.api.kafka.ops`:
  - `PoolStats { Integer active, Integer idle, Integer total, Integer waiting }` —
    JDBC pool stats. Fields are `Integer` (nullable) so providers that expose only a
    subset of HikariCP-style metrics can leave the rest `null`. **Renamed from the
    earlier `int busy` to `Integer active`** to match HikariCP / Tomcat JDBC / DBCP2
    naming conventions; `waiting` is added for consumers diagnosing pool backpressure.
  - `EntityStats { String name, EntityState state, Long rowCount, Long lastSyncEpochMs,
Long lastCycleDurationMs, ChangesSummary lastCycleChanges, Integer
consecutiveErrors }` — populated by `cdc-engine` per cycle; surfaced through the
    `Stats.entities` slot. `name` is the entity name (`"clan"`, `"character"`,
    `"item"`), not the source table name.
  - `EntityState` enum with two constants on the wire as uppercase strings:
    - `HEALTHY` — last cycle completed without error
    - `DEGRADED` — last cycle threw / Kafka publish failed / query timeout
    - (Earlier `SKIPPED` constant — for the per-entity RAM cap path — was
      removed when `cdc-engine` R8 stripped the cap. Engine no longer enforces
      a snapshot row-count limit; operators size the host JVM heap.)
  - `ChangesSummary { long created, long updated, long deleted }` — change counts
    from the last cycle.

- [done] R4. `NxAdapter.start()` MUST invoke `ServiceLoader.load(AdapterModule.class)`
  AFTER Kafka producer init success (after R6 of adapter-bootstrap). Result is cached
  in adapter-core for the JVM lifetime. Discovery is one-shot per JVM.

- [done] R5. After discovery, `NxAdapter` MUST run lifecycle dispatch:
  - **on connect:** for each discovered module: `module.onConnect(ctx)`. After all
    `onConnect` calls complete (in order), iterate again and call `module.start()` on
    each. Two-phase so modules that need cross-module references (rare) can resolve
    them before `start`.
  - **on shutdown:** `NxAdapter.shutdown()` iterates modules in **reverse discovery
    order**, calling `module.stop()` first on each, then iterates again calling
    `module.onDisconnect()`. Reverse order matches resource-acquisition ordering for
    future cross-module deps.

- [done] R6. `HeartbeatEvent` MUST carry `List<ModuleStatus> enabledModules`.
  `AdapterModule` defines a `default ModuleStatus currentStatus()` method returning
  `{name, registry-managed-state, empty Stats}`; modules override to surface module-
  specific stats (e.g. db-sync injects `pool` from `JdbcConnectionSource`). Adapter-
  core invokes `currentStatus()` per heartbeat tick, wraps the call in `SafeRunnable`,
  and falls back to `{name, FAILED, empty Stats}` if it throws (R7).

- [done] R7. Every entry into a module hook (`onConnect`, `start`, `stop`,
  `onDisconnect`, plus the per-tick `currentStatus()` if R6 lands as a module method)
  MUST be wrapped in `SafeRunnable` (existing helper from adapter-bootstrap). Throwable
  from any hook is caught at adapter-core level, logged via `NxLog`, the module is
  transitioned to internal `FAILED` state (and reflected in subsequent
  `ModuleStatus.state`), and the host JVM thread is never affected. Other modules keep
  running normally.

- [wip] R8. Released versions:
  - `nx-gs-adapter-api` `0.5.0` — initial Tier-1 SPI (`AdapterModule`,
    `ConnectContext`, `ModuleStatus`, `PoolStats { busy, idle, total }`).
  - `nx-gs-adapter-core` `0.3.0` (then `0.3.1`) — ServiceLoader call, lifecycle
    dispatch, heartbeat enrichment, resource-based version reporting.
  - **Pending `0.6.0`** for `nx-gs-adapter-api` — wire-shape break: `PoolStats.busy`
    → `PoolStats.active` rename, `PoolStats.waiting` added, `Stats.tables` upgrades
    to `Stats.entities` (`Optional<List<String>>` → `Optional<List<EntityStats>>`);
    new types `EntityStats`, `EntityState`, `ChangesSummary`. Entity-centric
    vocabulary throughout. No production heartbeat consumer yet — the `nx-tenants`
    consumer ships in lockstep, coordinated atomic upgrade is fine.

**Should:**

- [todo] R9. `NxAdapter` SHOULD expose `Map<String, AdapterModule> modules()` (or
  similar accessor) so host JVM code can introspect discovered modules
  programmatically — useful for ops dashboards / debugging UI in the host. Read-only
  view; mutation through this accessor is forbidden.

**Could:**

- [todo] R10. `NxAdapter` COULD expose `void registerModule(AdapterModule)` for
  programmatic module registration alongside ServiceLoader auto-discovery. Edge case:
  a host that wants to hand-pick modules at runtime instead of relying on classpath
  presence. Low priority.

**Non-goals:**

- **Specific module implementations** (`db-sync`, `dp-sync`, `metrics`) — their own
  features. They depend on this slice for the SPI contract; they ship their own
  `META-INF/services/app.l2nx.gs.adapter.api.spi.AdapterModule` descriptors.
- **Sub-tier SPIs inside modules** (`DbSchemaProvider`, `JdbcConnectionSource`) — those
  are concerns of the consumer modules; this feature only delivers Tier-1.
- **Per-module config switches** like `l2nx.modules.<name>.enabled=false`.
  Single-impl-per-classpath assumption: a module on the classpath WILL run. Operators
  who don't want a module remove the JAR.
- **Parallel module start** — sequential `onConnect` + `start` for predictability. A
  slow module blocks the next. Acceptable for MVP; revisit if a module's start time
  becomes a real problem.
- **Backward-compat for `HeartbeatEvent.enabledModules` wire-shape** — no
  platform-side consumers exist in production yet (the `nx-tenants` heartbeat consumer
  ships in lockstep with this slice). Coordinated atomic upgrade is fine.
- **Per-module shutdown timeout / forced kill** — `module.stop()` is invoked
  unconditionally; if it hangs, JVM shutdown hangs. Operator concern, not our scope
  for MVP.
- **Module dependency declaration** — modules cannot declare "I need `db-sync` to be
  active before me". If cross-module wiring is needed, modules look each other up via
  `NxAdapter.modules()` at `start`-time and degrade gracefully if absent.

## Open questions

- [resolved: `ModuleStatus` wire-shape — **nested** with typed `Stats` inner class
  (`pool` Phase 1; `tables` etc. forward). Cleaner extension story than flat or
  `Map<String,Object>`; consumers ignore unknown slots forward-compat.]
- [resolved: `currentStatus()` lives as **default method** on `AdapterModule` itself,
  returning `{name, registry-managed-state, empty Stats}`. Modules override only when
  they have non-empty stats to report (db-sync overrides with `pool`).]
- [resolved: Discovery order — **sorted by `module.name()`** (deterministic across
  restarts). Explicit ordering hooks added later if a real start-order dependency
  surfaces.]
- [resolved: `module.stop()` cap — **unlimited for MVP**. Host JVM stalls if a buggy
  module hangs; flipped to capped (with `Thread.interrupt()`) only if a real bug
  surfaces.]
- [assumed: `ConnectContext` is immutable — passed once at `onConnect`; modules cache
  what they need. If creds rotate (future feature), modules receive an `onReconnect`
  hook with a fresh `ConnectContext`; that's a separate slice.]
- [assumed: `state` on the wire is uppercase strings (`ACTIVE` etc.), matching
  `AdapterState` from adapter-bootstrap. Platform consumer treats unknown values as
  `UNKNOWN` (forward-compat for new states).]

## Links

- Sibling feature: [`docs/specs/001-adapter-bootstrap.md`](../001-adapter-bootstrap.md)
  — R7 (heartbeat shape) + Non-goals are revised when this feature ships
- First consumer: [`docs/specs/003-db-sync/spec.md`](../003-db-sync/spec.md)
  — `DbSyncModule` implements `AdapterModule` per this feature's R1
- Sibling feature (Tier-3 SPI):
  [`docs/specs/004-jdbc-connection-source.md`](../004-jdbc-connection-source.md)
  — `DbSyncModule.onConnect` consumes `ConnectContext` defined here
- Sibling feature (CDC engine):
  [`docs/specs/005-cdc-engine/spec.md`](../005-cdc-engine/spec.md) — populates
  `Stats.entities[]` per cycle via `EntityStats` + `EntityState` + `ChangesSummary`
  defined in this feature's R3

---

## Technical design

> Covers: spec.md
> Sibling: [`adapter-bootstrap spec`](../001-adapter-bootstrap.md) — owns
> `NxAdapter` skeleton this slice extends

### Overview

Tier-1 SPI delivered as `app.l2nx.gs.adapter.api.spi.AdapterModule` plus a single
ServiceLoader call site inside `NxAdapter` after Kafka producer init success. Lifecycle
dispatch (`onConnect → start` on connect, `stop → onDisconnect` on shutdown in reverse
order) lives in adapter-core; per-module exception isolation reuses `SafeRunnable` from
adapter-bootstrap. `HeartbeatEvent.enabledModules` wire-shape is upgraded from
`List<String>` to `List<ModuleStatus>` carrying name / state / pool stats / per-entity
stats — enriched per-tick by querying each discovered module's current status. The
`Stats.entities[]` slot is populated by [`cdc-engine`](../005-cdc-engine/spec.md) from
`EntityStats` / `EntityState` / `ChangesSummary` value types defined here (api/0.6.0).

### Structure

- `nx-gs-adapter-api/src/main/java/app/l2nx/gs/adapter/api/spi/`
  - `AdapterModule.java` — Tier-1 SPI interface (R1)
  - `ConnectContext.java` — immutable value type passed to
    `onConnect` (R2); Java 8 POJO with builder
- `nx-gs-adapter-api/src/main/java/app/l2nx/gs/adapter/api/kafka/ops/`
  - `ModuleStatus.java` — value type for heartbeat enrichment (R3); `Stats` inner
    class with `pool` slot and `entities` slot
  - `PoolStats.java` — value type carried inside `ModuleStatus.Stats.pool`. Fields:
    `Integer active`, `Integer idle`, `Integer total`, `Integer waiting` (all
    nullable). **`busy` field renamed to `active` in api/0.6.0** to match HikariCP /
    Tomcat JDBC / DBCP2 conventions; `waiting` added for backpressure diagnostics.
  - `EntityStats.java` — per-entity operational state populated
    by `cdc-engine` (R3, R10 in cdc-engine). Fields: `name` (entity name like
    `"clan"`, NOT source table), `state` (EntityState enum), `rowCount`,
    `lastSyncEpochMs`, `lastCycleDurationMs`, `lastCycleChanges` (ChangesSummary),
    `consecutiveErrors`.
  - `EntityState.java` — enum `HEALTHY | DEGRADED`; serialized as uppercase string
    on the wire (same convention as `ModuleStatus.state`). Earlier draft included a
    `SKIPPED` constant for the per-entity RAM cap path; cap was stripped from
    cdc-engine R8 and SKIPPED was removed in lockstep — cycles that would have
    `SKIPPED` now ride the operator's heap-sizing choice instead.
  - `ChangesSummary.java` — value type `{ long created, long
updated, long deleted }`.
  - `HeartbeatEvent.java` — carries `List<ModuleStatus> enabledModules` plus
    `tenantId`, `tenantSlug`, `serverSlug`, `serverName`, `adapterVersion`, `uptimeMs` (R6)
- `nx-gs-adapter-core/src/main/java/app/l2nx/gs/adapter/core/modules/`
  - `ModuleRegistry.java` — holds discovered modules + per-module
    `LifecycleState (HEALTHY | FAILED)`; owns the two-phase `onConnect → start`
    and reverse-order `stop → onDisconnect` dispatch; exposes
    `currentStatuses()` for heartbeat enrichment. Each hook invocation is wrapped
    in a try/catch (`invokeAndTrack`/`invokeIgnoringFailure`) — equivalent to the
    `SafeRunnable` pattern; a separate `ModuleLifecycleDispatcher` class was not
    needed for Phase 1 (logic fits inline).
- `nx-gs-adapter-core/src/main/java/app/l2nx/gs/adapter/core/NxAdapter.java`
  - extended — adds `ServiceLoader.load(AdapterModule.class)` call inside
    `initKafka()` after Kafka init success; delegates to `ModuleRegistry` for
    connect / shutdown phases; `HeartbeatService` consults
    `ModuleRegistry.currentStatuses()` per tick
- `nx-gs-adapter-core/src/main/java/app/l2nx/gs/adapter/core/heartbeat/HeartbeatService.java`
  - extended — populates `HeartbeatEvent.enabledModules` from a
    `Supplier<List<ModuleStatus>>` injected at construction
    (`ModuleRegistry::currentStatuses` in production)

### Key components

- **`AdapterModule`** (R1) — interface in `nx-gs-adapter-api`
  (`app.l2nx.gs.adapter.api.spi`). Five lifecycle methods (`name`, `onConnect`,
  `start`, `stop`, `onDisconnect`) plus a `default ModuleStatus currentStatus()`
  returning `{name, "ACTIVE", empty Stats}` — modules override only when they have
  state-or-stats to surface beyond the registry-managed lifecycle health.
- **`ConnectContext`** (R2) — immutable identity bundle passed to
  `onConnect`. Phase 1 fields: `tenantId` (UUID), `tenantSlug`, `serverId` (UUID),
  `serverSlug`, `serverName`, `adapterVersion`. Hand-written builder + final
  fields. Phase 2 (api/0.6.0+) extends with:
  - `Map<String, String> syncTopics()` — per-entity Kafka topic names from
    `ConnectResponse.syncTopics` (see
    [`adapter-bootstrap` R16](../001-adapter-bootstrap.md)); keyed by
    `entityName`. Immutable view; consumed by `db-sync` (`DbSyncModule.onConnect`)
    and forwarded into `cdc-engine`'s `TopicResolver`.
  - `NxEvents events()` and `NxCommands commands()` — façades for the built-in
    outbound-events + inbound-commands surfaces. Both have stable identity
    across reconnect cycles (internal `AtomicReference` swap), so modules
    acquire them once at `onConnect` and cache for the JVM lifetime.
  - `Executor io()` — adapter-owned IO pool (`nx-io-N` daemon threads,
    `l2nx.io.workers`) for blocking IO from module code (non-handler).
    Handler-scoped equivalent lives on `CommandContext.io()`.
  - `AdapterConfig` (read-only operator config view) and `EventPublisher` SAM
    for Kafka publish — kept out of Phase 1 to keep the SPI minimal and
    `nx-gs-kafka` out of the api artifact.
- **`ModuleStatus`** (R3) — wire-format value type. `name` (required) +
  `state` (required string: `ACTIVE` / `DEGRADED` / `DISABLED` / `FAILED`) +
  `Stats stats` (never null; empty `Stats` when nothing to report).
- **`ModuleStatus.Stats`** (R3) — nested typed-slot bag. Phase 1 carries
  `Optional<PoolStats> pool`; Phase 2 (driven by `cdc-engine`) adds
  `Optional<List<EntityStats>> entities` — replaces the earlier placeholder
  `Optional<List<String>> tables` (names only, table-centric). Modules populate only
  the slots they own — consumers ignore unknown slots forward-compat.
- **`PoolStats`** (R3) — `Integer active`, `Integer idle`, `Integer total`, `Integer
waiting`. All fields nullable so providers expose only what their pool API gives
  them (e.g. legacy pools that report only active+idle leave total/waiting null).
  Naming follows HikariCP MBean conventions.
- **`EntityStats`** (R3) — value type populated by `cdc-engine`
  per cycle. Fields: `name` (entity name like `"clan"` — NOT source table),
  `state` (`EntityState`), `rowCount` (last Phase-1 size — equal to source-table row
  count in the 1 entity = 1 table MVP shape), `lastSyncEpochMs`
  (`Instant.toEpochMilli` at successful end of cycle), `lastCycleDurationMs`,
  `lastCycleChanges` (`ChangesSummary`), `consecutiveErrors` (counter reset on
  `HEALTHY`).
- **`EntityState`** (R3) — enum `HEALTHY | DEGRADED`, serialized as uppercase string
  on the wire (same convention as `ModuleStatus.state`).
- **`ChangesSummary`** (R3) — `long created, long updated, long
deleted` from one cycle's diff; nested inside `EntityStats.lastCycleChanges`.
- **`ModuleRegistry`** (R4, R5, R6, R7) — adapter-core component holding the
  discovered `List<AdapterModule>` plus per-module `LifecycleState` (`HEALTHY` /
  `FAILED`). Provides:
  - `discover()` — runs `ServiceLoader.load` with TCCL save/restore; sorts by
    `name()` for deterministic order; one-shot per JVM
  - `connect(ConnectContext)` — two-phase `onConnect` then `start` dispatch;
    modules whose `onConnect` throws are marked `FAILED` and skipped in the
    `start` pass
  - `shutdown()` — reverse-order `stop` then `onDisconnect` dispatch;
    shutdown failures are logged but do not abort the sequence
  - `currentStatuses()` — snapshot of `ModuleStatus` per discovered module;
    `FAILED` modules return `{name, "FAILED", empty}` without invoking the module;
    a throwing `currentStatus()` falls back to the same shape
  - State transitions guarded by an internal lock so per-tick reads from the
    heartbeat thread don't race with shutdown
  - Hook invocations use `invokeAndTrack` / `invokeIgnoringFailure` (try/catch
    wrappers equivalent to `SafeRunnable`) — a separate
    `ModuleLifecycleDispatcher` class was not needed for Phase 1; the dispatch
    logic fits inline.
- **`SafeRunnable`** (existing in adapter-bootstrap) — reused for the connect /
  heartbeat / shutdown daemon entry points; the registry uses an inlined
  try/catch pattern with the same swallow-Throwable + log semantics.
- **`HeartbeatService`** (extended) — accepts a
  `Supplier<List<ModuleStatus>> moduleStatuses` at construction
  (`ModuleRegistry::currentStatuses` in production); per tick populates
  `enabledModules`, falling back to an empty list if the supplier throws.

### Data flows

#### 1. Discovery + connect (one-shot at adapter startup)

```
NxAdapter.start()
  → POST /connect → 200 → ConnectResponse parsed
  → KafkaInitializer → NxKafka producer ready
  → ModuleRegistry.discover()
       → ServiceLoader.load(AdapterModule.class)
       → cache discovered List<AdapterModule>, ordered (sort key TBD per
         spec Open question)
  → ModuleRegistry.connect(ConnectContext)
       → for each module:
           SafeRunnable.wrap(() -> module.onConnect(ctx)).run()
           on Throwable: state=FAILED, log, continue with next module
       → for each NON-failed module:
           SafeRunnable.wrap(() -> module.start()).run()
           on Throwable: state=FAILED, log, continue
  → HeartbeatService.start(...)
       → on each tick: enabledModules = ModuleRegistry.currentStatuses()
```

#### 2. Per-tick heartbeat enrichment

```
HeartbeatService tick
  → ModuleRegistry.currentStatuses()
       → for each module: SafeRunnable.wrap(module.currentStatus()) OR fallback to
         { name=module.name(), state=registry.stateOf(module) } if module didn't
         override (final shape per spec Open question)
       → return List<ModuleStatus>
  → build HeartbeatEvent { ..., enabledModules = statuses }
  → NxKafka.send(heartbeatTopic, serverId, event)
```

#### 3. Shutdown (reverse-order)

```
NxAdapter.shutdown()
  → ModuleRegistry.shutdown()
       → for each module IN REVERSE DISCOVERY ORDER:
           SafeRunnable.wrap(module.stop()).run()
           on Throwable: log, continue (already shutting down)
       → for each module IN REVERSE DISCOVERY ORDER:
           SafeRunnable.wrap(module.onDisconnect()).run()
           on Throwable: log, continue
  → close NxKafka producer
  → state = CLOSED
```

### Integration points

- **`:nx-gs-adapter-api`** (R1, R2, R3) — added `AdapterModule`,
  `ConnectContext`, `ModuleStatus` (+ `Stats`), `PoolStats`. Extended
  `HeartbeatEvent` shape (R6). Released as `0.5.0`. **`0.6.0` (in source, tag
  pending)**: `PoolStats` field rename (`busy` → `active`), `PoolStats.waiting`
  added, `Stats` slot upgrade from placeholder names-only `tables` to
  `Optional<List<EntityStats>> entities`, new types `EntityStats` / `EntityState` /
  `ChangesSummary`, `ConnectContext.syncTopics()` Phase-2 field, `Tier-2` SPI
  (`DbSchemaProvider`, `EntityMapping<T>`) co-located with Tier-1 / Tier-3 in
  `app.l2nx.gs.adapter.api.spi`, `HeartbeatEvent.uptime` → `uptimeMs`
  (millisecond unit) — all driven by `cdc-engine` / `db-sync`.
- **`:nx-gs-adapter-core`** (R4–R7) — added `ModuleRegistry`; extended
  `NxAdapter.initKafka` / `shutdown` and `HeartbeatService`. Released as `0.3.0`
  (resource-based version reporting added in `0.3.1`).
- **`adapter-bootstrap` feature** — R7 (heartbeat shape) and Non-goal
  ("ServiceLoader-based discovery deferred") get revised once this feature ships.
  No code changes inside adapter-bootstrap's domain — `SafeRunnable` is reused.
- **`db-sync` feature** — first consumer. `DbSyncModule implements AdapterModule`,
  registered via `META-INF/services/app.l2nx.gs.adapter.api.spi.AdapterModule`. Consumes
  `ConnectContext` in `onConnect`; produces `ModuleStatus` per tick.
- **`jdbc-connection-source` feature** — `BohptsJdbcConnectionSource` is a Tier-3
  SPI (different file), but it's invoked from inside `DbSyncModule.onConnect` which
  receives the `ConnectContext` defined here.
- **`cdc-engine` feature** — populates `Stats.entities[]` per CDC cycle via the
  `EntityStats` / `EntityState` / `ChangesSummary` types defined here in api/0.6.0.
  Engine code lives in `:nx-gs-db-sync-core`; the wire types stay in
  `:nx-gs-adapter-api` so platform consumers depend on a single contract artifact.
- **Platform-side `nx-tenants` heartbeat consumer** — must update its DTO to
  consume the new `ModuleStatus` shape in lockstep with this slice's release. No
  consumers in production yet; coordinated atomic upgrade is fine.

### Decisions

- **ServiceLoader, not annotation-scanning.** Same rationale as Tier-2 / Tier-3
  SPIs: zero-config discovery, no reflection-heavy classpath walk, plain JDK,
  consistent with the rest of the SPI tiers. Annotation scanning would require
  ClassGraph or similar — extra dep against the adapter's "min deps" principle.
- **Sequential `onConnect` + `start`, not parallel.** Predictable ordering matters
  more than startup speed for an embedded library. A slow module's start surfaces
  in heartbeat as the per-module `state` (still `INIT` or transitions to `FAILED`);
  parallel start would obscure failure attribution and complicate the lifecycle
  state machine.
- **Two-phase connect (`onConnect` for all, then `start` for all).** Lets modules
  that need cross-module references look each other up via `NxAdapter.modules()`
  AFTER `onConnect` completes for everyone, BEFORE anyone has actually started doing
  work. Rare today (db-sync doesn't reference other modules), but cheap to design
  in now.
- **Reverse-order shutdown.** Mirrors resource-acquisition ordering — last-acquired
  is first-released. Matters once cross-module deps exist (a metrics module that
  reads counters from db-sync should stop publishing metrics BEFORE db-sync
  releases its connection pool).
- **`String state` on the wire, not enum.** Platform-side consumer is decoupled
  from the JVM enum ordinals; new states can be added on the adapter side without
  breaking consumer dependency on a frozen ordinal map. Consumers treat unknown
  values as `UNKNOWN` for forward compatibility.
- **No backward-compat shim for `enabledModules` wire-shape change.** The platform
  has no production heartbeat consumers yet; the `nx-tenants` consumer ships
  alongside this slice. If/when external consumers exist, future shape changes need
  versioning — not this slice's problem.
- **`SafeRunnable` reused, not duplicated.** Already exists in adapter-bootstrap
  for connect / heartbeat / shutdown hooks. Same wrapper, same swallow-Throwable +
  log semantics, applied to the four `AdapterModule` lifecycle hooks plus the
  per-tick `currentStatus()`.
- **Discovery is one-shot per JVM.** ServiceLoader is invoked once at `NxAdapter.start`
  and the result cached. No re-discovery on reconnect — modules registered at JVM
  bootstrap are the modules that ever run. Simpler than tracking dynamic
  registration; matches the "drop a JAR on classpath, restart, done" operator
  experience.

- **Reconnect cycle: façades persist; identity is stable.** When the adapter
  re-handshakes (creds rotation, transient platform unavailability), the
  `NxEvents` / `NxCommands` façades exposed via `ConnectContext` are NOT
  re-issued. The same façade instance has its internal `AtomicReference`
  swapped to the live publisher / consumer pair, so module code that cached
  `ctx.events()` or `ctx.commands()` in `onConnect` continues working without
  re-acquiring. Handler registry survives reconnect too — handlers registered
  via `ctx.commands().on(...)` persist across reconnect cycles.
- **Single-impl-per-classpath.** No `>1 same module name` resolution rule — modules
  have unique names by convention; if two `AdapterModule` impls share a name, that's
  a packaging bug surfaced via the heartbeat (both appear with the same name) but
  not adapter's responsibility to detect at runtime.

### Extension points

- **New module type** — implement `AdapterModule`, drop a
  `META-INF/services/app.l2nx.gs.adapter.api.spi.AdapterModule` descriptor in the
  module's resources, ensure the module JAR is on the host classpath. Adapter
  picks it up at the next start. No code changes in `nx-gs-adapter-core` or
  any other module.
- **Module-specific heartbeat extras** — once the `ModuleStatus`-shape Open
  question is resolved, modules can attach module-specific data (e.g. db-sync
  per-table state, future metrics module's last-push timestamp).
- **Programmatic module registration** (R10 Could) — host JVM calls
  `NxAdapter.registerModule(myModule)` instead of relying on classpath presence.
  Useful for tests or hosts that compose modules dynamically. Optional future
  capability; not in MVP.
- **Module ordering hooks** — if MVP's sort-by-name proves insufficient (e.g. a
  module needs to start strictly after another), an `int order()` default method
  on `AdapterModule` can be added without breaking existing impls.
