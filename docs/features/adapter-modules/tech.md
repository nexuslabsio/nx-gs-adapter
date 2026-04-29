# Adapter Modules — tech

> Covers: spec.md
> Sibling: [`adapter-bootstrap/tech.md`](../adapter-bootstrap/tech.md) — owns
> `NxAdapter` skeleton this slice extends

## Overview

Tier-1 SPI delivered as `app.l2nx.gs.adapter.api.spi.AdapterModule` plus a single
ServiceLoader call site inside `NxAdapter` after Kafka producer init success. Lifecycle
dispatch (`onConnect → start` on connect, `stop → onDisconnect` on shutdown in reverse
order) lives in adapter-core; per-module exception isolation reuses `SafeRunnable` from
adapter-bootstrap. `HeartbeatEvent.enabledModules` wire-shape is upgraded from
`List<String>` to `List<ModuleStatus>` carrying name / state / pool stats / per-entity
stats — enriched per-tick by querying each discovered module's current status. The
`Stats.entities[]` slot is populated by [`cdc-engine`](../cdc-engine/spec.md) from
`EntityStats` / `EntityState` / `ChangesSummary` value types defined here (api/0.6.0).

## Structure

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

## Key components

- **`AdapterModule`** (R1) — interface in `nx-gs-adapter-api`
  (`app.l2nx.gs.adapter.api.spi`). Five lifecycle methods (`name`, `onConnect`,
  `start`, `stop`, `onDisconnect`) plus a `default ModuleStatus currentStatus()`
  returning `{name, "ACTIVE", empty Stats}` — modules override only when they have
  state-or-stats to surface beyond the registry-managed lifecycle health.
- **`ConnectContext`** (R2) — immutable identity bundle passed to
  `onConnect`. Phase 1 fields: `tenantId` (UUID), `tenantSlug`, `serverId` (UUID),
  `serverSlug`, `serverName`, `adapterVersion`. Hand-written builder + final
  fields. Phase 2 (api/0.6.0) extends with:
    - `Map<String, String> syncTopics()` — per-entity Kafka topic names from
      `ConnectResponse.syncTopics` (see
      [`adapter-bootstrap` R16](../adapter-bootstrap/spec.md)); keyed by
      `entityName`. Immutable view; consumed by `db-sync` (`DbSyncModule.onConnect`)
      and forwarded into `cdc-engine`'s `TopicResolver`.
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

## Data flows

### 1. Discovery + connect (one-shot at adapter startup)

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

### 2. Per-tick heartbeat enrichment

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

### 3. Shutdown (reverse-order)

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

## Integration points

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

## Decisions

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
- **Single-impl-per-classpath.** No `>1 same module name` resolution rule — modules
  have unique names by convention; if two `AdapterModule` impls share a name, that's
  a packaging bug surfaced via the heartbeat (both appear with the same name) but
  not adapter's responsibility to detect at runtime.

## Extension points

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
