# Adapter Modules — tech

> Covers: spec.md
> Sibling: [`adapter-bootstrap/tech.md`](../adapter-bootstrap/tech.md) — owns
> `NxAdapter` skeleton this slice extends

## Overview

Tier-1 SPI delivered as `app.l2nx.gs.adapter.api.spi.AdapterModule` plus a single
ServiceLoader call site inside `NxAdapter` after Kafka producer init success. Lifecycle
dispatch (`onConnect → start` on connect, `stop → onDisconnect` on shutdown in reverse
order) lives in adapter-core; per-module exception isolation reuses
`SafeRunnable` from adapter-bootstrap. `HeartbeatEvent.enabledModules` wire-shape is
upgraded from `List<String>` to `List<ModuleStatus>` carrying name / state / optional
pool stats — enriched per-tick by querying each discovered module's current status.

## Structure

- `nx-gs-adapter-api/src/main/java/app/l2nx/gs/adapter/api/spi/` [planned]
    - `AdapterModule.java` [planned] — Tier-1 SPI interface (R1)
    - `ConnectContext.java` [planned] — immutable value type passed to
      `onConnect` (R2); Java 8 POJO with builder
- `nx-gs-adapter-api/src/main/java/app/l2nx/gs/adapter/api/kafka/ops/` [planned]
    - `ModuleStatus.java` [planned] — value type for heartbeat enrichment (R3)
    - `PoolStats.java` [planned] — value type carried inside `ModuleStatus.Stats.pool`
    - `HeartbeatEvent.java` [modified] — `enabledModules` field type changes from
      `List<String>` to `List<ModuleStatus>` (R6)
- `nx-gs-adapter-core/src/main/java/app/l2nx/gs/adapter/core/modules/` [planned]
    - `ModuleRegistry.java` [planned] — internal: holds discovered modules + their
      runtime state (`ACTIVE | DEGRADED | DISABLED | FAILED`); exposes lifecycle
      dispatch and `currentStatuses()` for heartbeat
    - `ModuleLifecycleDispatcher.java` [planned] — internal: owns the two-phase
      `onConnect → start` and reverse-order `stop → onDisconnect` flows; wraps each
      hook in `SafeRunnable`
- `nx-gs-adapter-core/src/main/java/app/l2nx/gs/adapter/core/NxAdapter.java`
    - modified [planned] — adds `ServiceLoader.load(AdapterModule.class)` call after
      Kafka init success; delegates to `ModuleLifecycleDispatcher` for connect /
      shutdown phases; `HeartbeatService` consults `ModuleRegistry.currentStatuses()`
      per tick
- `nx-gs-adapter-core/src/main/java/app/l2nx/gs/adapter/core/heartbeat/HeartbeatService.java`
    - modified [planned] — populates `HeartbeatEvent.enabledModules` from
      `ModuleRegistry.currentStatuses()` instead of the empty list

## Key components

- **`AdapterModule`** [planned] (R1) — interface in `nx-gs-adapter-api`
  (`app.l2nx.gs.adapter.api.spi`). Five lifecycle methods (`name`, `onConnect`,
  `start`, `stop`, `onDisconnect`) plus a `default ModuleStatus currentStatus()`
  returning `{name, "ACTIVE", empty Stats}` — modules override only when they have
  state-or-stats to surface beyond the registry-managed lifecycle health.
- **`ConnectContext`** [planned] (R2) — immutable identity bundle passed to
  `onConnect`. Phase 1 fields: `tenantId` (UUID), `tenantSlug`, `serverId` (UUID),
  `serverSlug`, `serverName`, `adapterVersion`. Hand-written builder + final fields.
  Phase 2 will extend with `AdapterConfig` (read-only) and `EventPublisher` SAM
  for Kafka publish — kept out of Phase 1 to keep the SPI minimal and `nx-gs-kafka`
  out of the api artifact.
- **`ModuleStatus`** [planned] (R3) — wire-format value type. `name` (required) +
  `state` (required string: `ACTIVE` / `DEGRADED` / `DISABLED` / `FAILED`) +
  `Stats stats` (never null; empty `Stats` when nothing to report).
- **`ModuleStatus.Stats`** [planned] (R3) — nested typed-slot bag. Phase 1 carries
  `Optional<PoolStats> pool`; Phase 2 adds `Optional<List<String>> tables` once
  db-sync's `TableProvider` SPI lands. Modules populate only the slots they own —
  consumers ignore unknown slots forward-compat.
- **`PoolStats`** [planned] (R3) — `int busy`, `int idle`, optional `Integer total`.
  Surfaced when the module's data source exposes pool metrics; absent otherwise.
- **`ModuleRegistry`** [planned] (R4, R5, R6) — internal adapter-core component
  holding the discovered `List<AdapterModule>` and per-module runtime state. Provides:
    - `discover()` — runs `ServiceLoader.load`, populates the registry once
    - `connect(ConnectContext)` — two-phase `onConnect` then `start` dispatch
    - `shutdown()` — reverse-order `stop` then `onDisconnect` dispatch
    - `currentStatuses()` — snapshot of `ModuleStatus` per discovered module for
      heartbeat enrichment
    - State transitions are protected by `synchronized` blocks so per-tick reads from
      heartbeat thread don't race with shutdown
- **`ModuleLifecycleDispatcher`** [planned] (R5, R7) — encapsulates the
  two-phase / reverse-order semantics; every hook invocation is `SafeRunnable.wrap`'d
  so a Throwable from one module hook only marks that module `FAILED` — others
  continue.
- **`SafeRunnable`** (existing in adapter-bootstrap) — reused. No changes needed.
- **`HeartbeatService`** (modified) — adds one line per tick to populate
  `enabledModules` from `ModuleRegistry.currentStatuses()`.

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

- **`:nx-gs-adapter-api`** [planned] (R1, R2, R3) — adds `AdapterModule`,
  `ConnectContext`, `ModuleStatus`, `PoolStats` types. Modifies `HeartbeatEvent`
  field type. Bumped to next minor.
- **`:nx-gs-adapter-core`** [planned] (R4–R7) — adds `ModuleRegistry`,
  `ModuleLifecycleDispatcher`; modifies `NxAdapter.start` / `shutdown` and
  `HeartbeatService`. Bumped to next minor.
- **`adapter-bootstrap` feature** — R7 (heartbeat shape) and Non-goal
  ("ServiceLoader-based discovery deferred") get revised once this feature ships.
  No code changes inside adapter-bootstrap's domain — `SafeRunnable` is reused.
- **`db-sync` feature** — first consumer. `DbSyncModule implements AdapterModule`,
  registered via `META-INF/services/app.l2nx.gs.adapter.api.spi.AdapterModule`. Consumes
  `ConnectContext` in `onConnect`; produces `ModuleStatus` per tick.
- **`jdbc-connection-source` feature** — `BohptsJdbcConnectionSource` is a Tier-3
  SPI (different file), but it's invoked from inside `DbSyncModule.onConnect` which
  receives the `ConnectContext` defined here.
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
