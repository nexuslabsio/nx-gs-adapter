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

- [wip] R2. `nx-gs-adapter-api` MUST define
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
      [`adapter-bootstrap` R16](../adapter-bootstrap/spec.md)). Keyed by
      `entityName` (`"clan"`, `"character"`, …); value is the fully-qualified topic
      (`"bohpts.gs.sync.clans"`). Returns an immutable map view; modules treat it
      as read-only. Empty map (or absent field on legacy responses) is a valid
      value — modules decide their own response (`db-sync` transitions to
      `DISABLED`).
    - operator config access (read-only `AdapterConfig` view — same `l2nx.*` chain
      as adapter-bootstrap) and Kafka publish capability (narrow `EventPublisher`
      SAM — keeps `nx-gs-adapter-api` free of `nx-gs-kafka` dependency).

  Phase 1 modules MUST NOT publish events or consult config — those capabilities
  ship intentionally later. `syncTopics` arrives in api/0.6.0 alongside the
  `EntityStats` types.

- [wip] R3. `nx-gs-adapter-api` MUST define
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
      [`cdc-engine` R10](../cdc-engine/spec.md)). Replaces the earlier placeholder
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

- Sibling feature: [`docs/features/adapter-bootstrap/spec.md`](../adapter-bootstrap/spec.md)
  — R7 (heartbeat shape) + Non-goals are revised when this feature ships
- First consumer: [`docs/features/db-sync/spec.md`](../db-sync/spec.md)
  — `DbSyncModule` implements `AdapterModule` per this feature's R1
- Sibling feature (Tier-3 SPI):
  [`docs/features/jdbc-connection-source/spec.md`](../jdbc-connection-source/spec.md)
  — `DbSyncModule.onConnect` consumes `ConnectContext` defined here
- Sibling feature (CDC engine):
  [`docs/features/cdc-engine/spec.md`](../cdc-engine/spec.md) — populates
  `Stats.entities[]` per cycle via `EntityStats` + `EntityState` + `ChangesSummary`
  defined in this feature's R3
