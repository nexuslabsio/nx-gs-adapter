# DB Sync

## Problem

Game-server cores (L2J / Lucera / Essence forks) store player / clan / item state in MySQL,
and the schema differs per fork (vanilla L2J vs Lucera) and per client patch (column renames,
custom columns, custom converters). The L2NX platform needs that state flowing live into
Kafka so player-facing UI and operator dashboards can show real data — but the adapter can't
ship one hardcoded set of SQL queries.

This slice introduces a generic CRC32-CDC engine plus a two-tier SPI so vendors and clients
can plug in their own schema descriptions. MVP target: a single bohpts client implementation
syncing the `clan_data` table end-to-end. The schema-provider code lives directly in the
client's own repo (`bohpts-core`), not as a separate published artifact — the SPI hosts
multi-tenant schema variants without forcing per-client Maven Central publishing. Vanilla
`nx-gs-db-l2j` is intentionally deferred until a second non-bohpts customer arrives (premature
extraction is YAGNI). Audience: operators (drop in `nx-gs-db-sync-core` JAR + a host JAR that
ships a `DbSchemaProvider`), platform-side consumers of `gs.sync.*` Kafka topics, future
module authors (datapack sync, metrics).

## Requirements

> **Sibling features carry the SPI plumbing and the engine algorithm:**
> - Tier-1 SPI (`AdapterModule` + ServiceLoader wire-up + `enabledModules` on
    > `HeartbeatEvent`) lives in [`adapter-modules`](../adapter-modules/spec.md).
> - Tier-3 SPI (`JdbcConnectionSource`) lives in
    > [`jdbc-connection-source`](../jdbc-connection-source/spec.md) (interface in
    > `nx-gs-adapter-api` package `app.l2nx.gs.adapter.api.spi`).
> - The CRC32 two-phase CDC algorithm — scheduler, in-memory snapshot, RAM cap, query
    > timeout, MIN/MAX recompute, sliding windows, per-table stats publishing — lives in
    > [`cdc-engine`](../cdc-engine/spec.md). db-sync owns the AdapterModule wiring, the
    > Tier-2 SPI shape (`DbSchemaProvider` / `TableMapping`), and bohpts MVP. The engine
    > consumes `TableMapping`s from db-sync's resolved `DbSchemaProvider` and runs the
    > protocol uniformly.
>
> All requirements below assume the three sibling features are in place.

> **Phase split:**
> - **Phase 1 (smoke test):** R1, R2 (smoke borrow only — not CDC), R9 (module-level
    > exception isolation), R11 (versions), R12 (heartbeat enrichment with `pool` slot only;
    > no `tables` slot yet). Goal: bohpts-core boots, registers
    > `BohptsJdbcConnectionSource`, db-sync surfaces
    > `{name: "db-sync", state: ACTIVE, stats: {pool: ...}}` in heartbeat.
> - **Phase 2 (CDC engine):** R3, R4, R5, R10, plus R12 upgrade to full `tables: List<TableStats>`
    > (per `cdc-engine` R10). Adds `DbSchemaProvider` Tier-2 SPI, bohpts `clan_data`
    > mapping, and wires the cdc-engine — engine R lands in the cdc-engine spec, NOT here.
> - **Could (post-MVP):** none directly on db-sync (R14 / R15 moved to cdc-engine).

**Must:**

- [done] R1. **[Phase 1]** `nx-gs-db-sync-core` MUST implement
  `app.l2nx.gs.adapter.api.spi.AdapterModule` and ship a
  `META-INF/services/app.l2nx.gs.adapter.api.spi.AdapterModule` descriptor pointing to its
  module class — so adding the JAR to the host classpath alongside `nx-gs-adapter-core` is
  the only step required for the engine to start.
    - SC1. Discovery is zero-config — operator does NOT set any `l2nx.modules=...` system
      property to enable db-sync; presence on the classpath is sufficient.
    - SC2. `AdapterModule.name()` returns the literal string `"db-sync"` — surfaced in
      `HeartbeatEvent.enabledModules`.

- [wip] R2. **[Phase 1: smoke borrow only / Phase 2: CDC borrows]** `nx-gs-db-sync-core`
  MUST borrow JDBC `Connection`s from a `JdbcConnectionSource` resolved per the resolution
  chain in the `jdbc-connection-source` feature (Phase 1: Path 1 host SPI only / Phase 3:
  add Path 2 bundled-Hikari fallback / FAILED if neither). The engine itself does NOT
  distinguish the path — it just borrows. **Phase 1**: a single smoke check on
  `onConnect` — borrow once, `setReadOnly(true)`, `isValid(5)`, close. Success → module
  state `ACTIVE`; smoke fails → `DEGRADED`. **Phase 2**: every CDC query calls
  `setReadOnly(true)` immediately after borrow (belt-and-suspenders with `GRANT SELECT`
  recommendation). No DDL or DML is ever issued. Connections are returned via
  `try-with-resources`.
    - SC3. Engine performs `setReadOnly(true)` per-borrow regardless of pool-level
      configuration — works identically across Path 1 (host pool, may or may not be
      pool-level read-only) and Path 2 (bundled pool, pool-level read-only is set).

- [done] R3. **[Phase 2]** `nx-gs-db-sync-core` MUST consume the Tier-2 SPI
  `DbSchemaProvider` (defined in `nx-gs-adapter-api` package
  `app.l2nx.gs.adapter.api.spi`, alongside Tier-1 `AdapterModule` and Tier-3
  `JdbcConnectionSource` — provider authors depend only on `nx-gs-adapter-api`,
  not on `nx-gs-db-sync-core`) and discover impls via
  `ServiceLoader.load(DbSchemaProvider.class)` once at module `start()`. Selection rule:
    - **0 impls** → log actionable WARN, db-sync transitions to `DISABLED` (other modules and
      the host JVM keep running)
    - **1 impl** → engine uses it (the dominant case)
    - **>1 impls** → log actionable ERROR listing the conflicting impl class names; db-sync
      transitions to `FAILED`. **Caveat:** when a client-override JAR (e.g. `nx-gs-db-bohpts`)
      brings the vanilla `nx-gs-db-l2j` JAR transitively, both ship a service descriptor — see
      Open question on resolution strategy. MVP assumes operator's classpath has exactly one
      activated descriptor.

- [done] R4. **[Phase 2]** `DbSchemaProvider` interface MUST expose:
    - `String schemaName()` — e.g. `"l2j"`, `"bohpts"`, `"lucera"` (informational; not a
      selection key in MVP)
    - `List<TableMapping<?>> mappings()` — the tables this provider knows about

- [done] R5. **[Phase 2]** `EntityMapping<T>` interface MUST describe ONE synced
  entity (clan, character, item, …). The adapter's domain vocabulary is entity-centric
  — operators and platform consumers think in entities, not in DB tables; the table is
  an internal-to-the-mapping detail. MVP enforces 1 entity = 1 source table; future
  multi-table entities are an extension point.
    - `String entityName()` — domain identifier in singular form: `"clan"`,
      `"character"`, `"item"`. Used as the lookup key into `ConnectResponse.syncTopics`
      to resolve the Kafka topic for this entity (per [`adapter-bootstrap`
      R16](../adapter-bootstrap/spec.md)). Surfaced through heartbeat as
      `EntityStats.name`.
    - `String tableName()` — source SQL table for the entity (e.g. `"clan_data"` for
      the `"clan"` entity). Internal to the mapping — used only by the engine's
      SQL `FROM` clause and `setReadOnly`-bounded SELECT statements; never appears on
      the wire.
    - `String pkColumn()` — primary-key column on `tableName` (single-column numeric
      PK assumption — see Non-goals). The engine reads PK values as `long` via
      `rs.getLong(pkColumn())` and binds them via `setLong(...)`. PK is `long`
      end-to-end — engine internals (fastutil `Long2IntOpenHashMap`), Kafka key
      (`LongSerializer`), and `SyncEvent.pk: long` payload all carry the raw long.
      See cdc-engine R1 + R12.
    - `List<String> hashedColumns()` — columns whose values feed CRC32 in Phase 1
    - `T mapRow(ResultSet rs)` — Phase 2 row → DTO conversion (called once per changed
      row). Schema providers convert source-column values to wire types in `mapRow`;
      ID-column types in DTOs are `Long` (matching the `long` PK invariant).
    - `Class<T> dtoType()` — DTO class for serialization

  **Notes:**
    - `EntityMapping` describes ONLY the schema shape (what to sync). All
      operational parameters (cadence, window size, timeouts) come from
      `l2nx.properties` per [`cdc-engine` R15](../cdc-engine/spec.md). The SPI does
      NOT carry `tickInterval()` / `strategy()` / `windowCount()` /
      `queryTimeout()` / `maxRows()` — these would mix operator concerns into the
      schema-provider contract.
    - Kafka topic names are NOT declared by the schema provider either; they
      arrive from the platform via `ConnectResponse.syncTopics` keyed by
      `entityName()`. See [`adapter-bootstrap` R16](../adapter-bootstrap/spec.md)
      and Decisions in tech.md.

> **R6, R7, R8 — moved to [`cdc-engine`](../cdc-engine/spec.md).** The CRC32 two-phase
> protocol (engine R1), scheduler semantics (cdc-engine R5), and initial-sync flow
> (cdc-engine R7) are owned by the engine slice. db-sync wires the engine with the
> resolved `DbSchemaProvider` at Phase 2 start. Numbers R6 / R7 / R8 are intentionally
> left as gaps here — not reused per the SpecKit "deleted numbers stay deleted" rule.

- [done] R9. **[Phase 1: module-level only / Phase 2: per-table]** db-sync engine MUST NOT
  propagate exceptions to host-JVM threads. **Phase 1**: only module-level handling —
  `onConnect` failures (SPI not resolved / smoke check throws) are caught and surface as
  `FAILED` / `DEGRADED` per R2. **Phase 2**: every CDC entry point (scheduler tick, Kafka
  producer callback, `JdbcConnectionSource.getConnection()` failure) catches `Throwable`,
  logs via `NxLog`, and transitions the **affected table** to `DEGRADED` — other tables
  continue ticking. Module-level `FAILED` is reserved for non-recoverable conditions:
  0 / >1 `JdbcConnectionSource`; >1 `DbSchemaProvider` (Phase 2); no `l2nx.db.*` fallback
  config when ServiceLoader returns 0 (Phase 3); bundled Hikari pool fails to open
  (Phase 3).

- [todo] R10. **[Phase 2]** Bohpts client + clan entity MVP — `bohpts-core` repo
  (private; `E:/bohpts/code/bohpts-core`) MUST host a `BohptsDbSchemaProvider` class
  implementing `DbSchemaProvider` directly (no `extends` — there is no vanilla
  `nx-gs-db-l2j` to inherit from in MVP), plus a
  `META-INF/services/app.l2nx.gs.adapter.api.spi.DbSchemaProvider` resource pointing to
  it. Bohpts-core declares `implementation 'app.l2nx:nx-gs-adapter-api:0.6.0'` from
  Maven Central. Provider contract:
    - `schemaName()` = `"bohpts"`
    - `mappings()` returns exactly one `EntityMapping<ClanDto>` for the `clan`
      entity:
        - `entityName()` = `"clan"`
        - `tableName()` = `"clan_data"` (source SQL table; internal to mapping)
        - `pkColumn()` = `"clan_id"`
        - `hashedColumns()` = `["clan_name", "clan_level", "leader_id", "ally_id"]`
          (4 plain columns — crest_id, ally_name, ally_crest_id, the four
          `*_penalty_*` / `*_expiry_time` fields and the `membersCount` formula
          are intentionally out-of-scope for MVP per Non-goals)
        - `dtoType()` = `ClanDto.class`
        - (no `tickInterval` / strategy / windowCount fields — engine config
          per [`cdc-engine` R15](../cdc-engine/spec.md))
    - `ClanDto` ships in `nx-gs-adapter-api` (Java 8 POJO, hand-written builder) so
      the platform-side consumer compiles against the same wire type. Field types
      mirror DB nullability — primitives for `NOT NULL` columns, boxed for
      nullable. ID fields are `long`/`Long` end-to-end (matches the engine's `long`
      PK invariant — platform stores PKs as `long`):
        - `long clanId` (PK, `NOT NULL`)
        - `String clanName` (`NOT NULL`)
        - `int clanLevel` (`NOT NULL`, source default `0`)
        - `Long leaderId` (null when source `leader_id = 0` per L2J convention)
        - `Long allyId` (null when source `ally_id = 0`)
    - The package for `BohptsDbSchemaProvider` inside bohpts-core is operator-chosen
      — see Open question. No bohpts-internal class names or column conventions leak
      into this monorepo (bohpts-core is private; this monorepo stays open-core).
    - The Kafka topic the engine publishes `clan` events to is delivered by the
      platform via `ConnectResponse.syncTopics["clan"]` (e.g.
      `"bohpts.gs.sync.clans"`) — NOT declared in the mapping.

- [wip] R11. **[Phase 1]** First published versions:
    - `nx-gs-db-sync-core` = `0.1.0` (new module in this monorepo, published to Maven Central).
      Phase 1 ships with `DbSyncModule` only (no CDC engine, no DbSchemaProvider — Phase 2
      bumps the minor). Phase 3 adds `BundledHikariConnectionSource`.
    - `nx-gs-adapter-api` bumped to next minor (adds `AdapterModule`, `ConnectContext`,
      `ModuleStatus` + `Stats` + `PoolStats`, `JdbcConnectionSource`). `SyncEvent` /
      `ClanDto` ship in the Phase 2 minor bump.
    - **No** `nx-gs-db-bohpts` artifact is published — the bohpts schema provider is shipped
      as part of the bohpts-core game-server JAR itself.

**Should:**

- [done] R12. **[Phase 1: pool only / Phase 2: + entities]** db-sync SHOULD surface
  per-module health on `HeartbeatEvent.enabledModules` via `ModuleStatus` (defined
  in `adapter-modules`). **Phase 1**: `{name: "db-sync", state: "ACTIVE", stats:
  {pool: {active, idle, total, waiting}}}` — pool stats forwarded from
  `JdbcConnectionSource.stats()`. Field rename `busy` → `active` lands in api/0.6.0
  (HikariCP / DBCP2 / Tomcat JDBC convention); `waiting` field added for
  pool-backpressure diagnostics. **Phase 2**: `stats.entities` slot populated with
  `List<EntityStats>` (`{name, state, rowCount, lastSyncEpochMs,
  lastCycleDurationMs, lastCycleChanges, consecutiveErrors}`) populated by the CDC
  engine on every cycle — see [`cdc-engine` R10](../cdc-engine/spec.md). Replaces
  the earlier placeholder `tables: ["clan_data"]` (names only, table-centric)
  shape.

> **R13 (RAM cap) — REMOVED.** Earlier draft had db-sync (and cdc-engine R8) enforce
> a per-entity row-count cap. The CDC engine no longer enforces a cap — operators
> size the host JVM heap to fit the configured entities. See [`cdc-engine` R8 strip
> note](../cdc-engine/spec.md).

**Could:**

> **R14 — moved to [`cdc-engine` R15 (Must)](../cdc-engine/spec.md).** Operator-side
> cadence tuning lives on the engine: `l2nx.cdc-engine.tick-interval-seconds` /
> `l2nx.cdc-engine.window-count` global overrides + R16 startup log of resolved
> values. Per-table granularity is intentionally NOT in MVP — comes via cdc-engine
> R14 (Could, dynamic Kafka config) when a real ops case demands it.

> **R15 (formerly `SLIDING_WINDOW` Could) — REMOVED.** The CDC engine ships a
> single windowed scan algorithm (`l2nx.cdc-engine.rows-per-window`, default
> 500_000); small entities collapse to 1 window naturally. No `FULL_SCAN` /
> `SLIDING_WINDOW` enum, no `mapping.strategy()`. See
> [`cdc-engine` R2](../cdc-engine/spec.md).

- [done] R16. **[Phase 2]** `DbSyncModule.onConnect` MUST consult
  `ctx.syncTopics()` (per [`adapter-modules` R2](../adapter-modules/spec.md) and
  [`adapter-bootstrap` R16](../adapter-bootstrap/spec.md)) before instantiating the
  CDC engine. Behavior:
    - `null` or empty `syncTopics` map → log actionable WARN (`"no entity topics in
      ConnectResponse — db-sync has nothing to sync"`), transition the module to
      `DISABLED` state, no engine instantiated, no scheduler threads started, the
      pool is released. Defensive path; not expected in steady-state operation
      (the platform always delivers a non-empty map for tenants with sync enabled).
    - Non-empty map → pass it into the engine via `TopicResolver` (see
      [`cdc-engine` R17](../cdc-engine/spec.md)). The engine handles per-entity
      missing-topic situations on its own (entity → `DEGRADED`); the module-level
      `DISABLED` triage is reserved for the all-empty case only.

**Non-goals:**

- **Schema discovery / introspection** — the adapter does not query the host DB's
  `information_schema`. `EntityMapping` is the source of truth.
- **Composite PKs** — single-column PK assumption. Entities with multi-column PKs
  are out of scope for MVP; can be added as `CompositeEntityMapping` later.
- **DML** on the host DB — read-only. Hikari pool is `setReadOnly(true)`, MySQL user is
  expected to have `GRANT SELECT` only.
- **Backfill control from platform** — no operator-triggered "resync from scratch".
  Snapshot is wiped on `onDisconnect` and rebuilt on next `onConnect`.
- **Vanilla `nx-gs-db-l2j` artifact** — deferred until a second non-bohpts customer arrives.
  Until then, premature extraction of "common L2J vanilla code" is YAGNI: there is no
  evidence yet for what's actually shareable across forks. When the second customer ships,
  common code is extracted into `nx-gs-db-l2j` and bohpts is refactored to extend it via
  template method.
- **Vanilla Lucera support (`nx-gs-db-lucera`)** — separate slice (next vanilla provider
  after the SPI design is validated by bohpts MVP).
- **Datapack sync (`nx-gs-dp-*`)** — separate slice; uses the same Tier-1 SPI but a
  different engine (file-based diffing, not CRC32-CDC).
- **Bohpts-specific columns** — `crest_id`, `ally_name`, `ally_crest_id`,
  `ally_penalty_type`, `ally_penalty_expiry_time`, `char_penalty_expiry_time`,
  `dissolving_expiry_time`, and the `membersCount` Hibernate `@Formula` are NOT synced in
  MVP. Goal is plain-data smoke test of the SPI + CDC engine, not feature-complete clan
  data. Penalty/expiry/crest fields and member count come in a follow-up slice.
- **Transactional consistency between Phase 1 and Phase 2** — eventual consistency is
  acceptable. A row deleted between phases simply doesn't appear in Phase 2 results; the
  deletion is detected on the next Phase 1 cycle. The alternative (REPEATABLE READ for the
  whole cycle) would hold a long-running transaction on the host DB — operator-hostile.
- **Bundling a JDBC driver** — host JVM is expected to provide the MariaDB / MySQL driver
  (game-server core needs it anyway). Avoids classpath conflicts with the host's own driver
  version.

## Open questions

- [resolved: Topic naming — per-entity topics, names supplied by the platform via
  `ConnectResponse.syncTopics: Map<entityName, topic>` (see
  [`adapter-bootstrap` R16](../adapter-bootstrap/spec.md)). Adapter does not
  construct topic names. Earlier "per-table vs single-topic" debate moot — the
  platform delivers explicit per-entity topics and is free to choose its own naming
  policy.]
- [resolved: Strategy + cadence — engine config from `l2nx.properties` only per
  [`cdc-engine` R15](../cdc-engine/spec.md); no provider-side declarations on
  `EntityMapping`. Single global `tick-interval-seconds` / `rows-per-window` /
  `query-timeout-seconds` / `publish-flush-seconds`. No `mapping.strategy()` —
  engine has a single windowed strategy. Dynamic Kafka-driven per-entity config
  remains a future Could on cdc-engine R14.]
- [resolved: Multi-impl `DbSchemaProvider` resolution is a non-issue in MVP. Bohpts-core
  ships exactly one provider; no transitive vanilla JAR exists yet (`nx-gs-db-l2j` deferred
  per Non-goals). The conflict scenario re-emerges only once vanilla L2J ships AND a
  customer takes a transitive dep on it. At that point the resolution strategy (config
  selector / shadow exclusion / vanilla activator JAR) is decided in the second-customer
  feature slice. MVP keeps R3's fail-loud single-impl rule.]
- [resolved: All ID fields serialized as `Long` on the wire — PK + FK columns in
  DTOs + `SyncEvent.pk`. Engine reads PK via `rs.getLong(pkColumn)`, Phase 2 binds
  via `setLong(...)`, Kafka key uses `LongSerializer` (8 bytes). Platform stores PK
  as `long`. Earlier "all IDs as String for cross-schema invariance" stance reversed
  — cross-schema (UUID/composite) is an explicit Non-goal in MVP; when those
  schemas arrive a parallel String-PK / composite-PK wire variant ships separately.
  See `cdc-engine` Decisions.]
- [resolved: Kafka key for sync events — the row's `long` PK serialized via
  `LongSerializer` (8 bytes). Same row → same partition → ordering guarantee per
  row. Topic name itself encodes the (tenant, entity) tuple, so the key needs only
  the row identifier.]
- [resolved: `SyncEvent` is **typed** — `SyncEvent<T>` parameterized by DTO type.
  Platform-side consumer compiles against `SyncEvent<ClanDto>` and gets compile-time
  payload guarantees. Adding a new entity bumps the api artifact (ship the new DTO)
  and the platform consumer upgrades in lockstep — coordinated upgrade is acceptable
  for the small entity catalog. Erased shape (`String payloadJson`) was rejected:
  payload must be re-parsed by every consumer and shows up as an escaped JSON string
  on the wire. Engine R12 in cdc-engine reflects this.]
- [NEEDS CLARIFICATION: Module rename `nx-gs-adapter-db-*` → `nx-gs-db-*`. Affects future
  siblings: `nx-gs-db-l2j`, `nx-gs-db-lucera`, `nx-gs-dp-l2j`, `nx-gs-dp-lucera`. Update
  README + CLAUDE.md siblings list. Confirmed for `nx-gs-db-sync-core` already;
  sweep the rest before the first vanilla module ships.]
- [NEEDS CLARIFICATION: Java package for `BohptsDbSchemaProvider` inside bohpts-core.
  Candidates: `l2e.gameserver.nx.db` (matches existing bohpts-core convention),
  `app.l2nx.gs.db.bohpts` (uses L2NX namespace inside bohpts), `com.bohpts.gs.l2nx`. Up to
  bohpts-core owner; no impact on the SPI contract.]
- [assumed: `AdapterModule.name()` literal = `"db-sync"`. Used by heartbeat
  `enabledModules`.]
- [assumed: SyncEvent send semantics — events are fire-and-forget; if Kafka send fails
  (broker unreachable) the engine logs and moves on. Snapshot is NOT advanced for failed
  publishes, so the next tick re-detects the same diff and retries. Eventual consistency.]

## Links

- Sibling feature: [`docs/features/adapter-bootstrap/spec.md`](../adapter-bootstrap/spec.md)
  — Tier-1 SPI extension lands there
- Sibling feature (Tier-3 SPI):
  [`docs/features/jdbc-connection-source/spec.md`](../jdbc-connection-source/spec.md)
  — `JdbcConnectionSource` design, pool-agnostic borrowing, bohpts reference impl
  wrapping `l2e.gameserver.database.DatabaseFactory`
- Sibling feature (CDC algorithm):
  [`docs/features/cdc-engine/spec.md`](../cdc-engine/spec.md) — CRC32 two-phase
  protocol, single windowed strategy with `rows-per-window` partition, scheduler,
  query timeout, per-entity heartbeat stats
- Module discovery diagrams: [`module-discovery.md`](./module-discovery.md)
- CRC32 CDC resource estimates: image attached to /specl-take invocation (bohpts
  x20 benchmark — Characters 152k / Clans 1k / Items 12M — informs window-count
  math in cdc-engine tech.md)
