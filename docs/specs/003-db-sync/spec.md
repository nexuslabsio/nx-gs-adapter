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
    > `HeartbeatEvent`) lives in [`adapter-modules`](../002-adapter-modules/spec.md).
> - Tier-3 SPI (`JdbcConnectionSource`) lives in
    > [`jdbc-connection-source`](../004-jdbc-connection-source.md) (interface in
    > `nx-gs-adapter-api` package `app.l2nx.gs.adapter.api.spi`).
> - The CRC32 two-phase CDC algorithm — scheduler, in-memory snapshot, RAM cap, query
    > timeout, MIN/MAX recompute, sliding windows, per-table stats publishing — lives in
    > [`cdc-engine`](../005-cdc-engine/spec.md). db-sync owns the AdapterModule wiring, the
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
  `onConnect` — borrow once, `isValid(5)`, close. Success → module state `ACTIVE`;
  smoke fails → `DEGRADED`. **Phase 2**: read-only enforcement is at the SQL level
  (`START TRANSACTION ... READ ONLY` wrapping every Phase 1 / Phase 2 query — see
  [`cdc-engine` R11](../005-cdc-engine/spec.md)); the engine does NOT call
  `setReadOnly(true)` on the borrowed connection (the host pool may not reset it on
  return, and consumers borrowing from the same pool would inherit the flag). No DDL
  or DML is ever issued. Connections are returned via `try-with-resources`.
    - SC3. Engine relies on `START TRANSACTION ... READ ONLY` per window, not on
      connection-level `setReadOnly`. Pool-level read-only is the host's choice (see
      `jdbc-connection-source` for the host-pool contract).

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

- [wip] R5. **[Phase 2]** `EntityMapping<T>` interface MUST describe ONE synced
  entity (clan, character, item, …). The adapter's domain vocabulary is entity-centric
  — operators and platform consumers think in entities, not in DB tables; the source
  tables are internal-to-the-mapping details. An entity may be assembled from
  multiple source tables: one **primary source** (drives windowing + identity) and
  zero-or-more **child sources** (each with FK back to primary's PK).
    - `String entityName()` — domain identifier in singular form: `"clan"`,
      `"character"`, `"item"`. Used as the lookup key into `ConnectResponse.syncTopics`
      to resolve the Kafka topic for this entity (per [`adapter-bootstrap`
      R16](../001-adapter-bootstrap.md)). Surfaced through heartbeat as
      `EntityStats.name`.
    - `Class<T> dtoType()` — DTO class for serialization. Concrete,
      non-parameterized class (Gson serializes the typed payload slot directly).
    - `PrimarySource<?> primary()` — the source table that drives windowing and
      defines entity identity. Single-column numeric PK assumption (`long`):
      engine reads PK values as `long` via `rs.getLong(pkColumn())` and binds
      them via `setLong(...)`. PK is `long` end-to-end — engine internals
      (fastutil `Long2IntOpenHashMap`), Kafka key (`LongSerializer`), and
      `SyncEvent.pk: long` payload all carry the raw long. See cdc-engine R1 + R12.
        - `String tableName()` — source SQL table (e.g. `"clan_data"`).
        - `String pkColumn()` — primary-key column on `tableName`.
        - `List<String> hashedColumns()` — columns feeding `CRC32(CONCAT_WS(...))`
          in Phase 1.
        - `P mapRow(ResultSet rs)` — Phase 2 single-row mapper to an opaque
          per-source row record. The engine treats `P` as `Object`; the
          `EntityMapping` impl casts back to its private record type inside
          `mapEntity`.
    - `List<ChildSource<?>> children()` — additional source tables that
      contribute to entity assembly via FK back to primary's PK column. May be
      empty (single-table entity). For each child:
        - `String tableName()` — child SQL table (e.g. `"clan_skills"`).
        - `String fkColumn()` — column referencing primary's PK.
        - `List<String> hashedColumns()` — columns feeding the child's
          `BIT_XOR(CRC32(CONCAT_WS(...)))` aggregate in Phase 1 (see
          [`cdc-engine` R20](../005-cdc-engine/spec.md)).
        - `C mapRow(ResultSet rs)` — Phase 2 single-row mapper to an opaque
          per-source row record.
    - `T mapEntity(Object primaryRow, Map<String, List<Object>> childRowsByTable)`
      — assembles the entity DTO. `primaryRow` is the value produced by
      `primary().mapRow(rs)`; `childRowsByTable` is keyed by
      `child.tableName()` and carries the (possibly empty) list of child rows
      produced by each `ChildSource.mapRow(rs)`. Implementation casts back to
      its private row types and returns the typed DTO `T`. Called once per
      created/updated PK; never called for deletions (tombstones have
      `payload=null`).

  **Notes:**
    - `EntityMapping` describes ONLY the schema shape (what to sync). All
      operational parameters (cadence, window size, timeouts) come from
      `l2nx.properties` per [`cdc-engine` R15](../005-cdc-engine/spec.md). The SPI does
      NOT carry `tickInterval()` / `strategy()` / `windowCount()` /
      `queryTimeout()` / `maxRows()` — these would mix operator concerns into the
      schema-provider contract.
    - Kafka topic names are NOT declared by the schema provider either; they
      arrive from the platform via `ConnectResponse.syncTopics` keyed by
      `entityName()`. See [`adapter-bootstrap` R16](../001-adapter-bootstrap.md)
      and Decisions in the Technical design section below.
    - Engine-side aggregation (`BIT_XOR` of child CRCs XOR-folded into the
      primary CRC per PK), Phase-2 fetch strategy (separate IN-queries per
      source), orphan-FK handling (silently dropped), and the no-JOIN rule
      live in [`cdc-engine` R1 + R20](../005-cdc-engine/spec.md). Provider authors
      only declare per-source schema shape; engine handles the rest.

> **R6, R7, R8 — moved to [`cdc-engine`](../005-cdc-engine/spec.md).** The CRC32 two-phase
> protocol (engine R1), scheduler semantics (cdc-engine R5), and initial-sync flow
> (cdc-engine R7) are owned by the engine slice. db-sync wires the engine with the
> resolved `DbSchemaProvider` at Phase 2 start. Numbers R6 / R7 / R8 are intentionally
> left as gaps here — not reused per the SpecKit "deleted numbers stay deleted" rule.

> **Engine configuration knobs (relayed from
> [`cdc-engine` R15](../005-cdc-engine/spec.md)).** All keys live under
> `l2nx.cdc-engine.*` in the operator's `l2nx.properties`; resolved one-shot at
> module start. Operators of db-sync deployments tune these without touching
> the schema provider:
>
> | Key                                              | Type           | Default                            |
> |--------------------------------------------------|----------------|------------------------------------|
> | `l2nx.cdc-engine.tick-interval-seconds`          | long, seconds  | 60                                 |
> | `l2nx.cdc-engine.rows-per-window`                | int            | 500_000 (cap 10_000_000)           |
> | `l2nx.cdc-engine.query-timeout-seconds`          | int, seconds   | 10                                 |
> | `l2nx.cdc-engine.publish-flush-seconds`          | int, seconds   | 5                                  |
> | `l2nx.cdc-engine.workers`                        | int            | `max(2, min(entities, cores/2))`   |
> | `l2nx.cdc-engine.fetch-size`                     | int            | 10_000                             |
>
> `workers` sizes the shared CDC scheduler pool (daemon threads
> `nx-cdc-pool-<schema>-N`) — replaces the legacy thread-per-entity model.
> `fetch-size` is a fetch-size hint for Postgres / other drivers. JDBC dialect
> is auto-detected from `Connection.getMetaData().getURL()` at engine start:
> MySQL/MariaDB hosts switch to `Integer.MIN_VALUE` row-by-row streaming (the
> only mode Connector/J honors for large result sets — positive `fetch-size`
> is silently ignored there); Postgres uses `fetch-size` as a server-side
> cursor batch.

> **`DbSchemaProvider` identifier contract.** Every SQL identifier returned
> from `PrimarySource` / `ChildSource` — `tableName`, `pkColumn`, `fkColumn`,
> every entry in `hashedColumns` — MUST match the regex
> `^[A-Za-z_][A-Za-z0-9_]{0,63}$`. Bare identifiers only — schema-qualified
> names (`db.tbl`), back-tick-quoted names, hyphens, spaces, dots are
> REJECTED at engine start (the engine transitions to `STATE_FAILED`).
> See [`cdc-engine` R19](../005-cdc-engine/spec.md) for the rationale (engine
> interpolates these into SQL; parameter binding only covers literals).

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

- [wip] R10. **[Phase 2]** Bohpts client + clan entity MVP — `bohpts-core` repo
  (private; `E:/projects/bohpts/bohpts-core`) MUST host a `BohptsDbSchemaProvider`
  class implementing `DbSchemaProvider` directly (no `extends` — there is no
  vanilla `nx-gs-db-l2j` to inherit from in MVP), plus a
  `META-INF/services/app.l2nx.gs.adapter.api.spi.DbSchemaProvider` resource
  pointing to it. Bohpts-core declares `implementation
  'app.l2nx:nx-gs-adapter-api:0.7.0'` from Maven Central. Provider contract:
    - `schemaName()` = `"bohpts"`
    - `mappings()` returns exactly one `EntityMapping<ClanDbDto>` for the `clan`
      entity:
        - `entityName()` = `"clan"`
        - `dtoType()` = `ClanDbDto.class`
        - `primary()` = `ClanPrimarySource`:
            - `tableName()` = `"clan_data"`
            - `pkColumn()` = `"clan_id"`
            - `hashedColumns()` = `["clan_name", "clan_level", "leader_id",
              "ally_id"]` (crest_id, ally_name, ally_crest_id, the four
              `*_penalty_*` / `*_expiry_time` fields and the `membersCount`
              formula are intentionally out-of-scope per Non-goals)
            - `mapRow(rs)` → `ClanRow` (package-private record/class with
              `clanId`, `clanName`, `clanLevel`, nullable `leaderId`,
              nullable `allyId`)
        - `children()` = `[ClanSkillsChildSource]`:
            - `tableName()` = `"clan_skills"`
            - `fkColumn()` = `"clan_id"`
            - `hashedColumns()` = `["skill_id", "skill_level"]` (the
              `sub_pledge_id` and `skill_name` columns are intentionally
              out-of-scope; only id + level are surfaced on the wire)
            - `mapRow(rs)` → `ClanSkillRow` (package-private record/class with
              `int skillId`, `int skillLevel`)
        - `mapEntity(primaryRow, childRowsByTable)` casts `primaryRow` to
          `ClanRow`, reads `childRowsByTable.get("clan_skills")` (defaulting
          to empty list), casts each row to `ClanSkillRow`, builds a
          `List<ClanSkillDbDto>`, and assembles `ClanDbDto.builder()
          .clanId(...).clanName(...).clanLevel(...).leaderId(...).allyId(...)
          .skills(skills).build()`.
    - `ClanDbDto` ships in `nx-gs-adapter-api` (Java 8 POJO, hand-written builder) so
      the platform-side consumer compiles against the same wire type. Field types
      mirror DB nullability — primitives for `NOT NULL` columns, boxed for
      nullable. ID fields are `long`/`Long` end-to-end:
        - `long clanId` (PK, `NOT NULL`)
        - `String clanName` (`NOT NULL`)
        - `int clanLevel` (`NOT NULL`, source default `0`)
        - `Long leaderId` (null when source `leader_id = 0` per L2J convention)
        - `Long allyId` (null when source `ally_id = 0`)
        - `List<ClanSkillDbDto> skills` — `null` when the tenant does not
          declare a `ChildSource` for skills at all (no `clan_skills`
          equivalent in the source schema, or skills intentionally not
          synced); empty list when the tenant syncs skills but the clan
          has none. Gson's default `serializeNulls=false` omits the field
          from JSON when `null`, so the wire shape distinguishes
          "feature not synced" from "feature synced, value empty".
    - `ClanSkillDbDto` (new, ships in `nx-gs-adapter-api`) is a Java 8 POJO with
      `int skillId`, `int skillLevel`, hand-written builder, equals/hashCode/
      toString.
    - The package for `BohptsDbSchemaProvider` inside bohpts-core is
      `l2e.gameserver.l2nx`. No bohpts-internal class names or column
      conventions leak into this monorepo (bohpts-core is private; this
      monorepo stays open-core).
    - The Kafka topic the engine publishes `clan` events to is delivered by the
      platform via `ConnectResponse.syncTopics["clan"]` (e.g.
      `"bohpts.gs.sync.clans"`) — NOT declared in the mapping.

- [wip] R11. **Module versions (current state):**
    - `nx-gs-adapter-api` = `0.7.0` (breaking SPI change for multi-source
      `EntityMapping`: split into `PrimarySource<P>` + `List<ChildSource<C>>` +
      `mapEntity(...)`; removes top-level `tableName` / `pkColumn` /
      `hashedColumns` / `mapRow`. Adds `ClanSkillDbDto`; extends `ClanDbDto` with
      `List<ClanSkillDbDto> skills`).
    - `nx-gs-db-sync-core` = `0.2.0` (multi-source CDC engine: per-source
      Phase 1 + `BIT_XOR` aggregate, per-source Phase 2 + `mapEntity`
      assembly, envelope-based windowing for DELETE-at-boundary correctness).
    - `nx-gs-adapter-core` stays at `0.3.2` (no wire change in `/connect` /
      `ConnectContext` for this slice).
    - `nx-gs-kafka` stays at `0.2.0` (no wire change).
    - **No** `nx-gs-db-bohpts` artifact is published — the bohpts schema
      provider is shipped as part of the bohpts-core game-server JAR itself.

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
  engine on every cycle — see [`cdc-engine` R10](../005-cdc-engine/spec.md). Replaces
  the earlier placeholder `tables: ["clan_data"]` (names only, table-centric)
  shape.

> **R13 (RAM cap) — REMOVED.** Earlier draft had db-sync (and cdc-engine R8) enforce
> a per-entity row-count cap. The CDC engine no longer enforces a cap — operators
> size the host JVM heap to fit the configured entities. See [`cdc-engine` R8 strip
> note](../005-cdc-engine/spec.md).

**Could:**

> **R14 — moved to [`cdc-engine` R15 (Must)](../005-cdc-engine/spec.md).** Operator-side
> cadence tuning lives on the engine: `l2nx.cdc-engine.tick-interval-seconds` /
> `l2nx.cdc-engine.window-count` global overrides + R16 startup log of resolved
> values. Per-table granularity is intentionally NOT in MVP — comes via cdc-engine
> R14 (Could, dynamic Kafka config) when a real ops case demands it.

> **R15 (formerly `SLIDING_WINDOW` Could) — REMOVED.** The CDC engine ships a
> single windowed scan algorithm (`l2nx.cdc-engine.rows-per-window`, default
> 500_000); small entities collapse to 1 window naturally. No `FULL_SCAN` /
> `SLIDING_WINDOW` enum, no `mapping.strategy()`. See
> [`cdc-engine` R2](../005-cdc-engine/spec.md).

- [done] R16. **[Phase 2]** `DbSyncModule.onConnect` MUST consult
  `ctx.syncTopics()` (per [`adapter-modules` R2](../002-adapter-modules/spec.md) and
  [`adapter-bootstrap` R16](../001-adapter-bootstrap.md)) before instantiating the
  CDC engine. Behavior:
    - `null` or empty `syncTopics` map → log actionable WARN (`"no entity topics in
      ConnectResponse — db-sync has nothing to sync"`), transition the module to
      `DISABLED` state, no engine instantiated, no scheduler threads started, the
      pool is released. Defensive path; not expected in steady-state operation
      (the platform always delivers a non-empty map for tenants with sync enabled).
    - Non-empty map → pass it into the engine via `TopicResolver` (see
      [`cdc-engine` R17](../005-cdc-engine/spec.md)). The engine handles per-entity
      missing-topic situations on its own (entity → `DEGRADED`); the module-level
      `DISABLED` triage is reserved for the all-empty case only.

**Non-goals:**

- **Schema discovery / introspection** — the adapter does not query the host DB's
  `information_schema`. `EntityMapping` is the source of truth.
- **Composite PKs** — single-column PK assumption. Entities with multi-column PKs
  are out of scope for MVP; can be added as `CompositeEntityMapping` later.
- **DML** on the host DB — read-only. Read-only is enforced at the SQL level via
  `START TRANSACTION ... READ ONLY` (every Phase 1 / Phase 2 query), MySQL user is
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
  [`adapter-bootstrap` R16](../001-adapter-bootstrap.md)). Adapter does not
  construct topic names. Earlier "per-table vs single-topic" debate moot — the
  platform delivers explicit per-entity topics and is free to choose its own naming
  policy.]
- [resolved: Strategy + cadence — engine config from `l2nx.properties` only per
  [`cdc-engine` R15](../005-cdc-engine/spec.md); no provider-side declarations on
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
  Platform-side consumer compiles against `SyncEvent<ClanDbDto>` and gets compile-time
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
- [resolved: `op=DELETED` carries `payload=null` in the JSON envelope but is NOT a
  Kafka tombstone — db-sync topics use bounded retention, not log compaction, so the
  value-null tombstone optimisation doesn't apply. Consumers MUST handle the
  `DELETED` op explicitly; the Kafka value is a non-null JSON envelope carrying
  entityName + pk + op + null payload + timestamp. Reflected in the `SyncEvent`
  Javadoc.]

## Links

- Sibling feature: [`docs/specs/001-adapter-bootstrap.md`](../001-adapter-bootstrap.md)
  — Tier-1 SPI extension lands there
- Sibling feature (Tier-3 SPI):
  [`docs/specs/004-jdbc-connection-source.md`](../004-jdbc-connection-source.md)
  — `JdbcConnectionSource` design, pool-agnostic borrowing, bohpts reference impl
  wrapping `l2e.gameserver.database.DatabaseFactory`
- Sibling feature (CDC algorithm):
  [`docs/specs/005-cdc-engine/spec.md`](../005-cdc-engine/spec.md) — CRC32 two-phase
  protocol, single windowed strategy with `rows-per-window` partition, scheduler,
  query timeout, per-entity heartbeat stats
- Module discovery diagrams: [`module-discovery.md`](./module-discovery.md)
- CRC32 CDC resource estimates: image attached to /specl-take invocation (bohpts
  x20 benchmark — Characters 152k / Clans 1k / Items 12M — informs window-count
  math in the cdc-engine spec)

---

## Technical design

> Covers: spec.md
> Sibling: [module-discovery.md](./module-discovery.md) (visual walkthroughs of the SPI flow)

### Overview

Two-tier SPI architecture sitting on top of `adapter-bootstrap`. `nx-gs-db-sync-core`
registers itself as an `AdapterModule` (Tier 1, discovered by `nx-gs-adapter-core`) and uses
ServiceLoader internally to discover one `DbSchemaProvider` (Tier 2) on the host classpath.
The provider returns a list of `EntityMapping`s — table name, PK column, hashed columns,
row-DTO mapper, strategy, cadence. The CDC algorithm itself (CRC32 two-phase protocol,
scheduler, in-memory snapshot, per-table stats) lives in the
[`cdc-engine`](../005-cdc-engine/spec.md) feature; `DbSyncModule` instantiates the engine after
`DbSchemaProvider` is resolved and feeds it the provider's `EntityMapping`s.
MVP target: bohpts client implements `DbSchemaProvider` directly inside its own `bohpts-core`
repo (no separate published artifact, no template-method indirection — vanilla L2J
extraction deferred to second-customer time per spec Non-goals). One `EntityMapping` for
`clan_data` (4 hashed columns: clan_name, clan_level, leader_id, ally_id) validates the
design end-to-end.

### Structure

- `nx-gs-db-sync-core/src/main/java/app/l2nx/gs/db/sync/`
    - `DbSyncModule.java` — implements `app.l2nx.gs.adapter.api.spi.AdapterModule`;
      Phase 1 owns SPI smoke check + heartbeat enrichment. Phase 2 wires `CdcEngine`
      (defined in [`cdc-engine`](../005-cdc-engine/spec.md)) with the resolved
      `DbSchemaProvider`'s mappings.
    - `engine/` — CDC algorithm; structure documented in
      [`cdc-engine spec`](../005-cdc-engine/spec.md). Engine code physically ships in
      `nx-gs-db-sync-core` JAR; the `cdc-engine` feature is a design-doc slice, not a
      separate Gradle module. `SyncEventPublisher` lives under this `engine/` package
      (not a separate `kafka/`).
    - `META-INF/services/app.l2nx.gs.adapter.api.spi.AdapterModule` — service descriptor
      with `app.l2nx.gs.db.sync.DbSyncModule`
- `bohpts-core/` [planned, **lives in the private bohpts-core repo, NOT this monorepo**]
    - depends on `app.l2nx:nx-gs-db-sync-core:0.2.0` (Maven Central)
    - `<bohpts-package>/BohptsDbSchemaProvider.java` [planned] — implements
      `DbSchemaProvider` directly (no `extends` — vanilla L2J doesn't exist yet);
      `schemaName="bohpts"`. Package up to bohpts-core owner — see spec Open question.
    - `<bohpts-package>/mapping/ClanMapping.java` [planned] — only `EntityMapping`
      in MVP; `entityName="clan"`, `tableName="clan_data"`. Applies the
      zero-as-null convention to `leader_id` / `ally_id` (`0L` → `null`) in `mapRow`
    - `src/main/resources/META-INF/services/app.l2nx.gs.adapter.api.spi.DbSchemaProvider`
      [planned] — service descriptor pointing to `BohptsDbSchemaProvider`
- `nx-gs-adapter-api/src/main/java/app/l2nx/gs/adapter/api/`
    - `spi/AdapterModule.java` — Tier-1 SPI (declared by `adapter-modules`; listed
      here for completeness because db-sync implements it)
    - `spi/ConnectContext.java` — context object passed to
      `AdapterModule.onConnect`; carries identity bundle + per-entity Kafka topic
      map (`syncTopics`)
    - `spi/DbSchemaProvider.java` — Tier-2 SPI interface (api/0.7.0)
    - `spi/EntityMapping.java` — Tier-2 SPI; one per synced entity (api/0.7.0)
    - `kafka/sync/db/SyncEvent.java` — typed wire envelope `SyncEvent<T>` with
      `String entityName`, `long pk`, `String op`, `T payload`, `long timestampEpochMs`
      (api/0.7.0). `op=DELETED` carries `payload=null` in JSON but is NOT a Kafka
      tombstone — db-sync topics use bounded retention, not log compaction;
      consumers MUST handle the DELETED op explicitly (Javadoc on `SyncEvent`
      reflects this).
    - `kafka/sync/db/ClanDbDto.java` — clan row DTO (Java 8 POJO, hand-written builder);
      `long clanId` / `int clanLevel` (NOT NULL columns); `Long leaderId` / `Long allyId`
      (nullable per L2J `0`-sentinel convention). Co-located with `SyncEvent<T>` under
      `kafka.sync.db` — DB-sync wire types share one sub-package.

### Key components

- **`DbSyncModule`** (R1, R2, R3, R9, R12, R16) — Tier-1 SPI entry point. Lifecycle:
    - `onConnect(ctx)` — gates on `ctx.syncTopics()` first (null/empty →
      `DISABLED` + WARN, no SPI lookups). Then resolves Tier-3
      `JdbcConnectionSource` (0 / >1 → `FAILED`) and runs the smoke check
      (`isValid(5)` only — adapter no longer calls `setReadOnly(true)` on
      borrowed connections; see `jdbc-connection-source` R3); pass → on
      track for `ACTIVE`, fail → on track for `DEGRADED`. Then resolves
      Tier-2 `DbSchemaProvider`
      (0 → `DISABLED`, >1 → `FAILED`, 1 → cache). On the happy path: state
      becomes `ACTIVE` if smoke passed, else `DEGRADED` (engine still runs
      so the next cycle retries borrow).
    - `start()` — short-circuits on `DISABLED`/`FAILED`/`INIT`. Otherwise
      validates cached `(provider, source, ctx, tracker)`, then if
      `provider.mappings()` empty → `DISABLED`, else builds `EngineConfig`
      from `EngineConfig.productionChain()`, `TopicResolver.fromContext(ctx)`,
      `SyncEventPublisher(kafkaSender)`, and instantiates `CdcEngine` with the
      whole bundle. `engine.start()` schedules every entity as a task on the
      shared daemon pool `nx-cdc-pool-<schema>-N` (sized by
      `l2nx.cdc-engine.workers`, default `max(2, min(entities, cores/2))`)
      with first-tick delay 0 (initial sync). On `Throwable` from
      `engine.start` → `FAILED`.
    - `stop()` — `engine.stop()`; cancels schedulers, awaits brief in-flight
      drain (2 s), clears all snapshots. Idempotent.
    - `onDisconnect()` — clears every cached ref (`source`, `provider`, `ctx`,
      `statsTracker`, `engine`). Source itself is host-owned; module does not
      close it.
    - `currentStatus()` — surfaces both
      `JdbcConnectionSource.stats() → Stats.pool` and
      `EntityStatsTracker.currentStatuses() → Stats.entities`. Returns
      `Stats.empty()` (singleton EMPTY) when both slots are absent so the
      INIT-state heartbeat carries the canonical empty stats. Tolerates a
      throwing `stats()` / `currentStatuses()` impl by logging and falling back.
- **`DbSchemaProvider`** (R3, R4) — Tier-2 SPI. Single source of truth for "what tables
  look like in this schema". Vanilla impls expose `protected` template-method hooks for column
  / table names so client overrides change one thing without re-implementing the whole provider.
- **`EntityMapping<T>`** (R5) — declares one **primary source** + zero-or-more
  **child sources** for a single platform entity:
    - `entityName()` — domain identifier (`"clan"`, `"character"`, `"item"`).
    - `dtoType()` — `Class<T>`, the wire DTO.
    - `primary()` — `PrimarySource<?>` driving windowing + identity:
      `tableName`, `pkColumn`, `hashedColumns`, `mapRow(rs)` → opaque per-row
      record.
    - `children()` — `List<ChildSource<?>>` (may be empty); each child carries
      `tableName`, `fkColumn` referencing primary's PK, `hashedColumns`,
      `mapRow(rs)` → opaque per-row record.
    - `mapEntity(primaryRow, childRowsByTable)` — assembles the typed DTO `T`.
      Engine groups child rows by FK and passes a
      `Map<String, List<Object>>` keyed by `child.tableName()`. Implementations
      cast back to their private row types and build the DTO. Nullable
      collection fields on the DTO that the tenant doesn't sync (no
      corresponding `ChildSource` declared) stay `null` → Gson omits them on
      the wire.

  Engine consumes the SPI uniformly: per-cycle, per-window it runs one Phase 1
  hash query per source (primary CRC32, each child `BIT_XOR(CRC32(...))
  GROUP BY fk`), XOR-folds child contributions into the per-PK aggregate, and
  in Phase 2 runs one `IN`-fetch per source. See
  [`cdc-engine/spec.md`](../005-cdc-engine/spec.md) R1 + R20.
- **CDC engine** — `CdcEngine`, `EntitySyncTask`, `Phase1Hasher`,
  `Phase2Fetcher`, `SnapshotStore`, `WindowPlanner`, `SyncEventPublisher`,
  `TopicResolver`, `EntityStatsTracker`, `EngineConfig`,
  `ConfigResolutionLogger` are designed in
  [`cdc-engine spec`](../005-cdc-engine/spec.md). The module assembles the
  graph in `start()` and reads from it in `currentStatus()`. Engine code
  physically lives in the `nx-gs-db-sync-core` JAR — db-sync's design
  surface stops at "wire the engine with the resolved provider's mappings".
- **`BohptsDbSchemaProvider`** [planned] (R10) — implements `DbSchemaProvider` directly
  (no template-method base class in MVP — vanilla L2J extraction deferred to
  second-customer time per spec Non-goals). Lives in bohpts-core repo. Returns one
  `ClanMapping` from `mappings()`. `schemaName="bohpts"`.
- **`ClanMapping`** [planned] (R10) — only `EntityMapping` in MVP. Lives in
  bohpts-core. `entityName = "clan"`, `tableName = "clan_data"`. 4 hashed cols
  (`clan_name`, `clan_level`, `leader_id`, `ally_id`). `mapRow` keeps BIGINT
  `leader_id` / `ally_id` as `Long`, applying the zero-as-null convention so the
  wire payload's `leaderId` / `allyId` is `null` for "no value" rather than `0L`.

### Data flows

#### 1. Module discovery (Tier 1) at adapter startup

Per `adapter-bootstrap` extension. After successful `/connect`:

```
NxAdapter.start()
  → POST /connect → 200 → ConnectResponse parsed
  → Kafka producer initialized (adapter-bootstrap R6)
  → ServiceLoader.load(AdapterModule.class)               [extension]
  → for each module:
        module.onConnect(ctx)
        module.start()
  → HeartbeatService.start(...) — enabledModules carries collected names
```

#### 2. Module discovery (Tier 2) inside db-sync-core

```
DbSyncModule.onConnect(ctx)                           [Phase 2]
  → ServiceLoader.load(DbSchemaProvider.class)
  → providers.size():
       0 → log WARN "no DbSchemaProvider on classpath"; state = DISABLED; return
       >1 → log ERROR listing fqcns; state = FAILED; return
       1 → cache provider
  → if (ctx.syncTopics() == null || ctx.syncTopics().isEmpty()) {
        log WARN "no entity topics in ConnectResponse — db-sync has nothing to sync"
        state = DISABLED; return    -- R16: defensive, not expected in steady state
    }
  → cache topicResolver = TopicResolver.from(ctx.syncTopics())

DbSyncModule.start()                                  [Phase 2]
  → engine = new CdcEngine(provider.mappings(), jdbcConnectionSource, kafkaProducer,
                            topicResolver, configResolver)
  → engine.start()                                    -- shared scheduler pool
                                                         nx-cdc-pool-<schema>-N
                                                         (cdc-engine R5)
```

#### 3. CDC tick + initial sync

Documented in [`cdc-engine spec`](../005-cdc-engine/spec.md) data flows. Initial sync is
the first tick after `engine.start()` with empty `SnapshotStore` — same code path,
diff yields every PK as `created` (cdc-engine R7).

#### 4. Module shutdown

```
NxAdapter.shutdown()
  → for each AdapterModule (reverse discovery order):
        module.stop()              -- cancel schedulers, drain Kafka sends
  → for each AdapterModule:
        module.onDisconnect()      -- clear snapshots (Tier-3 source owned by host)
  → close NxKafka producer
  → state = CLOSED
```

### Data model

- **Adapter-side persistence: per-entity snapshot files on local disk** —
  `SnapshotStore` is dumped to
  `<persist-dir>/<schema>/<entityName>.snap` after every successful
  cycle (throttled, default 300s) and force-flushed on engine stop.
  Reloaded on start BEFORE the first tick — so DELETE events fire on the
  next cycle for rows removed from the host DB while the adapter was
  offline. See [`snapshot-persistence`](../012-snapshot-persistence.md)
  for the boundary contract + file format.
- **Host DB tables (read-only)** — bohpts schema confirmed against
  `bohpts-core/com.bohpts.game.clan.Clan`:
    - `clan_data` [bohpts] — `clan_id` (PK, BIGINT), `clan_name` (VARCHAR), `clan_level`
      (INT), `leader_id` (BIGINT FK → `characters.charId`), `crest_id` (BIGINT), `ally_id`
      (BIGINT), `ally_name` (VARCHAR), `ally_crest_id` (BIGINT), `ally_penalty_type` (INT,
      custom converter), `ally_penalty_expiry_time` / `char_penalty_expiry_time` /
      `dissolving_expiry_time` (BIGINT epoch). MVP syncs only the 4 plain cols (clan_name,
      clan_level, leader_id, ally_id) — see spec Non-goals.
    - Hibernate `@Formula` `membersCount` — computed via subquery, not a real column. NOT
      synced in MVP. Member counts are derivable from a future `characters` table sync.
- **Wire types** (in `nx-gs-adapter-api`) — IDs are `long`/`Long` end-to-end
  (engine `long`, JSON number, Kafka key via `LongSerializer`). DTO field types
  mirror DB nullability: primitives for `NOT NULL` columns, boxed for nullable —
  Gson serializes both identically, but the type carries the nullability contract:
    - `SyncEvent` — final wire shape:
      candidate fields: `entityName`, `op (CREATED|UPDATED|DELETED)`, `pk: long`,
      `payload`, `timestamp`
    - `ClanDbDto`:
        - `long clanId` (PK, `NOT NULL`)
        - `String clanName` (`NOT NULL`)
        - `int clanLevel` (`NOT NULL`, source default `0`)
        - `Long leaderId` (null when source `leader_id = 0` per L2J convention)
        - `Long allyId` (null when source `ally_id = 0`)

### Integration points

- **`:nx-gs-adapter-api`** (R10, R11) — adds `AdapterModule`,
  `ConnectContext`, `SyncEvent`, `ClanDbDto`. Bumped to next minor release. Lands in
  two slices: `AdapterModule` + `ConnectContext` arrive with the `adapter-bootstrap`
  / `adapter-modules` extension; `SyncEvent` (with `entityName`, `pk: long`) +
  `ClanDbDto` (Long ID fields) arrive with this feature in api/0.7.0 alongside
  `EntityStats` / `EntityState` / `ChangesSummary` and
  `ConnectResponse.syncTopics`.
- **`:nx-gs-adapter-core`** (R1) — extends `NxAdapter.start()` with
  `ServiceLoader.load(AdapterModule.class)` invocation; populates
  `HeartbeatEvent.enabledModules`. Lands as part of the `adapter-bootstrap` extension.
- **`:nx-gs-kafka`** — sync events published via `NxKafka.instance().send(topic, key,
  syncEvent)` from `cdc-engine`'s `SyncEventPublisher`. No change to `nx-gs-kafka` API.
- **`:nx-gs-db-sync-core`** (R1–R9) — new module in this monorepo, published to
  Maven Central as `app.l2nx:nx-gs-db-sync-core`. Phase 1 released as `0.1.0`,
  Phase 2 single-table CDC as `0.1.1`, multi-source CDC engine as `0.2.0`
  with `DbSyncModule` only (no engine, no `DbSchemaProvider` SPI yet).
- **`bohpts-core`** [planned] (R10) — bohpts-core repo (private) declares
  `implementation 'app.l2nx:nx-gs-adapter-api:0.7.0'` (Tier-2 SPI lives in api;
  bohpts-core does NOT need a runtime dep on `nx-gs-db-sync-core`), hosts
  `BohptsDbSchemaProvider` + `ClanMapping` classes inline in its source tree, and
  ships `META-INF/services/app.l2nx.gs.adapter.api.spi.DbSchemaProvider` in its
  resources. NO separate `nx-gs-db-bohpts` artifact is published.
- **`jdbc-connection-source` feature** (R2) — Tier-3 SPI feature delivering
  `JdbcConnectionSource` + the bundled-Hikari fallback. `nx-gs-db-sync-core` consumes
  the resolved instance via `JdbcConnectionSourceResolver`. Pool implementation choice
  (host Path 1 — bohpts `DatabaseFactory`, vanilla L2J pool, etc. / Path 2 — bundled
  shadowed Hikari 3.4.5 from `l2nx.db.*` config) lives in that feature.
- **Shadowed Hikari 3.4.5** [planned] (R2 fallback path) — bundled in
  `nx-gs-db-sync-core`, relocated to `app.l2nx.shaded.hikari.*` so it cannot collide
  with whatever pool the host JVM already ships. Adds ~150 KB to the
  `nx-gs-db-sync-core.jar`.
- **`fastutil-core`** (cdc-engine R4) — declared as `implementation` dep on
  `nx-gs-db-sync-core` (~3 MB). Used by the engine's `Long2IntOpenHashMap` snapshots.
- **`cdc-engine` feature** — engine code physically lives in `nx-gs-db-sync-core` JAR;
  design surface (algorithm, scheduler, RAM cap, query timeout, per-table heartbeat
  stats) lives in [`cdc-engine/spec.md`](../005-cdc-engine/spec.md) +
  [`cdc-engine spec`](../005-cdc-engine/spec.md). `DbSyncModule` instantiates `CdcEngine`
  in Phase 2 `start()`.
- **`nx-tenants` `nexus.adapter.sync-config` Kafka topic** — out of scope for MVP.
  Future feature for platform-driven cadence / strategy overrides; tracked under
  cdc-engine R14.

### Decisions

- **Module rename `nx-gs-adapter-db-*` → `nx-gs-db-*`.** "adapter" prefix is reserved for the
  bootstrap layer (`nx-gs-adapter-api` defining wire contracts, `nx-gs-adapter-core` running
  the connect / heartbeat / lifecycle). DB-sync sits *above* adapter-core (a consumer of its
  Tier-1 SPI), not part of bootstrap. Same convention applies to siblings: `nx-gs-db-l2j`
  (future), `nx-gs-db-lucera` (future), `nx-gs-dp-l2j` (future), `nx-gs-dp-lucera` (future).
  README to be updated.
- **Two-tier SPI.** Tier 1 (`AdapterModule` in `nx-gs-adapter-api`, discovered by
  `nx-gs-adapter-core`) is the open-core module SPI — any module type plugs in here. Tier 2
  (`DbSchemaProvider` in `nx-gs-db-sync-core`, discovered by `nx-gs-db-sync-core`) is internal
  to the DB-sync stack — schema variants plug in here. Tier 2 lives in `db-sync-core` (NOT
  `adapter-api`) because `adapter-api` stays focused on platform↔adapter wire contracts;
  internal adapter SPIs do not belong there.
- **Bohpts schema provider lives inline in `bohpts-core`, not as a published artifact.**
  Per-client modules whose code references client-proprietary schema details NEVER ship to
  Maven Central — that's the open-core boundary. Two equivalent ways to implement this:
  (1) a separate published-but-private `nx-gs-db-bohpts` artifact, or (2) the schema-provider
  classes living directly in bohpts-core's source tree alongside the existing JPA entities.
  We pick (2) for MVP: less ceremony, fewer artifacts, schema mapping naturally co-located
  with the schema source. The bohpts-core JAR is already deployed onto the operator's
  classpath; adding `META-INF/services/...DbSchemaProvider` + a class is the smallest
  possible change. Switch to (1) only if a third-party operator needs to consume
  bohpts-equivalent code without owning bohpts-core (no current scenario).
- **Skip vanilla `nx-gs-db-l2j` in MVP.** Until a second non-bohpts customer arrives,
  extracting "common L2J vanilla code" is YAGNI — there is no real evidence yet for what's
  shareable across forks. The bohpts impl directly implements `DbSchemaProvider` (no
  `extends`). When the second customer ships, common code is extracted into `nx-gs-db-l2j`,
  bohpts is refactored to extend it via template method, and the multi-impl
  resolution rules (config selector / shadow exclusion / activator JAR — see spec Open
  questions) are decided.
- **All IDs serialized as `Long` end-to-end.** PK + FK columns in DTOs +
  `SyncEvent.pk` — uniformly `Long`. Engine reads PK via `rs.getLong(pkColumn)`,
  Phase 2 binds via `setLong(...)`, Kafka key uses `LongSerializer` (8 bytes
  big-endian). Platform stores PK as `long` and reads it back as `long`.
  Rationale:
  (1) **Numeric PK is the bohpts (and the dominant L2J family) reality** — BIGINT
  surrogate keys; cross-schema variance (UUID/composite) is an explicit Non-goal in
  MVP and would arrive as a separate wire-shape variant.
  (2) **No client-side stringification** — engine internals already use `long`
  (fastutil `Long2IntOpenHashMap`); stringifying at the wire boundary added
  conversion cost without buying anything for the long-PK happy path.
  (3) **Kafka key is 8 bytes** — `LongSerializer` is binary, smaller than UTF-8
  string PK. Same partition affinity guarantee.
  (4) Earlier "all IDs as String for cross-schema invariance" stance reversed —
  cross-schema is Non-goal in MVP; deferring to a parallel String-PK / composite-PK
  wire variant when a non-numeric customer arrives is cleaner than a premature
  stringification.
  Schema providers still handle the **zero-as-null convention** in `mapRow` (L2J FK
  columns use `0` to mean "no value" — bohpts emits `null` in the DTO instead of
  `0L` for cleaner platform-side semantics).
- **Single-impl assumption for `DbSchemaProvider` discovery.** In MVP, only one provider
  exists on the classpath (the bohpts one, inside bohpts-core). The fail-loud behaviour for
  > 1 providers (R3) surfaces classpath ambiguity to the operator instead of silently picking
  one. The full multi-impl resolution story (config selector / shadow exclusion / activator
  JAR) is open and resolved when vanilla `nx-gs-db-l2j` ships AND a customer ends up with
  both vanilla and bohpts on classpath — see spec Open questions.
- **Per-entity Kafka topics delivered by the platform via `ConnectResponse.syncTopics`.**
  Topic names arrive at handshake time (`adapter-bootstrap` R16), are surfaced through
  `ConnectContext.syncTopics()` (`adapter-modules` R2), and feed into
  `cdc-engine`'s `TopicResolver` (`cdc-engine` R17). Schema providers do NOT declare
  topic names — they only declare `entityName`, which the platform uses as the
  lookup key into `syncTopics`. Empty / missing map at module level → `DISABLED`
  (R16); per-entity missing topic → engine marks the entity `DEGRADED` (R17).
  Rationale: topic naming is a platform concern (per-tenant ACLs, retention, naming
  conventions); decoupling it from the schema provider lets the platform rename
  / reorganize topics without coordinated provider releases. Defensive paths are
  guarded but not expected to fire in steady-state production.
- **Read-only enforced at the SQL level, not via `Connection.setReadOnly`.** Every
  Phase 1 / Phase 2 query runs inside `START TRANSACTION WITH CONSISTENT SNAPSHOT,
  READ ONLY` (see [`cdc-engine` R11](../005-cdc-engine/spec.md)). The engine deliberately
  does NOT call `connection.setReadOnly(true)` on borrowed connections — the host
  pool (Path 1) may not reset that flag on return, and other consumers borrowing
  from the same pool would inherit it. Pool-level read-only is the host's choice
  (see `jdbc-connection-source` for the host-pool contract: pool MUST reset
  read-only on connection return; adapter does not enforce it at the connection
  level). Engine NEVER issues DDL or DML — Phase 1 + Phase 2 are pure `SELECT`
  inside read-only transactions.
- **Engine-algorithm decisions (CRC32 sufficiency, fastutil RAM model, window-scoped
  consistent snapshot, MIN/MAX recompute, shared scheduler pool, cursor-mode JDBC
  fetch, Statement.cancel on shutdown, SQL identifier validation, two-pass
  walk-in-flight, per-row snapshot swap) live in
  [`cdc-engine spec`](../005-cdc-engine/spec.md) Decisions** to keep db-sync's design
  surface focused on module wiring + Tier-2 SPI shape.

### Extension points

- **New table support (vanilla)** — vanilla `L2jSchemaProvider.mappings()` adds new
  `EntityMapping` entries. No engine change.
- **New schema variant (client)** — implement `DbSchemaProvider` directly (MVP path,
  bohpts-style) OR extend an existing vanilla provider via template method
  (post-vanilla path). Either way, ship a `META-INF/services/...DbSchemaProvider`
  descriptor pointing to the client class. Engine treats both identically.
- **New module type** — implement `AdapterModule` in a sibling module (e.g.
  `nx-gs-dp-sync-core` for datapack sync). adapter-core ServiceLoader picks it up
  alongside db-sync. Independent lifecycle, independent failure isolation.
- **Engine algorithm extensions** (new strategy, alternate hash function, composite/
  non-numeric PK, dynamic config consumer) — see
  [`cdc-engine spec`](../005-cdc-engine/spec.md) Extension points.
