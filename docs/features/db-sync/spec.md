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

> **Sibling features carry the SPI plumbing:** Tier-1 SPI (`AdapterModule` + ServiceLoader
> wire-up + `enabledModules` on `HeartbeatEvent`) lives in
> [`adapter-modules`](../adapter-modules/spec.md). Tier-3 SPI (`JdbcConnectionSource`) lives
> in [`jdbc-connection-source`](../jdbc-connection-source/spec.md) (interface in
> `nx-gs-adapter-api` package `app.l2nx.gs.adapter.api.spi`). All requirements below assume
> both are in place.

> **Phase split:**
> - **Phase 1 (smoke test):** R1, R2 (smoke borrow only — not CDC), R9 (module-level
    > exception isolation), R11 (versions), R12 (heartbeat enrichment with `pool` slot only;
    > no `tables` slot yet). Goal: bohpts-core boots, registers `BohptsJdbcConnectionSource`,
    > db-sync surfaces `{name: "db-sync", state: ACTIVE, stats: {pool: ...}}` in heartbeat.
> - **Phase 2 (CDC engine):** R3 — R8, R10, R13. Adds `DbSchemaProvider` Tier-2 SPI, CRC32
    > two-phase protocol, scheduler, initial sync, bohpts `clan_data` mapping, RAM cap, and
    > the `tables` slot in `ModuleStatus.Stats`.
> - **Could (post-MVP):** R14, R15.

**Must:**

- [todo] R1. **[Phase 1]** `nx-gs-db-sync-core` MUST implement
  `app.l2nx.gs.adapter.api.spi.AdapterModule` and ship a
  `META-INF/services/app.l2nx.gs.adapter.api.spi.AdapterModule` descriptor pointing to its
  module class — so adding the JAR to the host classpath alongside `nx-gs-adapter-core` is
  the only step required for the engine to start.
    - SC1. Discovery is zero-config — operator does NOT set any `l2nx.modules=...` system
      property to enable db-sync; presence on the classpath is sufficient.
    - SC2. `AdapterModule.name()` returns the literal string `"db-sync"` — surfaced in
      `HeartbeatEvent.enabledModules`.

- [todo] R2. **[Phase 1: smoke borrow only / Phase 2: CDC borrows]** `nx-gs-db-sync-core`
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

- [todo] R3. **[Phase 2]** `nx-gs-db-sync-core` MUST expose a Tier-2 SPI `DbSchemaProvider`
  (in package `app.l2nx.gs.db.sync.spi`) and discover impls via
  `ServiceLoader.load(DbSchemaProvider.class)` once at module `start()`. Selection rule:
    - **0 impls** → log actionable WARN, db-sync transitions to `DISABLED` (other modules and
      the host JVM keep running)
    - **1 impl** → engine uses it (the dominant case)
    - **>1 impls** → log actionable ERROR listing the conflicting impl class names; db-sync
      transitions to `FAILED`. **Caveat:** when a client-override JAR (e.g. `nx-gs-db-l2j-bohpts`)
      brings the vanilla `nx-gs-db-l2j` JAR transitively, both ship a service descriptor — see
      Open question on resolution strategy. MVP assumes operator's classpath has exactly one
      activated descriptor.

- [todo] R4. **[Phase 2]** `DbSchemaProvider` interface MUST expose:
    - `String schemaName()` — e.g. `"l2j"`, `"l2j-bohpts"`, `"lucera"` (informational; not a
      selection key in MVP)
    - `List<TableMapping<?>> mappings()` — the tables this provider knows about

- [todo] R5. **[Phase 2]** `TableMapping<T>` interface MUST describe ONE table:
    - `String tableName()` — fully-qualified table name in the host DB
    - `String pkColumn()` — primary-key column (single-column PK assumption — see Non-goals).
      The engine ALWAYS reads PK values as `String` (`rs.getString(pkColumn())`) regardless
      of source-column SQL type, so all wire payloads carry IDs as strings. See R6 + tech.md
      Decisions for the cross-cutting stringification rule.
    - `List<String> hashedColumns()` — columns whose values feed CRC32 in Phase 1
    - `T mapRow(ResultSet rs)` — Phase 2 row → DTO conversion (called once per changed row).
      Schema providers convert source-column values to wire types in `mapRow`, including
      Long-to-String for non-PK ID columns (FK references, etc.).
    - `String topicSuffix()` — Kafka topic suffix (e.g. `"clans"`)
    - `Class<T> dtoType()` — DTO class for serialization
    - `SyncStrategy strategy()` — `FULL_SCAN` (small tables) | `SLIDING_WINDOW` (large tables;
      MVP ships only `FULL_SCAN`)
    - `Duration tickInterval()` — per-table sync cadence

- [todo] R6. **[Phase 2]** The CDC engine MUST execute the CRC32 two-phase protocol against each
  `TableMapping` on every scheduled tick:
    - **Phase 1:** `SELECT <pk>, CRC32(CONCAT_WS(',', col1, col2, ...)) FROM <table>` — MySQL
      computes hashes server-side; adapter reads PK via `rs.getString(1)` (stringification
      decision — see tech.md), holds `Map<String, Long>` in memory
    - **Diff:** previous-snapshot vs current-snapshot →
      `{ created: Set<String>, updated: Set<String>, deleted: Set<String> }`
    - **Phase 2:** `SELECT * FROM <table> WHERE <pk> IN (?, ?, ...)` for `created ∪ updated`
      (chunked at 1000 PKs per query). Engine binds PKs via `setString(...)` regardless of
      source SQL type; the JDBC driver coerces (verified for MariaDB BIGINT columns). Rows
      mapped via `TableMapping.mapRow`
    - Build `SyncEvent` per row + per deleted PK with `pk: String`; publish via the Kafka
      producer initialized by `adapter-bootstrap`
    - Snapshot is replaced atomically only after a successful publish — a mid-cycle failure
      replays the same diff next tick

- [todo] R7. **[Phase 2]** The engine MUST run each `TableMapping` on a daemon `ScheduledExecutorService`:
    - Thread name: `nx-db-sync-{schemaName}-{topicSuffix}`
    - First tick fires immediately (initial sync — see R8); subsequent ticks at
      `mapping.tickInterval()`
    - Tick wrapped in `SafeRunnable` so a runaway throwable does not cancel the schedule

- [todo] R8. **[Phase 2]** Initial sync — first tick after `onConnect` — MUST replay the entire table as
  `CREATED` events (previous snapshot empty → diff returns all PKs as created). No special
  bootstrap mode; the engine's normal Phase 1 + Phase 2 path handles this naturally.
    - SC5. For `clan_data` (~1k rows on a typical x20 server) initial sync completes in one
      Phase 2 query, well under 1s.

- [todo] R9. **[Phase 1: module-level only / Phase 2: per-table]** db-sync engine MUST NOT
  propagate exceptions to host-JVM threads. **Phase 1**: only module-level handling —
  `onConnect` failures (SPI not resolved / smoke check throws) are caught and surface as
  `FAILED` / `DEGRADED` per R2. **Phase 2**: every CDC entry point (scheduler tick, Kafka
  producer callback, `JdbcConnectionSource.getConnection()` failure) catches `Throwable`,
  logs via `NxLog`, and transitions the **affected table** to `DEGRADED` — other tables
  continue ticking. Module-level `FAILED` is reserved for non-recoverable conditions:
  0 / >1 `JdbcConnectionSource`; >1 `DbSchemaProvider` (Phase 2); no `l2nx.db.*` fallback
  config when ServiceLoader returns 0 (Phase 3); bundled Hikari pool fails to open
  (Phase 3).

- [todo] R10. **[Phase 2]** Bohpts client + clans MVP — `bohpts-core` repo (private; `E:/bohpts/code/bohpts-core`)
  MUST host a `BohptsDbSchemaProvider` class implementing `DbSchemaProvider` directly (no
  `extends` — there is no vanilla `nx-gs-db-l2j` to inherit from in MVP), plus a
  `META-INF/services/app.l2nx.gs.db.sync.spi.DbSchemaProvider` resource pointing to it.
  Bohpts-core declares `implementation 'app.l2nx:nx-gs-db-sync-core:0.1.0'` from Maven
  Central. Provider contract:
    - `schemaName()` = `"bohpts"`
    - `mappings()` returns exactly one `TableMapping<ClanDto>` for the `clan_data` table:
        - `tableName()` = `"clan_data"`
        - `pkColumn()` = `"clan_id"`
        - `hashedColumns()` = `["clan_name", "clan_level", "leader_id", "ally_id"]`
          (4 plain columns — crest_id, ally_name, ally_crest_id, the four `*_penalty_*` /
          `*_expiry_time` fields and the `membersCount` formula are intentionally
          out-of-scope for MVP per Non-goals)
        - `topicSuffix()` = `"clans"`
        - `strategy()` = `FULL_SCAN`
        - `tickInterval()` = `Duration.ofSeconds(60)`
        - `dtoType()` = `ClanDto.class`
    - `ClanDto` ships in `nx-gs-adapter-api` (Java 8 POJO, hand-written builder) so the
      platform-side consumer compiles against the same wire type. All ID fields declared as
      `String` (cross-schema invariance — see Decisions in tech.md):
        - `String clanId`
        - `String clanName`
        - `Integer clanLevel`
        - `String leaderId`
        - `String allyId`
    - The package for `BohptsDbSchemaProvider` inside bohpts-core is operator-chosen — see
      Open question. No bohpts-internal class names or column conventions leak into this
      monorepo (bohpts-core is private; this monorepo stays open-core).

- [todo] R11. **[Phase 1]** First published versions:
    - `nx-gs-db-sync-core` = `0.1.0` (new module in this monorepo, published to Maven Central).
      Phase 1 ships with `DbSyncModule` only (no CDC engine, no DbSchemaProvider — Phase 2
      bumps the minor). Phase 3 adds `BundledHikariConnectionSource`.
    - `nx-gs-adapter-api` bumped to next minor (adds `AdapterModule`, `ConnectContext`,
      `ModuleStatus` + `Stats` + `PoolStats`, `JdbcConnectionSource`). `SyncEvent` /
      `ClanDto` ship in the Phase 2 minor bump.
    - **No** `nx-gs-db-bohpts` artifact is published — the bohpts schema provider is shipped
      as part of the bohpts-core game-server JAR itself.

**Should:**

- [todo] R12. **[Phase 1: pool only / Phase 2: + tables]** db-sync SHOULD surface per-module
  health on `HeartbeatEvent.enabledModules` via `ModuleStatus` (defined in
  `adapter-modules`). **Phase 1**: `{name: "db-sync", state: "ACTIVE", stats: {pool:
  {busy, idle, total}}}` — pool stats forwarded from `JdbcConnectionSource.stats()`.
  **Phase 2**: `stats.tables` slot added once the table provider SPI lands —
  `tables: ["clan_data"]` (just names; per-table runtime state — `lastSyncEpoch`,
  per-table `state` — pushed into a follow-up slice if real ops need surfaces).

- [todo] R13. **[Phase 2]** db-sync SHOULD enforce a per-table snapshot RAM cap (default 100k rows for
  `FULL_SCAN`). If Phase 1 returns more rows than the cap, log a WARN and skip the cycle —
  protects host JVM from a runaway memory footprint when an operator points the engine at a
  table that grew unexpectedly. Tunable later.

**Could:**

- [todo] R14. db-sync COULD expose per-table cadence overrides via the same config chain as
  `adapter-bootstrap` — e.g. `l2nx.db-sync.tick-interval.clans=120s` — so operators tweak
  cadence without redeploying.

- [todo] R15. db-sync COULD support `SLIDING_WINDOW` strategy for large tables (e.g. `items`
  with ~12M rows): Phase 1 walks PK ranges in 1.2M-row chunks, holding only one window's
  hashes in RAM at a time. Designed in tech.md but not in MVP code.

**Non-goals:**

- **Schema discovery / introspection** — the adapter does not query the host DB's
  `information_schema`. `TableMapping` is the source of truth.
- **Composite PKs** — single-column PK assumption. Tables with multi-column PKs are out of
  scope for MVP; can be added as `CompositeTableMapping` later.
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

- [NEEDS CLARIFICATION: Topic naming strategy. Two options on the table:
  (a) **per-table topics** — `{tenant}.gs.sync.{topicSuffix}` (e.g. `bohpts.gs.sync.clans`).
  Pro: per-table consumer-group sharding, per-table retention policies, easier ACLs.
  Con: dozens of topics per tenant when many tables sync.
  (b) **single topic** — `{tenant}.gs.sync` with `tableName` discriminator on each event.
  Pro: simpler ACL, easier cross-table ordering on platform side.
  Con: one big consumer, retention is uniform.
  Resolved during /specl-plan.]
- [NEEDS CLARIFICATION: Strategy + cadence — operator-static (compiled into TableMapping
  by vanilla module author) vs platform-driven (delivered via `nexus.adapter.sync-config`
  Kafka topic). Static is simpler for MVP; platform-driven enables ops to
  re-tune without redeploying. MVP assumes static; document the migration path to dynamic
  if/when needed.]
- [resolved: Multi-impl `DbSchemaProvider` resolution is a non-issue in MVP. Bohpts-core
  ships exactly one provider; no transitive vanilla JAR exists yet (`nx-gs-db-l2j` deferred
  per Non-goals). The conflict scenario re-emerges only once vanilla L2J ships AND a
  customer takes a transitive dep on it. At that point the resolution strategy (config
  selector / shadow exclusion / vanilla activator JAR) is decided in the second-customer
  feature slice. MVP keeps R3's fail-loud single-impl rule.]
- [resolved: All ID fields serialized as `String` on the wire — PK + FK columns in DTOs +
  `SyncEvent.pk`. Engine reads PK via `rs.getString(pkColumn)` regardless of source SQL type;
  Phase 2 binds via `setString(...)` and lets the JDBC driver coerce. Cross-schema invariance
  (some clients use UUID/composite/INT, bohpts uses BIGINT). See tech.md Decisions.]
- [NEEDS CLARIFICATION: Wire shape of `SyncEvent` — typed `SyncEvent<T>` parameterized by
  DTO type vs erased `SyncEvent { tableName, pk, op, payloadJson }`. Typed gives compile-time
  guarantees on the platform-side consumer; erased simplifies adding new tables (no `api`
  re-publish per table). Tied to topic-naming decision.]
- [NEEDS CLARIFICATION: Kafka key for sync events — pk-string-as-key for partition affinity
  (same row → same partition → ordering guaranteed) or `{schemaName}:{topicSuffix}:{pk}` if
  per-tenant single-topic wins. Decided once topic-naming is decided.]
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
  — `JdbcConnectionSource` design, pool-agnostic borrowing, bohpts reference impl wrapping
  `l2e.gameserver.database.DatabaseFactory`
- Module discovery diagrams: [`module-discovery.md`](./module-discovery.md)
- CRC32 CDC resource estimates: image attached to /specl-take invocation (bohpts x20
  benchmark — Characters 152k / Clans 1k / Items 12M — informs strategy table in tech.md)
