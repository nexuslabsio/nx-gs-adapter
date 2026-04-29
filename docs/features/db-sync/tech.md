# DB Sync — tech

> Covers: spec.md
> Sibling: [module-discovery.md](./module-discovery.md) (visual walkthroughs of the SPI flow)

## Overview

Two-tier SPI architecture sitting on top of `adapter-bootstrap`. `nx-gs-db-sync-core`
registers itself as an `AdapterModule` (Tier 1, discovered by `nx-gs-adapter-core`) and uses
ServiceLoader internally to discover one `DbSchemaProvider` (Tier 2) on the host classpath.
The provider returns a list of `EntityMapping`s — table name, PK column, hashed columns,
row-DTO mapper, strategy, cadence. The CDC algorithm itself (CRC32 two-phase protocol,
scheduler, in-memory snapshot, per-table stats) lives in the
[`cdc-engine`](../cdc-engine/spec.md) feature; `DbSyncModule` instantiates the engine after
`DbSchemaProvider` is resolved and feeds it the provider's `EntityMapping`s.
MVP target: bohpts client implements `DbSchemaProvider` directly inside its own `bohpts-core`
repo (no separate published artifact, no template-method indirection — vanilla L2J
extraction deferred to second-customer time per spec Non-goals). One `EntityMapping` for
`clan_data` (4 hashed columns: clan_name, clan_level, leader_id, ally_id) validates the
design end-to-end.

## Structure

- `nx-gs-db-sync-core/src/main/java/app/l2nx/gs/db/sync/`
    - `DbSyncModule.java` — implements `app.l2nx.gs.adapter.api.spi.AdapterModule`;
      Phase 1 owns SPI smoke check + heartbeat enrichment. Phase 2 wires `CdcEngine`
      (defined in [`cdc-engine`](../cdc-engine/tech.md)) with the resolved
      `DbSchemaProvider`'s mappings.
    - `spi/DbSchemaProvider.java` [planned] — Tier-2 SPI interface
    - `spi/EntityMapping.java` [planned] — Tier-2 SPI; one per synced entity
    - `engine/` [planned] — CDC algorithm; structure documented in
      [`cdc-engine/tech.md`](../cdc-engine/tech.md). Engine code physically ships in
      `nx-gs-db-sync-core` JAR; the `cdc-engine` feature is a design-doc slice, not a
      separate Gradle module.
    - `kafka/` [planned] — `SyncEventPublisher` and friends; documented in cdc-engine
      tech.md.
    - `META-INF/services/app.l2nx.gs.adapter.api.spi.AdapterModule` — service descriptor
      with `app.l2nx.gs.db.sync.DbSyncModule`
- `bohpts-core/` [planned, **lives in the private bohpts-core repo, NOT this monorepo**]
    - depends on `app.l2nx:nx-gs-db-sync-core:0.1.0` (Maven Central)
    - `<bohpts-package>/BohptsDbSchemaProvider.java` [planned] — implements
      `DbSchemaProvider` directly (no `extends` — vanilla L2J doesn't exist yet);
      `schemaName="bohpts"`. Package up to bohpts-core owner — see spec Open question.
    - `<bohpts-package>/mapping/ClanMapping.java` [planned] — only `EntityMapping`
      in MVP; `entityName="clan"`, `tableName="clan_data"`. Applies the
      zero-as-null convention to `leader_id` / `ally_id` (`0L` → `null`) in `mapRow`
    - `src/main/resources/META-INF/services/app.l2nx.gs.adapter.api.spi.DbSchemaProvider`
      [planned] — service descriptor pointing to `BohptsDbSchemaProvider`
- `nx-gs-adapter-api/src/main/java/app/l2nx/gs/adapter/api/`
    - `AdapterModule.java` [planned] — Tier-1 SPI (lands as part of `adapter-bootstrap`
      extension; listed here for completeness because db-sync depends on it)
    - `ConnectContext.java` [planned] — context object passed to `AdapterModule.onConnect`;
      carries DB creds, Kafka producer ref, serverId, tenantSlug
    - `kafka/sync/db/SyncEvent.java` — typed wire envelope `SyncEvent<T>` with
      `String entityName`, `long pk`, `String op`, `T payload`, `long timestampEpochMs`
      (api/0.6.0)
    - `kafka/sync/db/ClanDto.java` — clan row DTO (Java 8 POJO, hand-written builder);
      `long clanId` / `int clanLevel` (NOT NULL columns); `Long leaderId` / `Long allyId`
      (nullable per L2J `0`-sentinel convention). Co-located with `SyncEvent<T>` under
      `kafka.sync.db` — DB-sync wire types share one sub-package.

## Key components

- **`DbSyncModule`** (R1, R2, R12, R16) — Tier-1 SPI entry point. Lifecycle:
    - `onConnect(ctx)` — Phase 1: discovers Tier-3 `JdbcConnectionSource` via
      ServiceLoader (0 / >1 → `FAILED`), runs the smoke check
      (`setReadOnly(true)` + `isValid(5)`); pass → state `ACTIVE`, fail →
      `DEGRADED` (source kept so `stats()` still surfaces in heartbeat). No Kafka
      producer capture in Phase 1. **Phase 2** additionally:
        - discovers `DbSchemaProvider` (0 → `DISABLED`, >1 → `FAILED`, 1 → cache);
        - reads `ctx.syncTopics()` — null/empty → log actionable WARN, transition
          to `DISABLED`, no engine instantiated (R16). Defensive path; not expected
          in steady-state operation.
    - `start()` — Phase 1: no-op. **Phase 2**: instantiates `CdcEngine` (see
      cdc-engine/tech.md) with `provider.mappings()` + `JdbcConnectionSource` +
      Kafka producer + `TopicResolver` (built from `ctx.syncTopics()`); calls
      `engine.start()` which schedules per-entity daemon ticks. Per-entity
      missing-topic situations are handled inside the engine (R17 — entity →
      `DEGRADED`, no scheduler) without affecting the module's overall state.
    - `stop()` — Phase 1: no-op. **Phase 2**: `engine.stop()` — cancels schedulers,
      drains in-flight Kafka sends.
    - `onDisconnect()` — clears the cached `JdbcConnectionSource` reference. The
      source itself is host-owned; db-sync does not close it. **Phase 2**: also
      drops engine reference (snapshots are GC'd).
    - `currentStatus()` — overrides the default to surface
      `JdbcConnectionSource.stats()` in `ModuleStatus.Stats.pool` (Phase 1) and
      `CdcEngine.currentEntityStats()` in `Stats.entities` (Phase 2). Tolerates a
      throwing `stats()` impl by logging and falling back to empty stats.
- **`DbSchemaProvider`** [planned] (R3, R4) — Tier-2 SPI. Single source of truth for "what tables
  look like in this schema". Vanilla impls expose `protected` template-method hooks for column
  / table names so client overrides change one thing without re-implementing the whole provider.
- **`EntityMapping<T>`** [planned] (R5) — describes one table generically. CDC engine consumes
  uniformly without knowing the DTO type. Generic `T` carried for compile-time `mapRow` safety.
- **CDC engine** [planned] — `CdcEngine`, `TableSyncTask`, `Phase1Hasher`,
  `Phase2Fetcher`, `SnapshotStore`, `WindowPlanner`, `SyncEventPublisher` are designed
  in [`cdc-engine/tech.md`](../cdc-engine/tech.md). `DbSyncModule.start()` (Phase 2)
  instantiates `new CdcEngine(provider.mappings(), jdbcConnectionSource, kafkaProducer)`
  and calls `engine.start()`; `currentStatus()` calls `engine.currentTableStats()` to
  populate `ModuleStatus.Stats.entities[]`. Engine code physically lives in the
  `nx-gs-db-sync-core` JAR but db-sync's design surface stops at "wire the engine with
  the resolved provider's mappings".
- **`BohptsDbSchemaProvider`** [planned] (R10) — implements `DbSchemaProvider` directly
  (no template-method base class in MVP — vanilla L2J extraction deferred to
  second-customer time per spec Non-goals). Lives in bohpts-core repo. Returns one
  `ClanMapping` from `mappings()`. `schemaName="bohpts"`.
- **`ClanMapping`** [planned] (R10) — only `EntityMapping` in MVP. Lives in
  bohpts-core. `entityName = "clan"`, `tableName = "clan_data"`. 4 hashed cols
  (`clan_name`, `clan_level`, `leader_id`, `ally_id`). `mapRow` keeps BIGINT
  `leader_id` / `ally_id` as `Long`, applying the zero-as-null convention so the
  wire payload's `leaderId` / `allyId` is `null` for "no value" rather than `0L`.

## Data flows

### 1. Module discovery (Tier 1) at adapter startup

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

### 2. Module discovery (Tier 2) inside db-sync-core

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
  → engine.start()                                    -- per-entity daemon ticks
                                                         (cdc-engine R5)
```

### 3. CDC tick + initial sync

Documented in [`cdc-engine/tech.md`](../cdc-engine/tech.md) data flows. Initial sync is
the first tick after `engine.start()` with empty `SnapshotStore` — same code path,
diff yields every PK as `created` (cdc-engine R7).

### 4. Module shutdown

```
NxAdapter.shutdown()
  → for each AdapterModule (reverse discovery order):
        module.stop()              -- cancel schedulers, drain Kafka sends
  → for each AdapterModule:
        module.onDisconnect()      -- clear snapshots (Tier-3 source owned by host)
  → close NxKafka producer
  → state = CLOSED
```

## Data model

- **No adapter-side persistence.** `SnapshotStore` lives in heap and is lost on JVM restart.
  Cold start replays everything via initial sync. Acceptable bursty cost on rare reboots.
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
    - `SyncEvent` [planned] — final wire shape decided per spec Open questions;
      candidate fields: `entityName`, `op (CREATED|UPDATED|DELETED)`, `pk: long`,
      `payload`, `timestamp`
    - `ClanDto` [planned]:
        - `long clanId` (PK, `NOT NULL`)
        - `String clanName` (`NOT NULL`)
        - `int clanLevel` (`NOT NULL`, source default `0`)
        - `Long leaderId` (null when source `leader_id = 0` per L2J convention)
        - `Long allyId` (null when source `ally_id = 0`)

## Integration points

- **`:nx-gs-adapter-api`** [planned] (R10, R11) — adds `AdapterModule`,
  `ConnectContext`, `SyncEvent`, `ClanDto`. Bumped to next minor release. Lands in
  two slices: `AdapterModule` + `ConnectContext` arrive with the `adapter-bootstrap`
  / `adapter-modules` extension; `SyncEvent` (with `entityName`, `pk: long`) +
  `ClanDto` (Long ID fields) arrive with this feature in api/0.6.0 alongside
  `EntityStats` / `EntityState` / `ChangesSummary` and
  `ConnectResponse.syncTopics`.
- **`:nx-gs-adapter-core`** (R1) — extends `NxAdapter.start()` with
  `ServiceLoader.load(AdapterModule.class)` invocation; populates
  `HeartbeatEvent.enabledModules`. Lands as part of the `adapter-bootstrap` extension.
- **`:nx-gs-kafka`** — sync events published via `NxKafka.instance().send(topic, key,
  syncEvent)` from `cdc-engine`'s `SyncEventPublisher`. No change to `nx-gs-kafka` API.
- **`:nx-gs-db-sync-core`** (R1–R9) — new module in this monorepo, published to
  Maven Central as `app.l2nx:nx-gs-db-sync-core`. Phase 1 released as `0.1.0`
  with `DbSyncModule` only (no engine, no `DbSchemaProvider` SPI yet).
- **`bohpts-core`** [planned] (R10) — bohpts-core repo (private) declares
  `implementation 'app.l2nx:nx-gs-adapter-api:0.6.0'` (Tier-2 SPI lives in api;
  bohpts-core does NOT need a runtime dep on `nx-gs-db-sync-core`), hosts
  `BohptsDbSchemaProvider` + `ClanMapping` classes inline in its source tree, and
  ships `META-INF/services/app.l2nx.gs.adapter.api.spi.DbSchemaProvider` in its
  resources. NO separate `nx-gs-db-bohpts` artifact is published.
- **`jdbc-connection-source` feature** [planned] (R2) — Tier-3 SPI feature delivering
  `JdbcConnectionSource` + the bundled-Hikari fallback. `nx-gs-db-sync-core` consumes
  the resolved instance via `JdbcConnectionSourceResolver`. Pool implementation choice
  (host Path 1 — bohpts `DatabaseFactory`, vanilla L2J pool, etc. / Path 2 — bundled
  shadowed Hikari 3.4.5 from `l2nx.db.*` config) lives in that feature.
- **Shadowed Hikari 3.4.5** [planned] (R2 fallback path) — bundled in
  `nx-gs-db-sync-core`, relocated to `app.l2nx.shaded.hikari.*` so it cannot collide
  with whatever pool the host JVM already ships. Adds ~150 KB to the
  `nx-gs-db-sync-core.jar`.
- **`fastutil-core`** [planned] (cdc-engine R4) — added as `implementation` dep on
  `nx-gs-db-sync-core` (~3 MB). Used by the engine's `Long2IntOpenHashMap` snapshots.
- **`cdc-engine` feature** — engine code physically lives in `nx-gs-db-sync-core` JAR;
  design surface (algorithm, scheduler, RAM cap, query timeout, per-table heartbeat
  stats) lives in [`cdc-engine/spec.md`](../cdc-engine/spec.md) +
  [`cdc-engine/tech.md`](../cdc-engine/tech.md). `DbSyncModule` instantiates `CdcEngine`
  in Phase 2 `start()`.
- **`nx-tenants` `nexus.adapter.sync-config` Kafka topic** — out of scope for MVP.
  Future feature for platform-driven cadence / strategy overrides; tracked under
  cdc-engine R14.

## Decisions

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
- **Read-only is set per-borrow on every Connection, not at the pool level.** The pool is
  owned by the host (via the Tier-3 `JdbcConnectionSource` SPI — see
  `jdbc-connection-source` feature), so we cannot impose pool-level config. The engine
  calls `connection.setReadOnly(true)` immediately after borrow and before any
  Statement/PreparedStatement is created. Rationale: a `GRANT SELECT`-only MySQL user
  is recommended at the operator side; the read-only flag is belt-and-suspenders
  and a meaningful hint for replication routers (ProxySQL, MaxScale) that route read-only
  connections to replicas — adapter traffic doesn't compete with the game core's writes
  on the primary. Engine NEVER issues DDL or DML — Phase 1 + Phase 2 are pure `SELECT`.
- **Engine-algorithm decisions (CRC32 sufficiency, fastutil RAM model, per-query consistent
  snapshot, MIN/MAX recompute, one scheduler thread per mapping, strategy selection in
  `EntityMapping`, all-or-nothing snapshot swap) live in
  [`cdc-engine/tech.md`](../cdc-engine/tech.md) Decisions** to keep db-sync's design
  surface focused on module wiring + Tier-2 SPI shape.

## Extension points

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
  [`cdc-engine/tech.md`](../cdc-engine/tech.md) Extension points.
