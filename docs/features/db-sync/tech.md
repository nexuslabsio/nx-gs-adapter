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
    - `engine/` — CDC algorithm; structure documented in
      [`cdc-engine/tech.md`](../cdc-engine/tech.md). Engine code physically ships in
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
    - `kafka/sync/db/ClanDto.java` — clan row DTO (Java 8 POJO, hand-written builder);
      `long clanId` / `int clanLevel` (NOT NULL columns); `Long leaderId` / `Long allyId`
      (nullable per L2J `0`-sentinel convention). Co-located with `SyncEvent<T>` under
      `kafka.sync.db` — DB-sync wire types share one sub-package.

## Key components

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
  [`cdc-engine/spec.md`](../cdc-engine/spec.md) R1 + R20.
- **CDC engine** — `CdcEngine`, `EntitySyncTask`, `Phase1Hasher`,
  `Phase2Fetcher`, `SnapshotStore`, `WindowPlanner`, `SyncEventPublisher`,
  `TopicResolver`, `EntityStatsTracker`, `EngineConfig`,
  `ConfigResolutionLogger` are designed in
  [`cdc-engine/tech.md`](../cdc-engine/tech.md). The module assembles the
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
  → engine.start()                                    -- shared scheduler pool
                                                         nx-cdc-pool-<schema>-N
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
    - `SyncEvent` — final wire shape:
      candidate fields: `entityName`, `op (CREATED|UPDATED|DELETED)`, `pk: long`,
      `payload`, `timestamp`
    - `ClanDto`:
        - `long clanId` (PK, `NOT NULL`)
        - `String clanName` (`NOT NULL`)
        - `int clanLevel` (`NOT NULL`, source default `0`)
        - `Long leaderId` (null when source `leader_id = 0` per L2J convention)
        - `Long allyId` (null when source `ally_id = 0`)

## Integration points

- **`:nx-gs-adapter-api`** (R10, R11) — adds `AdapterModule`,
  `ConnectContext`, `SyncEvent`, `ClanDto`. Bumped to next minor release. Lands in
  two slices: `AdapterModule` + `ConnectContext` arrive with the `adapter-bootstrap`
  / `adapter-modules` extension; `SyncEvent` (with `entityName`, `pk: long`) +
  `ClanDto` (Long ID fields) arrive with this feature in api/0.7.0 alongside
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
- **Read-only enforced at the SQL level, not via `Connection.setReadOnly`.** Every
  Phase 1 / Phase 2 query runs inside `START TRANSACTION WITH CONSISTENT SNAPSHOT,
  READ ONLY` (see [`cdc-engine` R11](../cdc-engine/spec.md)). The engine deliberately
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
