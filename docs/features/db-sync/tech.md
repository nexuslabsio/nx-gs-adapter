# DB Sync — tech

> Covers: spec.md
> Sibling: [module-discovery.md](./module-discovery.md) (visual walkthroughs of the SPI flow)

## Overview

Two-tier SPI architecture sitting on top of `adapter-bootstrap`. `nx-gs-db-sync-core` registers
itself as an `AdapterModule` (Tier 1, discovered by `nx-gs-adapter-core`) and uses ServiceLoader
internally to discover one `DbSchemaProvider` (Tier 2) on the host classpath. The provider
returns a list of `TableMapping`s — table name, PK column, hashed columns, row-DTO mapper,
strategy, cadence. The engine runs the CRC32 two-phase CDC protocol on a daemon scheduler per
table and publishes change events to Kafka via the producer initialized in `adapter-bootstrap`.
MVP target: bohpts client implements `DbSchemaProvider` directly inside its own `bohpts-core`
repo (no separate published artifact, no template-method indirection — vanilla L2J extraction
deferred to second-customer time per spec Non-goals). One `TableMapping` for `clan_data` (4
hashed columns: clan_name, clan_level, leader_id, ally_id) validates the design end-to-end.

## Structure

- `nx-gs-db-sync-core/src/main/java/app/l2nx/gs/db/sync/` [planned]
    - `DbSyncModule.java` [planned] — implements `app.l2nx.gs.adapter.api.spi.AdapterModule`;
      Tier-1 SPI entry point
    - `spi/DbSchemaProvider.java` [planned] — Tier-2 SPI interface
    - `spi/TableMapping.java` [planned] — Tier-2 SPI; one per table
    - `spi/SyncStrategy.java` [planned] — enum `FULL_SCAN | SLIDING_WINDOW`
    - `engine/CdcEngine.java` [planned] — orchestrator: schedules table ticks, dispatches
      Phase 1 → diff → Phase 2 → publish
    - `engine/Phase1Hasher.java` [planned] — runs the CRC32 hash query, returns `Map<Long, Long>`
    - `engine/Phase2Fetcher.java` [planned] — fetches changed rows by PK list (chunked at 1000),
      calls `mapping.mapRow`
    - `engine/SnapshotStore.java` [planned] — in-memory `Map<PK, CRC32>` per table;
      thread-confined to the per-table scheduler thread
    - `engine/ChangeSet.java` [planned] — `{ created, updated, deleted }` PK sets
    - `kafka/SyncEventPublisher.java` [planned] — translates `(mapping, pk, op, dto)` →
      `SyncEvent` + Kafka key + topic; calls `NxKafka.instance().send(...)`
    - `META-INF/services/app.l2nx.gs.adapter.api.spi.AdapterModule` [planned] — service descriptor
      with `app.l2nx.gs.db.sync.DbSyncModule`
- `bohpts-core/` [planned, **lives in the private bohpts-core repo, NOT this monorepo**]
    - depends on `app.l2nx:nx-gs-db-sync-core:0.1.0` (Maven Central)
    - `<bohpts-package>/BohptsDbSchemaProvider.java` [planned] — implements
      `DbSchemaProvider` directly (no `extends` — vanilla L2J doesn't exist yet);
      `schemaName="bohpts"`. Package up to bohpts-core owner — see spec Open question.
    - `<bohpts-package>/mapping/ClanMapping.java` [planned] — only `TableMapping` in MVP;
      handles Long-to-String conversion for `leader_id` / `ally_id` in `mapRow`
    - `src/main/resources/META-INF/services/app.l2nx.gs.db.sync.spi.DbSchemaProvider`
      [planned] — service descriptor pointing to `BohptsDbSchemaProvider`
- `nx-gs-adapter-api/src/main/java/app/l2nx/gs/adapter/api/`
    - `AdapterModule.java` [planned] — Tier-1 SPI (lands as part of `adapter-bootstrap`
      extension; listed here for completeness because db-sync depends on it)
    - `ConnectContext.java` [planned] — context object passed to `AdapterModule.onConnect`;
      carries DB creds, Kafka producer ref, serverId, tenantSlug
    - `kafka/SyncEvent.java` [planned] — wire shape (final form decided per spec Open
      questions); `pk` field declared as `String`
    - `dto/ClanDto.java` [planned] — clan row DTO (Java 8 POJO, hand-written builder); ID
      fields (`clanId`, `leaderId`, `allyId`) declared as `String`

## Key components

- **`DbSyncModule`** [planned] (R1, R2, R3) — Tier-1 SPI entry point. Lifecycle:
    - `onConnect(ctx)` — discovers the Tier-3 `JdbcConnectionSource` via ServiceLoader
      (designed in `jdbc-connection-source` feature; fail to `FAILED` if zero or >1 found),
      captures Kafka producer reference; nothing schedule-related happens yet
    - `start()` — runs `ServiceLoader.load(DbSchemaProvider.class)`, applies the selection rule
      (zero / one / many — see R3), instantiates `CdcEngine` for the chosen provider, kicks off
      one scheduler per `TableMapping`
    - `stop()` — cancels schedulers (5s `awaitTermination`), drains in-flight Kafka sends
    - `onDisconnect()` — clears `SnapshotStore`. The Tier-3 `JdbcConnectionSource` is owned
      by the host; nothing for db-sync to close.
- **`DbSchemaProvider`** [planned] (R3, R4) — Tier-2 SPI. Single source of truth for "what tables
  look like in this schema". Vanilla impls expose `protected` template-method hooks for column
  / table names so client overrides change one thing without re-implementing the whole provider.
- **`TableMapping<T>`** [planned] (R5) — describes one table generically. CDC engine consumes
  uniformly without knowing the DTO type. Generic `T` carried for compile-time `mapRow` safety.
- **`CdcEngine`** [planned] (R6, R7, R8) — per-provider orchestrator. Owns one
  `ScheduledExecutorService` (single-threaded per `TableMapping` so per-table state never
  needs synchronization), hosts per-table `SnapshotStore`, dispatches Phase 1 → diff → Phase 2
  → publish on every tick.
- **`Phase1Hasher`** [planned] (R6) — issues `SELECT pk, CRC32(CONCAT_WS(',', cols...)) FROM tbl`
  through the Tier-3 `JdbcConnectionSource`. Reads PK via `rs.getString(1)` (ID stringification —
  see Decisions). Returns `Map<String, Long>` (PK → CRC32). Always closes
  `ResultSet` / `Statement` / `Connection` via `try-with-resources` so the connection
  returns to the host's pool promptly.
- **`Phase2Fetcher`** [planned] (R6) — given `Set<PK> changedPks`, builds
  `SELECT * FROM tbl WHERE pk IN (?, ?, ...)`. Chunks at 1000 PKs per query (keeps prepared-
  statement bind size safe + works around per-DB `max_allowed_packet`). Calls
  `mapping.mapRow(rs)` for each row.
- **`SnapshotStore`** [planned] — `Map<Long, Long>` per `TableMapping`, thread-confined to
  the scheduler thread. Wiped on `onDisconnect`. RAM cap (R13) enforced — log + skip if Phase 1
  exceeds the cap. Replacement is "all-or-nothing" — only swapped in after a successful publish
  cycle so a mid-cycle failure replays the same diff next tick.
- **`SyncEventPublisher`** [planned] (R6) — translates `(tableMapping, pk, op, dto)` →
  `SyncEvent` payload + Kafka key + topic name; calls `NxKafka.instance().send(...)`. Kafka
  send is async; the engine does not block waiting for ACKs.
- **JDBC connection borrowing** [planned] (R2) — engine consumes `Connection`s from a
  Tier-3 `JdbcConnectionSource` SPI (designed in the `jdbc-connection-source` feature —
  see Links in spec.md). No pool wrapping in db-sync-core: pool implementation, sizing,
  read-only-user vs shared-pool decisions, JDBC driver bundling are all the host's
  responsibility, exposed through the SPI. Engine's contract per query: borrow
  Connection → `setReadOnly(true)` → execute → close (returns to host pool).
- **`BohptsDbSchemaProvider`** [planned] (R10) — implements `DbSchemaProvider` directly
  (no template-method base class in MVP — vanilla L2J extraction deferred to
  second-customer time per spec Non-goals). Lives in bohpts-core repo. Returns one
  `ClanMapping` from `mappings()`. `schemaName="bohpts"`.
- **`ClanMapping`** [planned] (R10) — only `TableMapping` in MVP. Lives in bohpts-core.
  4 hashed cols (clan_name, clan_level, leader_id, ally_id). `mapRow` converts BIGINT
  `leader_id` / `ally_id` to `String` (zero-as-null convention applied here so the wire
  payload's `leaderId`/`allyId` is `null` for "no value", not `"0"`).

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
DbSyncModule.start()
  → ServiceLoader.load(DbSchemaProvider.class)
  → providers.size():
       0 → log WARN "no DbSchemaProvider on classpath"; state = DISABLED; return
       >1 → log ERROR listing fqcns; state = FAILED; return
       1 → engine = new CdcEngine(provider, jdbcConnectionSource, kafkaProducer, kafkaTopicResolver)
  → for each TableMapping:
        scheduler.scheduleWithFixedDelay(SafeRunnable.wrap(tick), 0, mapping.tickInterval(), ...)
```

### 3. CDC tick (per `TableMapping`)

```
Tick fires
  → current = Phase1Hasher.hash(mapping)              -- Map<PK, CRC32>
  → changeSet = SnapshotStore.diff(mapping, current)  -- created / updated / deleted
  → if changeSet is empty: return                     -- no work, no Phase 2
  → fetched = Phase2Fetcher.fetch(mapping, changeSet.created ∪ changeSet.updated)
                                                      -- List<T> mapped via mapping.mapRow
  → for each row in fetched:
        SyncEventPublisher.publish(mapping, op = CREATED|UPDATED, pk, dto)
  → for each pk in changeSet.deleted:
        SyncEventPublisher.publish(mapping, op = DELETED, pk, null)
  → SnapshotStore.replace(mapping, current)           -- atomic swap; only on success
```

### 4. Initial sync

Same path as (3) — first tick after `onConnect`. `SnapshotStore` is empty for this mapping →
diff yields all PKs as `created`. For `clan_data` (~1k rows) Phase 2 is a single chunked
`IN (...)` query.

### 5. Module shutdown

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
- **Wire types** (in `nx-gs-adapter-api`) — all ID fields are `String`:
    - `SyncEvent` [planned] — final wire shape decided per spec Open questions; candidate
      fields: `tableName`, `op (CREATED|UPDATED|DELETED)`, `pk: String`, `payload`, `timestamp`
    - `ClanDto` [planned]:
        - `String clanId`
        - `String clanName`
        - `Integer clanLevel`
        - `String leaderId` (null when source `leader_id = 0` per L2J convention)
        - `String allyId` (null when source `ally_id = 0`)

## Integration points

- **`:nx-gs-adapter-api`** [planned] (R10, R11) — adds `AdapterModule`, `ConnectContext`,
  `SyncEvent`, `ClanDto`. Bumped to next minor release. Lands in two slices: `AdapterModule` +
  `ConnectContext` arrive with the `adapter-bootstrap` extension; `SyncEvent` + `ClanDto`
  arrive with this feature.
- **`:nx-gs-adapter-core`** (R1) — extends `NxAdapter.start()` with
  `ServiceLoader.load(AdapterModule.class)` invocation; populates
  `HeartbeatEvent.enabledModules`. Lands as part of the `adapter-bootstrap` extension.
- **`:nx-gs-kafka`** (R6) — sync events published via `NxKafka.instance().send(topic, key,
  syncEvent)`. No change to `nx-gs-kafka` API.
- **`:nx-gs-db-sync-core`** [planned] (R1–R9) — new module in this monorepo, published to
  Maven Central as `app.l2nx:nx-gs-db-sync-core`.
- **`bohpts-core`** [planned] (R10) — bohpts-core repo (private) declares
  `implementation 'app.l2nx:nx-gs-db-sync-core:0.1.0'`, hosts `BohptsDbSchemaProvider` +
  `ClanMapping` classes inline in its source tree, and ships
  `META-INF/services/app.l2nx.gs.db.sync.spi.DbSchemaProvider` in its resources. NO
  separate `nx-gs-db-bohpts` artifact is published.
- **`jdbc-connection-source` feature** [planned] (R2) — Tier-3 SPI feature delivering
  `JdbcConnectionSource` + the bundled-Hikari fallback. `nx-gs-db-sync-core` consumes
  the resolved instance via `JdbcConnectionSourceResolver`. Pool implementation choice
  (host Path 1 — bohpts `DatabaseFactory`, vanilla L2J pool, etc. / Path 2 — bundled
  shadowed Hikari 3.4.5 from `l2nx.db.*` config) lives in that feature.
- **Shadowed Hikari 3.4.5** [planned] (R2 fallback path) — bundled in
  `nx-gs-db-sync-core`, relocated to `app.l2nx.shaded.hikari.*` so it cannot collide
  with whatever pool the host JVM already ships. Adds ~150 KB to the
  `nx-gs-db-sync-core.jar`.
- **`nx-tenants` `nexus.adapter.sync-config` Kafka topic** — out of scope for MVP. Future
  feature for platform-driven cadence / strategy overrides.

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
- **All IDs serialized as `String` on the wire.** PK + FK columns in DTOs +
  `SyncEvent.pk` — uniformly `String`. Engine reads PK via `rs.getString(pkColumn)`
  regardless of source SQL type; Phase 2 binds via `setString(...)` and lets the JDBC driver
  coerce (verified for MariaDB BIGINT — driver pushes the conversion to the index, no
  full-table scan). Rationale:
  (1) **Cross-schema invariance** — bohpts uses BIGINT, future customers may use UUID
  (VARCHAR), composite (string-encoded), INT. Wire format stays stable across all.
  (2) **Kafka key is naturally `String`** — no extra conversion at publish time.
  (3) **Platform consumers treat IDs as opaque tokens** — no code path needs the source SQL
  type. Range-queries on IDs (rare and bad practice anyway) work the same lexically.
  (4) **Wire overhead is negligible** — 5–10 bytes per ID per row × 12M rows full-sync ≈
  60–120 MB total bursty cost vs row payloads themselves (~10 KB/row).
  Schema providers handle the **zero-as-null convention** in `mapRow` (L2J FK columns use
  `0` to mean "no value" — bohpts emits `null` in the DTO instead of `"0"` for cleaner
  platform-side semantics).
- **Single-impl assumption for `DbSchemaProvider` discovery.** In MVP, only one provider
  exists on the classpath (the bohpts one, inside bohpts-core). The fail-loud behaviour for
  > 1 providers (R3) surfaces classpath ambiguity to the operator instead of silently picking
  one. The full multi-impl resolution story (config selector / shadow exclusion / activator
  JAR) is open and resolved when vanilla `nx-gs-db-l2j` ships AND a customer ends up with
  both vanilla and bohpts on classpath — see spec Open questions.
- **CRC32 over MD5 / SHA.** CRC32 is computed server-side by MySQL (zero adapter CPU),
  32-bit fits a Java `int`/`long`, collision probability is acceptable for
  change-detection (a missed change is corrected on the next cycle). MD5 / SHA-2 require
  client-side computation, which means transferring all column bytes over the wire — defeats
  the purpose of the lightweight Phase 1.
- **In-memory snapshot only, no persistence.** Cold start replays the whole table as initial
  sync. Persisting snapshots to a sidecar DB would require schema management on the host side
  and create a write-path on a strict-read-only adapter. Rebooting and resyncing 1k clans is
  cheap. Larger tables (items, ~12M rows = ~96 MB initial Kafka burst) re-sync — acceptable
  bursty cost on rare reboots.
- **No transactional consistency between Phase 1 and Phase 2.** Eventual consistency. A row
  deleted between phases simply does not appear in Phase 2 results — engine treats it as "no
  work for this PK"; the deletion is detected on the NEXT cycle via Phase 1 diff. Acceptable
  trade-off; the alternative (REPEATABLE READ for the whole cycle) would hold a long-running
  transaction on the host DB — operator-hostile.
- **Read-only is set per-borrow on every Connection, not at the pool level.** The pool is
  owned by the host (via the Tier-3 `JdbcConnectionSource` SPI — see
  `jdbc-connection-source` feature), so we cannot impose pool-level config. Engine calls
  `connection.setReadOnly(true)` immediately after borrow and before any
  Statement/PreparedStatement is created. Rationale: a `GRANT SELECT`-only MySQL user
  is recommended at the operator side; the read-only flag is belt-and-suspenders
  and a meaningful hint for replication routers (ProxySQL, MaxScale) that route read-only
  connections to replicas — adapter traffic doesn't compete with the game core's writes
  on the primary. Engine NEVER issues DDL or DML — Phase 1 + Phase 2 are pure `SELECT`.
- **Daemon threads, never propagate exceptions.** Same philosophy as `adapter-bootstrap`. Each
  scheduler tick wrapped in `SafeRunnable`; exception → log + per-table `DEGRADED`, never
  bubble out.
- **One scheduler thread per `TableMapping`.** Simplifies state management — `SnapshotStore`
  for a given table is read/written only by its own scheduler thread. Cost: N daemon threads
  for N tables. With ~5 tables per provider this is negligible. A single shared scheduler
  with a thread pool would force defensive synchronization on every snapshot access.
- **Strategy selector in `TableMapping` itself.** `mapping.strategy()` returns
  `FULL_SCAN | SLIDING_WINDOW`. The vanilla module author picks the right strategy per row
  count (clans 1k = FULL_SCAN, items 12M = SLIDING_WINDOW). This is operator-static for MVP;
  platform-driven override is a future capability — see Open questions.

## Extension points

- **New table support (vanilla)** — vanilla `L2jSchemaProvider.mappings()` adds new
  `TableMapping` entries. No engine change.
- **New schema variant (client)** — implement `DbSchemaProvider` directly (MVP path,
  bohpts-style) OR extend an existing vanilla provider via template method
  (post-vanilla path). Either way, ship a `META-INF/services/...DbSchemaProvider`
  descriptor pointing to the client class. Engine treats both identically.
- **New sync strategy** — add an enum constant + a `Strategy` impl in `CdcEngine` (e.g.
  `SLIDING_WINDOW`, `BINLOG_TAIL`). `TableMapping.strategy()` selects it.
- **New module type** — implement `AdapterModule` in a sibling module (e.g.
  `nx-gs-dp-sync-core` for datapack sync). adapter-core ServiceLoader picks it up alongside
  db-sync. Independent lifecycle, independent failure isolation.
- **Custom CDC algorithm** — replace CRC32 with binlog-tail or trigger-based for ops who want
  realtime. Re-implements the engine but reuses `TableMapping` as the schema description.
