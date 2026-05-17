# Plan: Multi-source CDC + DELETE envelope fix

> **Covers:**
> - [`cdc-engine`](../spec.md) — algorithm rework: per-source Phase 1 + BIT_XOR aggregate,
    > per-source Phase 2 fetch + `mapEntity` assembly, envelope-based windowing (closes the
    > DELETE-at-boundary bug).
> - [`db-sync`](../../db-sync/spec.md) R5 — Tier-2 SPI shape revision: split
    > `EntityMapping<T>` into `PrimarySource` + `List<ChildSource>` + `mapEntity(...)`.
> - `nx-gs-adapter-api` — bumped to `0.7.0` (breaking SPI change). No backward-compat
    > shims; pre-prod state, single tenant (bohpts), free hand.
>
> **Out of scope (deferred):**
> - 1 table → N entities fan-out (rare; not in this round per user decision).
> - Composite/non-numeric PKs (still Non-goal).
> - Persisted snapshot (R18 — separate post-MVP slice).
>
> **Resolved decisions (lock into specs in M1):**
> - **Multi-source assembly model:** `EntityMapping` declares one `PrimarySource`
    > (drives windowing + identity) and zero-or-more `ChildSource`s (each with FK back to
    > primary's PK). Entity DTO is built by `mapEntity(primaryRow, childRowsByTable)` —
    > single hook, no per-shape (ONE/MANY/KV) enum; the impl decides how to fold child
    > lists into the DTO.
> - **Phase 1 hash combination — `BIT_XOR` per child, XOR-fold across sources.** Per
    > child: `SELECT fk, BIT_XOR(CRC32(CONCAT_WS(',', cols))) FROM child WHERE fk
>   BETWEEN ? AND ? GROUP BY fk`. Per entity PK: `entityCrc[pk] = primaryCrc[pk] XOR
>   child1Crc[pk] XOR child2Crc[pk] XOR ...`. Order-insensitive; no per-row ordering
    > contract on children. Collision risk for two child rows with identical CRC32
    > inside the same FK is acknowledged (1/2^32 per pair) and accepted for
    > eventual-consistency game data — flagged as a known limitation in tech.md
    > Decisions; row-count guard (`XOR COUNT(*)`) deferred until a real collision
    > surfaces.
> - **DELETE detection — snapshot envelope.** Window planner takes
    > `[min(MIN_db, MIN_snapshot), max(MAX_db, MAX_snapshot)]`. Any PK ever stored in
    > snapshot is checked against current scan; deletion of extreme PKs (which shrinks
    > `MIN/MAX_db`) no longer escapes detection. `SnapshotStore` exposes
    > `minPk(entity)` / `maxPk(entity)` (cheap on AVL tree: `firstLongKey` /
    > `lastLongKey`).
> - **Orphan child rows (FK with no primary row) are ignored.** Aggregate CRC is
    > keyed by `primaryHash.keys()` only; child rows for missing parent PKs are
    > dropped silently.
> - **Per-source independence — no JOIN.** Each source is one SQL statement bounded
    > by `[from, to]` on PK/FK; engine never emits cross-source JOINs.

## Approach

Six layers, sequentially buildable:

1. **Specs** — lock decisions: cdc-engine spec.md/tech.md + db-sync spec.md R5
   reflect multi-source SPI + envelope rule.
2. **SPI v0.7.0** — break `EntityMapping<T>` into `PrimarySource<P>` +
   `List<ChildSource<C>>` + `T mapEntity(...)`. Add `ClanSkillDbDto`. Extend `ClanDbDto`
   with `List<ClanSkillDbDto> skills`. api artifact bumps to `0.7.0`.
3. **DELETE envelope fix** — `SnapshotStore.minPk/maxPk`; `WindowPlanner` consumes
   them, partitions enveloped range. Single, focused change — testable in isolation.
4. **Multi-source engine** — `Phase1Hasher` per-source SQL; engine XOR-folds into
   `currentScan`. `Phase2Fetcher` per-source IN-fetch; engine groups child rows by
   FK and calls `mapping.mapEntity(...)` per PK. `ChangeSet.diff` and snapshot-swap
   path stay byte-identical (CRC compare logic is pure XOR-already).
5. **Bohpts ClanMapping** — split into `ClanPrimarySource` + `ClanSkillsChildSource`;
   `mapEntity` assembles `ClanDbDto` with `skills`.
6. **E2E + version tags** — extend `CdcEngineE2ETest` with skills-CRUD scenarios
   (insert child / update child / delete child / delete primary) + DELETE-envelope
   regression (delete `MAX(clan_id)` row, assert tombstone fires next cycle).
   Tag `api/v0.7.0` + `db-sync/v0.2.0`.

## Milestones

### Specs

1. [pending — already drafted in this PR; awaiting milestone approval]
   **Update `cdc-engine/spec.md`.** Lock resolved decisions above into Open
   questions; rewrite R1 (per-source Phase 1 + XOR aggregate; per-source Phase 2 +
   `mapEntity`); rewrite R2 (envelope windowing); add R20 (`BIT_XOR` child aggregate
   rule); update Non-goals (no JOINs across sources; orphan children dropped); add
   Decisions: BIT_XOR collision rationale, envelope-DELETE rationale.

2. [pending — already drafted in this PR; awaiting milestone approval]
   **Update `cdc-engine/tech.md`.** Phase1Hasher section: per-source SQL + XOR fold.
   Phase2Fetcher section: per-source IN-query + child-row grouping. WindowPlanner:
   envelope query path. SnapshotStore: `minPk`/`maxPk` accessors. New Decisions:
   BIT_XOR + envelope. Updated cycle data-flow listing.

3. [pending] **Update `db-sync/spec.md` R5.** Replace single-table `EntityMapping`
   contract with `PrimarySource` + `List<ChildSource>` + `mapEntity`. Update R10
   (bohpts mapping shape) to enumerate `ClanSkillsChildSource` and `skills` field on
   `ClanDbDto`. Bump R11 versions to `nx-gs-adapter-api 0.7.0` +
   `nx-gs-db-sync-core 0.2.0`. Mark `db-sync` R5 / R10 / R11 from `[done]` →
   `[wip]` for the duration of this slice.

#### Checkpoint — specs locked

`cdc-engine` and `db-sync` specs describe multi-source + envelope semantics; SPI
shape is unambiguous; downstream milestones implement against frozen contracts.

### SPI v0.7.0 — `nx-gs-adapter-api`

4. [pending] **Define `PrimarySource<P>` interface** in
   `app.l2nx.gs.adapter.api.spi`:
   ```java
   interface PrimarySource<P> {
       String tableName();
       String pkColumn();
       List<String> hashedColumns();
       P mapRow(ResultSet rs) throws SQLException;
   }
   ```
   `P` is an opaque per-source row record (impl-defined). No builder helpers in the
   SPI — providers can use plain finals.

5. [pending] **Define `ChildSource<C>` interface** in
   `app.l2nx.gs.adapter.api.spi`:
   ```java
   interface ChildSource<C> {
       String tableName();
       String fkColumn();        // FK referencing primary's PK column
       List<String> hashedColumns();
       C mapRow(ResultSet rs) throws SQLException;
   }
   ```
   No `orderColumn` (rows arrive in DB-default order; `mapEntity` may sort if it
   cares). No `shape` enum (ONE/MANY/KV) — `mapEntity` decides folding.

6. [pending] **Rewrite `EntityMapping<T>`** in
   `app.l2nx.gs.adapter.api.spi`:
   ```java
   interface EntityMapping<T> {
       String entityName();
       Class<T> dtoType();
       PrimarySource<?> primary();
       List<ChildSource<?>> children();   // may be empty (single-table mode)
       T mapEntity(Object primaryRow, Map<String, List<Object>> childRowsByTable);
   }
   ```
   `primary()` and `children()` use wildcards because the engine treats per-source
   rows as opaque `Object`; impl casts inside `mapEntity` (it owns both the
   `mapRow` producers and `mapEntity` consumer — type-safety is impl-local). Old
   single-table fields (`tableName`, `pkColumn`, `hashedColumns`, `mapRow`) are
   removed wholesale (no deprecation step — pre-prod, breaking is fine).

7. [pending] **Add `ClanSkillDbDto`** in `app.l2nx.gs.adapter.api.kafka.sync.db`:
   ```java
   final class ClanSkillDbDto {
       int skillId;
       int skillLevel;
       // hand-written builder, equals, hashCode, toString
   }
   ```
   Per user scope: only `skill_id` + `skill_level` (no `sub_pledge_id`,
   `skill_name`).

8. [pending] **Extend `ClanDbDto`** with `List<ClanSkillDbDto> skills` field. Hand-add
   to existing builder; serializable as JSON array. Empty list when clan has no
   skills (NOT null — `Collections.emptyList()` default).

9. [pending] **Bump `nx-gs-adapter-api` to `0.7.0`** in `build.gradle.kts`. Tag
   deferred to M19.

#### Checkpoint — SPI compiles

`nx-gs-adapter-api-0.7.0` compiles; Tier-2 SPI types (`PrimarySource`, `ChildSource`,
`EntityMapping`, `ClanSkillDbDto`, extended `ClanDbDto`) resolve. Downstream
(`nx-gs-db-sync-core`, `bohpts-core`) targets it via composite include for the rest
of the plan.

### DELETE envelope fix

10. [pending] **`SnapshotStore.minPk(entity)` / `maxPk(entity)`.** Backed by
    `Long2IntAVLTreeMap.firstLongKey()` / `lastLongKey()` — O(log N), no scan.
    Returns `OptionalLong` (`empty` when entity has no entries — initial cold
    state). Unit test: empty store → both empty; populated store → both reflect
    extremes correctly.

11. [pending] **`WindowPlanner.plan` consumes envelope.** New signature:
    `plan(EntityMapping, Connection, SnapshotStore, int rowsPerWindow)`. Logic:
    - Run `SELECT MIN(pk), MAX(pk) FROM <primary>` → `minDb`, `maxDb`
      (both `OptionalLong`; empty when table is empty).
    - Read `snapshot.minPk(entity)` / `snapshot.maxPk(entity)` → `minSnap`, `maxSnap`.
    - Envelope:
        - both empty (cold + empty table) → return `Collections.emptyList()`.
        - one side empty → use the other.
        - both populated → `minEnv = min(minDb, minSnap)`,
          `maxEnv = max(maxDb, maxSnap)`.
    - Partition `[minEnv, maxEnv]` into half-open windows by `rowsPerWindow`
      (existing math).

    Unit tests:
    - `minDb` shrinks from snapshot (deleted MIN row): envelope still covers old
      MIN → window includes deleted PK.
    - `maxDb` shrinks from snapshot (deleted MAX row): envelope still covers old
      MAX → window includes deleted PK.
    - All rows deleted: `minDb`/`maxDb` empty; envelope = `[minSnap, maxSnap]`;
      Phase 1 returns empty currentScan; diff produces all snapshot keys as
      deleted.

#### Checkpoint — DELETE bug fixed

Unit-level proof of envelope behavior; full E2E regression deferred to M18.

### Multi-source engine — Phase 1 + Phase 2 + EntitySyncTask

12. [pending] **`Phase1Hasher.hashPrimary(...)` (renamed)** — same SQL as today, but
    typed against `PrimarySource` instead of the old single-table mapping. Returns
    `Long2IntMap`.

13. [pending] **`Phase1Hasher.hashChild(...)` — new method.** SQL:
    ```sql
    SELECT <fkColumn>,
           BIT_XOR(CRC32(CONCAT_WS(',', col1, col2, ...))) AS xor_crc
    FROM   <child.tableName>
    WHERE  <fkColumn> BETWEEN ? AND ?
    GROUP BY <fkColumn>
    ```
    Same `START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY` wrapper. Same
    `Statement.setQueryTimeout`. Returns `Long2IntMap` (FK → XOR-aggregate CRC).
    Unit test: 3 child rows for FK=1 → XOR matches Java-side
    `CRC32(row1) ^ CRC32(row2) ^ CRC32(row3)`.

14. [pending] **Aggregate CRC fold in `EntitySyncTask`.** After per-source hashes
    complete for a window:
    ```java
    Long2IntOpenHashMap currentScan = primaryHash;          // start with primary
    for (ChildSource<?> child : mapping.children()) {
        Long2IntMap childHash = phase1Hasher.hashChild(child, window, conn);
        for (entry : childHash) {
            // Only fold child contribution into PKs that exist in primary;
            // orphan child rows (FK without primary parent) are dropped.
            if (currentScan.containsKey(entry.pk)) {
                currentScan.put(entry.pk, currentScan.get(entry.pk) ^ entry.xorCrc);
            }
        }
    }
    ```
    `ChangeSet.diff(currentScan, prevKeysInRange, snapshot, entity)` runs unchanged
    against the aggregated map.

15. [pending] **`Phase2Fetcher.fetchPrimary(...)` (renamed)** — same chunked
    `SELECT * FROM primary WHERE pk IN (...)` as today, but typed against
    `PrimarySource`; returns `Long2ObjectMap<Object>` (PK → opaque primary row).

16. [pending] **`Phase2Fetcher.fetchChild(...)` — new method.** Per child source:
    ```sql
    SELECT * FROM <child.tableName> WHERE <fkColumn> IN (?, ?, ...)
    ```
    Chunked at 1000 FKs (same as primary), same per-chunk consistent-snapshot
    transaction, same query timeout. Calls `child.mapRow(rs)` per row. Groups
    results by FK. Returns `Long2ObjectMap<List<Object>>` (FK → list of child
    rows). Empty list for FKs with no rows (caller distinguishes via
    `childRowsByFk.get(pk)` returning null vs empty).

17. [pending] **`EntitySyncTask` — Phase 2 + publish rewrite.** For each `created ∪
    updated` PK:
    ```java
    Object primaryRow = primaryRowsByPk.get(pk);
    Map<String, List<Object>> childRowsByTable = new HashMap<>();
    for (ChildSource<?> child : mapping.children()) {
        List<Object> rows = childRowsByFk.get(child.tableName()).getOrDefault(pk, Collections.emptyList());
        childRowsByTable.put(child.tableName(), rows);
    }
    T dto = mapping.mapEntity(primaryRow, childRowsByTable);
    publisher.publish(mapping, op, pk, dto, topic);
    ```
    DELETE path unchanged (`payload=null` tombstone, no `mapEntity` call).

18. [pending] **DEGRADED triage extension.** Per-source query failure (timeout /
    SQLException on a child) → entity DEGRADED for the cycle, snapshot untouched,
    next tick retries from start. Same surface area as primary-source failures
    today; just plumbed through the per-source loop.

#### Checkpoint — engine compiles + multi-source unit tests pass

Phase1Hasher / Phase2Fetcher / EntitySyncTask unit tests cover happy path + one
failure mode each. Multi-source XOR aggregation has its own test (3 children → XOR
fold matches expected). E2E deferred to M21.

### Bohpts impl — `E:/bohpts/code/bohpts-core`

19. [pending] **Split `BohptsDbSchemaProvider.ClanMapping` into multi-source
    shape:**
    - `ClanPrimarySource` (`tableName="clan_data"`, `pkColumn="clan_id"`,
      `hashedColumns=[clan_name, clan_level, leader_id, ally_id]`,
      `mapRow → ClanRow` record).
    - `ClanSkillsChildSource` (`tableName="clan_skills"`, `fkColumn="clan_id"`,
      `hashedColumns=[skill_id, skill_level]`, `mapRow → ClanSkillRow` record).
    - `ClanMapping.mapEntity(primaryRow, children)` casts `primaryRow` to
      `ClanRow`, reads `children.get("clan_skills")`, casts each to
      `ClanSkillRow`, builds `ClanSkillDbDto` list, assembles `ClanDbDto.builder()
      .skills(...).build()`. `nullIfZero` helper retained for `leaderId`/`allyId`.
    - `ClanRow` / `ClanSkillRow` are package-private final classes (Java 8 — no
      records).
    - Service descriptor (`META-INF/services/...`) unchanged — still points at
      `BohptsDbSchemaProvider`.

20. [pending] **Bohpts-core compile-time dep update.** Bump
    `app.l2nx:nx-gs-adapter-api` from `0.6.0` to `0.7.0`. Verify bohpts-core builds
    against the new SPI shape; refactor `ClanMapping` accordingly.

#### Checkpoint — bohpts compiles against api/0.7.0

Bohpts-core builds with new SPI; `BohptsDbSchemaProvider` returns one
`EntityMapping<ClanDbDto>` with primary + 1 child + `mapEntity`.

### End-to-end smoke + regression

21. [pending] **Extend `CdcEngineE2ETest`.** Testcontainers MySQL fixture grows:
    `clan_data` (3 rows) + `clan_skills` (5 rows across 2 of the 3 clans). Existing
    `FakeBohptsDbSchemaProvider` test resource gets an in-test `ClanSkillsChildSource`.
    New assertions:
    - **Initial sync** — 3 `CREATED` events; payloads include `skills` arrays
      (clan A → 3 skills, clan B → 2 skills, clan C → empty list).
    - **Add child row** — `INSERT INTO clan_skills VALUES (clanA, newSkill, lvl1)`;
      next cycle emits exactly 1 `UPDATED` for clan A with the augmented skills
      list.
    - **Update child row** — `UPDATE clan_skills SET skill_level=lvl2 WHERE
      clan_id=clanA AND skill_id=existingSkill`; next cycle emits 1 `UPDATED` for
      clan A with the new level.
    - **Delete child row** — `DELETE FROM clan_skills WHERE clan_id=clanB AND
      skill_id=...`; next cycle emits 1 `UPDATED` for clan B with the shorter
      list.
    - **Delete primary row** — `DELETE FROM clan_data WHERE clan_id=clanC` (clan
      with no children); next cycle emits 1 `DELETED` tombstone for clan C.
    - **DELETE envelope regression** — fixture bootstraps with PKs `[1, 2,
      MAX]`; after initial sync, `DELETE FROM clan_data WHERE clan_id=MAX`; assert
      next cycle emits exactly 1 `DELETED` tombstone for `MAX` (would silently
      escape under the pre-fix MIN/MAX-only window planning). Symmetric subtest
      for deleting `MIN(clan_id)`.
    - HeartbeatEvent assertion unchanged (still surfaces `entities[clan]=HEALTHY`).

22. [pending — manual operator step] **Manual smoke in bohpts-core dev.** Same
    flow as M38 of `db-sync/clan-sync-mvp.md` plus skills CRUD via direct MySQL +
    extreme-PK delete. Tail logs for `UPDATED` after child mutation and `DELETED`
    after extreme-PK removal.

### Versions and publishing

23. [pending — manual git-tag step] **Tag and publish.**
    - Tag `api/v0.7.0` → `nx-gs-adapter-api-0.7.0` to Maven Central (breaking SPI
      change; jump from 0.6.0).
    - Tag `db-sync/v0.2.0` → `nx-gs-db-sync-core-0.2.0` (carries M11–M21 multi-source
      engine; minor bump because behavior changed but module name + Tier-1 contract
      stable).
    - `:nx-gs-adapter-core` stays at `0.3.2` (no wire change in `/connect` /
      `ConnectContext` for this slice).
    - `:nx-gs-kafka` stays at `0.2.0` (no wire change).
    - Verify all artifacts resolve from Maven Central before bohpts-core switches
      its `mavenLocal()` fallback off.

## Notes

- **BIT_XOR collision risk** is acknowledged in spec/tech Decisions but NOT mitigated
  in this slice (`XOR COUNT(*)` row-count guard considered, deferred). If real-world
  collisions surface during M21 e2e or M22 manual smoke, M-extra: add `^ COUNT(*)` to
  the child aggregate query and re-publish api/0.7.1 — purely additive on the wire
  (snapshot CRC values change once on rollout, behaves like an initial-sync replay
  for affected entities).
- **Single-table mode** (`children() = []`) is preserved as a degenerate case of the
  new SPI — orphan-children loop is a no-op. Future entities with no children
  (e.g. `item`) declare `Collections.emptyList()` and behave exactly like the
  pre-0.7.0 path.
- **Envelope cost** — `SnapshotStore.minPk/maxPk` are O(log N) on AVL tree; no
  measurable Phase 0 overhead vs the existing `SELECT MIN/MAX` query.
