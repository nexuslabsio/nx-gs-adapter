# Item augmentation sync — per-instance life-stone options end-to-end

> Owner: @n1rmata

## Problem

An item's **augmentation** (the life-stone result — the two option ids that grant
stat bonuses and/or a granted skill) is not synced anywhere on the platform. The
per-instance item feed carries `id / itemTemplateId / ownerId / count /
enchantLevel / location / attributes(elementals)` only — augmentation is absent
at every layer (`bohpts-core` mapping → `nx-gs-adapter-api` wire → `gs_items`).

The **static catalog already exists** in `nx-gamedata` (git-ingester-owned,
`v3.3.0_augment_enchant.sql`): `gd_item_options` (option id → color +
gear-score), `gd_item_option_stat_modifiers` (option id → stat bonus),
`gd_item_option_skills` (option id → granted skill), plus the generation tables
(`gd_life_stones`, `gd_life_stone_grades`, `gd_augment_chances`). So "what does
option N grant" is answerable today. What is missing is the join key: **which
two options sit on a given item instance**. Without it the platform cannot tell
an augmented item from a plain one — needed for the market miniapp (sellability
gating + augment display) and item inspection.

This spec covers the **sync only** — moving the per-instance option ids into a
platform table. Rendering/resolving the options against the gd catalog is
deliberately deferred (separate work).

## Goal

Sync per-instance augmentation end-to-end: `bohpts-core` reads the engine's
packed augment int, **decodes it at source** into two explicit option ids, the
adapter carries them on the item wire DTO, and `nx-gameservers` persists them
into a new sparse satellite `gs_item_augmentations` — mirroring the existing
elemental-`attributes` → `gs_item_attributes` child path exactly. The gd catalog
is untouched. No display.

## Decisions

| Decision                          | Choice                                                                                                              |
|-----------------------------------|---------------------------------------------------------------------------------------------------------------------|
| Where the packed int is decoded   | At source — `bohpts-core` `ItemMapping` (platform never sees L2 bit-packing)                                        |
| Wire model                        | Singular `@Nullable ItemAugmentationDbDto augmentation` on `ItemDbDto` (one augment per item)                       |
| Option id representation          | Two explicit ids: `option1Id` (required) + `option2Id` (nullable — absent when the high slot is 0)                  |
| Platform storage                  | New sparse satellite `gs_item_augmentations` (row only for augmented items) — mirrors `gs_item_attributes`          |
| Storage vs. columns on `gs_items` | Satellite, not columns — `gs_items` is the 3.86M-row hot UPSERT table; augment is sparse                            |
| Ingest keep-in-sync               | DELETE-by-touched-item-id + INSERT-if-augmented, per the elementals `batchReplace` idiom                            |
| Engine sentinels on the wire      | None — `-1`/`0` are absorbed at the `bohpts-core` edge; "absent" is expressed only as `null` (never a magic number) |
| Catalog (option id → bonus/skill) | Untouched — already git-owned in `nx-gamedata`                                                                      |
| Wire compatibility                | Additive field → `api/vX.Y.Z` minor bump (not breaking; old consumers ignore the new field)                         |
| Display / resolution              | Out of scope — deferred                                                                                             |

## Naming caveat (read before touching code)

Three similarly-named things — keep them straight:

- **`item_attributes`** — a **bohpts-core engine** host-DB table. Stores the
  packed augment int per item (`itemId`, `augAttributes`; `-1` = none). Legacy
  engine name, read as-is, never renamed (foreign schema).
- **`gs_item_attributes`** — a **platform** table. Stores **elemental
  attributes** (fire/water/wind/… — `gs_item_attributes.type/value`). Nothing to
  do with augmentation.
- **`gs_item_augmentations`** — the **new platform** table this spec adds. Stores
  the decoded augment option ids.

Platform-side naming is `augmentation` throughout (DTO field, table, columns).
`attributes` on the platform always means elementals.

## Wire contract change — `nx-gs-adapter-api` (additive)

New POJO `app.l2nx.gs.adapter.api.kafka.sync.db.item.ItemAugmentationDbDto`,
built to the module's hand-written pattern (Java 8, `final` fields + public
constructor + static `builder()` + `toBuilder()` + `equals`/`hashCode`/
`toString`; no Lombok, no records) — a structural copy of `ItemAttributeDbDto`:

```
ItemAugmentationDbDto {
    int     option1Id;   // low-slot option, always present when augmented
    Integer option2Id;   // high-slot option; null when the item carries only one
}
```

`ItemDbDto` gains one nullable field `@Nullable ItemAugmentationDbDto
augmentation` (`null` = not augmented). Because the DTO is a hand-written POJO,
"adding a field" means touching every member in lockstep: the `final` field +
its `@Nullable` JSpecify getter with Javadoc + the constructor param + the
`Builder` field/setter + `toBuilder()` + `equals`/`hashCode`/`toString`.
Mandatory Javadoc on the new public type and the new getter (module rule).

Additive: gson's `serializeNulls=false` omits the field when `null`, old
consumers ignore an unknown field, and a producer that never sets it emits
nothing. Not breaking → `api/vX.Y.Z` minor bump. The only producer
(`bohpts-core`) and the only consumer (`nx-gameservers`) adopt it in lockstep,
so no transient dual-emit is needed.

Reference existing shape: `ItemDbDto`
(`nx-gs-adapter-api/.../kafka/sync/db/item/ItemDbDto.java` — field + getter +
ctor + `Builder` + `toBuilder` + value semantics), `ItemAttributeDbDto`
(`.../item/ItemAttributeDbDto.java` — the exact template to copy).

> Related contract context (not a change here): the gd-sync gear-score ruleset
> already carries an `AUGMENT` category (`GearScoreRuleGroup.category`), i.e.
> "augment → gear-score/stat contribution" is a modelled, catalog-side concern.
> This spec only lands the per-instance option ids; resolving them against that
> catalog is the deferred display work.

## Source change — `bohpts-core`

`l2e.gameserver.l2nx.sync.db.ItemMapping` (the item `DbSchemaProvider` mapping)
today wires primary source `items` (pk `object_id`) + a single child source
`ItemElementalsChildSource` over `item_elementals` (fk `itemId`), and
`mapEntity` builds `ItemDbDto` via `ItemDbDto.builder()…build()` without
augmentation. `ItemMapping` is modern-Java (uses `List.of`, records, switch
expressions) — only `nx-gs-adapter-api` is constrained to Java 8.

Changes (mirror `ItemElementalsChildSource` exactly — it is the template):

1. **New inner `ChildSource` `ItemAugmentationChildSource`** — `tableName()` =
   `"item_attributes"`, `fkColumn()` = `"itemId"` (same fk as elementals),
   `hashedColumns()` = `List.of("augAttributes")`, `mapRow(rs)` reads the raw
   `int augAttributes`. Add it to the `children()` list
   (`List.of(new ItemElementalsChildSource(), new ItemAugmentationChildSource())`).
   Hashing `augAttributes` makes re-augment/de-augment on an existing item change
   the CRC32 → re-emit. The engine table has one row per item (`itemId` PK), so
   the child yields 0 or 1 row. Identifiers `item_attributes` / `augAttributes`
   satisfy the engine's `^[A-Za-z_][A-Za-z0-9_]{0,63}$` guard.
2. **Decode in `mapEntity`** from `childRowsByTable.get("item_attributes")` (0/1
   row → the raw `augAttributes`):
    - `augAttributes <= 0` → no augmentation → `.augmentation(null)`.
    - otherwise split the two 16-bit slots — `low = augAttributes & 0xFFFF`,
      `high = (augAttributes >>> 16) & 0xFFFF` — and take the **non-zero** ids in
      slot order: `option1Id` = first non-zero, `option2Id` = second non-zero (or
      `null` if only one). Robust to a single-option augment landing in either
      slot. Build `ItemAugmentationDbDto` and pass it to `.augmentation(...)`.

The decode is the same low/high split the engine itself uses
(`Augmentation.java:42-43`; client packet write
`AbstractItemPacket.java:166-167`), so there is no ambiguity about slot order.
Build-specific knowledge (that augmentation lives in `item_attributes`) stays in
`bohpts-core`, never in the adapter contract — consistent with the
tenant/build-agnostic constraint.

Deploying the new `bohpts-core` triggers a full CRC snapshot re-sync of items,
which backfills `gs_item_augmentations` for every currently-augmented item.

## `nx-gameservers`

**Migration** — new `src/main/resources/db/liquibase/v3.22.0_gs_item_augmentations.sql`
(latest today is `v3.21.0`), changeset id `gameservers:3.22.0-item-augmentations`,
forward-only:

```sql
CREATE TABLE gs_item_augmentations
(
    tenant_id   UUID   NOT NULL,
    server_id   UUID   NOT NULL,
    item_id     BIGINT NOT NULL,
    option_1_id INT    NOT NULL,
    option_2_id INT,
    PRIMARY KEY (tenant_id, server_id, item_id),
    FOREIGN KEY (tenant_id, server_id, item_id)
        REFERENCES gs_items (tenant_id, server_id, id) ON DELETE CASCADE
);
```

- **One row per item** (PK `(tenant_id, server_id, item_id)`), unlike
  `gs_item_attributes` which is one row per elemental type. Reflects the fixed
  singular arity of an augment.
- Sparse — only augmented items get a row.
- FK `ON DELETE CASCADE` to `gs_items` — parent hard-delete cleans the augment
  row automatically, mirroring `gs_item_attributes` (`v1.0.0_baseline.sql:108`).
- Option ids are the decoded 16-bit values (≤ 65535) — `INT` suffices; no FK to
  the gd catalog (cross-service; resolution is a read-time concern of the
  deferred display work).

**Ingest** — extend the existing item ingest, mirroring the elementals child
replace:

- `domain/item/ItemIngestor.ingestBatch` (`ItemIngestor.java:20-51`): alongside
  `attributesByItemId`, collect `augmentationByItemId` from
  `ItemDbDto.getAugmentation()` for upserted (non-deleted) items, and add a
  `batchReplaceAugmentations(...)` call next to `batchReplaceAttributes` inside
  the same transaction.
- `infra/postgres/ItemRepository` (`ItemRepository.java`): add
  `batchReplaceAugmentations(...)` — **DELETE by the full set of upserted item
  ids, then INSERT a row only for items whose `augmentation != null`**. Deleting
  across all touched ids (not only the augmented ones) makes de-augmentation
  land correctly — an item that lost its augment has its stale row removed even
  though it carries no augment payload this batch. INSERT SQL:
  `INSERT INTO gs_item_augmentations (tenant_id, server_id, item_id, option_1_id,
  option_2_id) VALUES (...)`. Parent hard-delete needs no explicit handling — the
  FK cascade covers it.

## Rollout (coordinated, additive)

The wire change is additive, but the producer and consumer must both understand
the new field for data to flow, so deploy in order:

1. `nx-gs-adapter-api` — add `ItemAugmentationDbDto` + the `ItemDbDto.augmentation`
   field; release `api/vX.Y.Z` (minor). Wait for Maven Central propagation
   before dependents build.
2. `bohpts-core` — bump adapter-api dep, add the `item_attributes` child source +
   decode in `ItemMapping`; deploy → full item re-sync populates augmentation.
3. `nx-gameservers` — bump adapter-api dep, run the `v3.22.0` migration, extend
   the ingestor/repository. Safe to deploy before or after step 2: with no
   augment field yet on the wire the table simply stays empty until the
   `bohpts-core` re-sync lands, then fills.

No transient dual-emit — additive field, single producer/consumer pair.

## Out of scope

- **Display / resolution** — resolving `option_1_id` / `option_2_id` against the
  gd catalog (`gd_item_options` + `_stat_modifiers` + `_skills`) into a
  human-readable "STR +1, Skill: Focus", and surfacing it in wiki / TMA market /
  bot inventory / admin-fe. Separate work; this spec only lands the data.
- **Sellability gating** — a `sellable`/`tradable` flag for the market
  pre-filter. The authoritative engine reject at store-open time remains the
  source of truth; a synced flag, if wanted, is its own initiative.
- **gd catalog changes** — the option catalog already exists and is git-owned.

## Invariants

- **No engine sentinel crosses the wire.** The engine's `augAttributes = -1`
  ("no augment") and `= 0` are absorbed entirely in `bohpts-core` `mapEntity`,
  which emits `augmentation = null`. The platform never receives, stores, or
  compares against `-1`/`0` — absence is `null` at every layer (wire DTO,
  `gs_item_augmentations` has no row). `option_1_id` is only ever a real,
  non-zero option id, because a satellite row exists only for an augmented item.
- **`option2Id` = `null` for a single-option augment** — never a `0` placeholder.

## Resolved

- Augment option ids are ≤ 65535 (16-bit low/high packing,
  `Augmentation.java:42-43`) → `INT` columns are safe, no `BIGINT`.
- `item_attributes` is one row per item (`itemId` unique; engine uses
  `REPLACE INTO item_attributes VALUES(?,?)` / `SELECT … WHERE itemId=?`), so the
  new `ChildSource` yields 0 or 1 row per parent.
