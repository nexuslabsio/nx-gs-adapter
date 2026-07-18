# Item stats unification — canonical build-agnostic stat map

## Problem

Item stats are carried twice, at every layer of the game-data pipeline
(`bohpts-core` → `nx-gs-adapter-api` wire → `nx-gamedata` → `nx-wiki`):

- A block of **typed combat fields** (`pAtk`, `mAtk`, `pDef`, `mDef`,
  `attackSpeed`, `criticalRate`, `attackRange`, `randomDamage`, `soulshots`,
  `spiritshots`, `mpConsume`, …).
- An open **`statBonuses` map** (`Map<String,Double>`) that already includes the
  combat *stat-func* values under raw L2J `Stats` enum names.

Prod data confirms the duplication on `gd_item_templates`:

```
id=1: p_atk=8, m_atk=6, attack_speed=379
      stat_bonuses = {"POWER_ATTACK":8.0,"MAGIC_ATTACK":6.0,
                      "CRITICAL_RATE":8.0,"POWER_ATTACK_SPEED":379.0}
```

Two further problems:

1. **The map vocabulary is build-specific** — keys are raw L2J `Stats.name()`
   (`POWER_ATTACK`, `POWER_DEFENCE`, `MCRITICAL_RATE`, …). Not "our names", not
   build-agnostic, and never enumerated for the frontend.
2. **The map is not a reliable superset today** — 76 259 items carry `p_atk`
   but only 44 593 carry `stat_bonuses`. "Just drop the columns and trust the
   map" would lose data without a fix at the producing end.

The 31 distinct keys present in prod today:

```
ACCURACY_COMBAT, AUTOLOOT, CRITICAL_RATE, DARK_POWER, DARK_RES, EARTH_POWER,
EARTH_RES, EVASION_RATE, FIRE_POWER, FIRE_RES, HOLY_POWER, HOLY_RES, INV_LIM,
MAGIC_ATTACK, MAGIC_DEFENCE, MAGIC_SUCCESS_RES, MAX_HP, MAX_MP, MCRITICAL_RATE,
MOVE_SPEED, POWER_ATTACK, POWER_ATTACK_ANGLE, POWER_ATTACK_RANGE,
POWER_ATTACK_SPEED, POWER_DEFENCE, SHIELD_DEFENCE, SHIELD_RATE, WATER_POWER,
WATER_RES, WIND_POWER, WIND_RES
```

`randomDamage`, `soulshots`, `spiritshots`, `mpConsume` exist **only** as typed
fields today (no stat-func equivalent in the map) — they are read from direct
weapon accessors, not from stat functions.

## Goal

**One** canonical, build-agnostic stat map — the single home for every item
stat value, including the weapon mechanics that have no stat-func today. `combat`
disappears entirely with no replacement block. Normalization happens **at
source** (`bohpts-core`), so every downstream layer passes the map through
unchanged. The only item-stat datum that stays outside the map is `magicWeapon`
(a boolean — it can't live in a `Map<String,Double>` and already surfaces in
`flags`).

## Decisions

| Decision                                                                | Choice                                                                               |
|-------------------------------------------------------------------------|--------------------------------------------------------------------------------------|
| Where canonicalization happens                                          | At source — `bohpts-core` provider                                                   |
| Canonical vocabulary home                                               | New `ItemStat` enum in `nx-gs-adapter-api`                                           |
| Token style                                                             | Short L2-community shorthand (`P_ATK`, `M_ATK`, …)                                   |
| Weapon mechanics (`soulshots`/`spiritshots`/`mpConsume`/`randomDamage`) | Folded into the unified map (full uniformity — one place)                            |
| `attackRange`                                                           | Folded into the map as `ATK_RANGE`                                                   |
| `magicWeapon`                                                           | Stays a boolean in `flags` (not a `Double`)                                          |
| Map field name                                                          | Renamed `statBonuses` → `stats` end-to-end (it is now *all* stats, not only bonuses) |
| Unmapped L2J stat-funcs                                                 | Dropped + logged once — vocabulary stays closed                                      |
| Rollout                                                                 | Coordinated cutover (no transient dual-emit)                                         |

## Canonical vocabulary — `ItemStat` enum

New enum `app.l2nx.gs.adapter.api.domain.item.ItemStat` (Java 8 enum,
UPPER_SNAKE). It is the closed, build-agnostic stat vocabulary and the source of
truth for both production (at source) and the frontend `@Schema`. This
introduces a new `domain` package root in `nx-gs-adapter-api` (siblings:
`kafka`, `rest`, `spi`, `ops`) for shared domain vocabulary types that wire DTOs
reference but that are not themselves wire payloads — `ItemStat` is the first
resident.

The `Stats → ItemStat` translation is **build-specific knowledge** and lives in
`bohpts-core`, never in the enum or the wire.

### Token mapping (raw L2J source → canonical `ItemStat`)

Offense:

| Raw L2J              | Canonical     |
|----------------------|---------------|
| `POWER_ATTACK`       | `P_ATK`       |
| `MAGIC_ATTACK`       | `M_ATK`       |
| `POWER_ATTACK_SPEED` | `ATK_SPD`     |
| `CRITICAL_RATE`      | `CRIT_RATE`   |
| `MCRITICAL_RATE`     | `M_CRIT_RATE` |
| `ACCURACY_COMBAT`    | `ACCURACY`    |
| `POWER_ATTACK_RANGE` | `ATK_RANGE`   |
| `POWER_ATTACK_ANGLE` | `ATK_ANGLE`   |

Defense:

| Raw L2J             | Canonical       |
|---------------------|-----------------|
| `POWER_DEFENCE`     | `P_DEF`         |
| `MAGIC_DEFENCE`     | `M_DEF`         |
| `EVASION_RATE`      | `EVASION`       |
| `SHIELD_DEFENCE`    | `SHIELD_DEF`    |
| `SHIELD_RATE`       | `SHIELD_RATE`   |
| `MAGIC_SUCCESS_RES` | `M_SUCCESS_RES` |

Vitals & movement:

| Raw L2J      | Canonical |
|--------------|-----------|
| `MAX_HP`     | `MAX_HP`  |
| `MAX_MP`     | `MAX_MP`  |
| `MOVE_SPEED` | `SPEED`   |

Weapon mechanics (folded in — sourced from weapon accessors, not stat-funcs):

| Source (L2J `Weapon`)  | Canonical                                      |
|------------------------|------------------------------------------------|
| `getBaseAttackRange()` | `ATK_RANGE` (same token as the stat-func form) |
| `getRandomDamage()`    | `RANDOM_DAMAGE`                                |
| `getSoulShotCount()`   | `SOULSHOT_COUNT`                               |
| `getSpiritShotCount()` | `SPIRITSHOT_COUNT`                             |
| `getMpConsume()`       | `MP_CONSUME`                                   |

Attribute attack power:

| Raw L2J                                   | Canonical                                 |
|-------------------------------------------|-------------------------------------------|
| `{FIRE,WATER,WIND,EARTH,HOLY,DARK}_POWER` | `{FIRE,WATER,WIND,EARTH,HOLY,DARK}_POWER` |

Attribute resistance:

| Raw L2J                                 | Canonical                               |
|-----------------------------------------|-----------------------------------------|
| `{FIRE,WATER,WIND,EARTH,HOLY,DARK}_RES` | `{FIRE,WATER,WIND,EARTH,HOLY,DARK}_RES` |

Special / non-combat:

| Raw L2J    | Canonical   |
|------------|-------------|
| `AUTOLOOT` | `AUTOLOOT`  |
| `INV_LIM`  | `INV_LIMIT` |

Base stats (forward-looking — not in current prod data, but jewelry can grant
them; included so the closed vocabulary is complete):

```
STR, DEX, CON, INT, WIT, MEN
```

**Unmapped funcs**: a func whose `Stats` value has no entry in the
`bohpts-core` translation table is dropped from the map and logged once per
unknown stat. Adding support for a new stat = one `ItemStat` constant + one
mapping entry — the same discipline as every other enum-like vocabulary on the
platform.

## Wire contract change — `ItemStats` (breaking, `api/vX.Y.Z` bump)

`app.l2nx.gs.adapter.api.kafka.sync.gd.itemtemplate.ItemStats` collapses to a
thin container:

- **Removed** (all folded into the map): `pAtk`, `mAtk`, `pDef`, `mDef`,
  `attackSpeed`, `criticalRate`, `attackRange`, `randomDamage`, `soulshots`,
  `spiritshots`, `mpConsume`.
- `statBonuses` → **renamed `stats`**, keys switch from raw L2J names to
  canonical `ItemStat` tokens. Wire type stays `Map<String,Double>` (open
  string), consistent with the platform's enum-like-vocab convention: the enum
  is the source of truth for producing and documenting the values, not a hard
  wire-key type.
- **Kept**: `magicWeapon` (boolean → routes to `flags` downstream).

Resulting shape: `ItemStats { Boolean magicWeapon; Map<String,Double> stats }`.
`stats == null` for etc-items (no stat profile), as today.

Breaking change → `api/vX.Y.Z` tag bump; the only consumer (`nx-gamedata`) is
updated in lockstep.

## Source change — `bohpts-core`

`l2e.gameserver.l2nx.data.BohptsItemTemplateProvider`:

- New `Stats → ItemStat` translator (build-specific, lives here). Drives the
  unmapped-stat logging.
- `buildStats(Item)` → fold all stat-funcs through the translator into the
  canonical `stats` map; additionally fold the weapon accessors
  (`getBaseAttackRange` → `ATK_RANGE`, `getRandomDamage` → `RANDOM_DAMAGE`,
  `getSoulShotCount` → `SOULSHOT_COUNT`, `getSpiritShotCount` →
  `SPIRITSHOT_COUNT`, `getMpConsume` → `MP_CONSUME`) into the same map. Keep
  `magicWeapon`. Remove the six `funcStat(...)` typed extractions and the typed
  weapon-accessor block.
- Map iteration is first-wins per stat (existing behaviour); the explicit
  accessor folds run after, only adding keys the stat-func pass did not already
  produce.

Deploying the new `bohpts-core` triggers a full CRC snapshot re-sync that
repopulates the `stats` map canonically and completely — refreshing the
in-migration backfill (see `nx-gamedata` below) and filling the 76k-vs-44k gap
(items that had typed columns but no `stat_bonuses`).

## `nx-gamedata`

- Migration (`v1.9.6_item_stats_unification.sql`, forward-only, three changesets):
    1. **Rename** `stat_bonuses` → `stats`.
    2. **Backfill** `stats` in-place from the existing raw-keyed map + the typed
       combat columns, so the read path serves canonical stats immediately
       (without waiting on the bohpts-core re-sync). Raw L2J keys are translated
       to canonical tokens; the typed columns are folded in as canonical keys
       (`p_atk`→`P_ATK`, …, `soulshots`→`SOULSHOT_COUNT`, …); translated
       stat-func keys win over typed columns; typed `0` is dropped
       (`= not applicable`, matching the provider's `putMechanic`).
    3. **Drop** the 11 combat columns: `p_atk, m_atk, p_def, m_def, attack_speed,
       critical_rate, attack_range, random_damage, soulshots, spiritshots,
       mp_consume`. Keep `is_magic_weapon`.
- `ItemTemplate` domain record + `ItemTemplateKafkaMapper`: drop the 11 fields;
  map `getStats()` (the renamed map) into the `stats` field as received (already
  canonical). `magicWeapon` mapping unchanged.
- `ItemTemplateRepositoryAdapter`: drop the 11 columns; rename the JSONB
  column.
- **Ordering**: the in-migration backfill makes the schema self-sufficient — once
  it runs, `stats` is populated from the data already in the table, so reads work
  before the bohpts-core re-sync. The subsequent full re-sync from the
  canonical-emitting provider refreshes the values and fills any gaps. The
  breaking wire change still couples the adapter-api release, bohpts-core, and
  nx-gamedata code, so those deploy together (see Rollout); the migration runs as
  part of that nx-gamedata deploy.

> `stat_bonuses` is sync-owned (not in the patch-`COALESCE` set), so the rename
> is safe — no `bohpts-patch-ingester` writer depends on the old name.

## `nx-wiki`

- **Delete** `ItemCombatDto`. **Delete** the `ItemStatsDto` wrapper — collapse
  it so `ItemTemplateDetailedDto.stats` is a flat
  `@Nullable Map<String,Double> stats` directly (one map under `stats`, no
  nested `statBonuses`).
- `@Schema` on `stats` enumerates the canonical `ItemStat` keys (kept in sync
  with the enum) so the frontend builds its own translations.
- `ItemTemplate` domain record + `ItemTemplateRepositoryAdapter` SQL: drop the
  11 combat fields/columns, read the renamed `stats` JSONB; delete the
  `combat(...)` builder in `ItemTemplateQueryService`. No `weapon`/`combat`
  block is added.

`magicWeapon` continues to surface in `flags` (unchanged).

### Before / after (wiki detailed response)

```jsonc
// before
"combat": { "pAtk": 8, "mAtk": 6, "attackSpeed": 379, "soulshots": 1, ... },
"stats":  { "statBonuses": { "POWER_ATTACK": 8.0, "MAGIC_ATTACK": 6.0, ... } }

// after
"stats": { "P_ATK": 8.0, "M_ATK": 6.0, "ATK_SPD": 379.0,
           "SOULSHOT_COUNT": 1.0, "CRIT_RATE": 8.0, ... }
```

## Rollout (coordinated cutover)

The platform is co-developed and `gd.sync`'s only consumer is `nx-gamedata`, so
a coordinated cutover beats transient dual-emit:

1. `nx-gs-adapter-api` — add `ItemStat`, collapse `ItemStats`, release
   `api/vX.Y.Z`.
2. `bohpts-core` — adopt the enum + translator, emit the canonical `stats` map;
   deploy → full re-sync repopulates `stats`.
3. `nx-gamedata` — bump adapter-api dep, update record/mapper/adapter, run the
   rename+backfill+drop migration. The backfill populates `stats` from the
   existing columns/map at migration time, so the read path is correct the
   instant nx-gamedata is up — independent of when the bohpts-core re-sync lands.
4. `nx-wiki` — drop `ItemCombatDto` + `ItemStatsDto`, flat `stats` map,
   `@Schema`, drop combat columns from the read path.

The breaking wire change couples steps 1–3 (adapter-api → bohpts-core →
nx-gamedata deploy together). The in-migration backfill bridges the gap until the
bohpts-core full re-sync refreshes `stats` with freshly-computed canonical values.

## Frontend API changelog

⚠️ Breaking read-API change. New file
`nx-wiki/docs/specs/038-wiki-gamedata-read-path/api-changelog-006.md`:

- `combat` removed (no replacement block — folded into `stats`).
- `stats` flattened: was `stats.statBonuses{}`, now `stats{}` directly.
- `stats` keys renamed to canonical `ItemStat` tokens, and now also carry the
  former combat/weapon values (`P_ATK`, `ATK_SPD`, `SOULSHOT_COUNT`, …).
- Before/after JSON + the full enumerated `ItemStat` key list with descriptions
  for translation.

## Out of scope

- Armor-set stat bonuses (`ArmorSetStatBonus`, base-stat `STR/DEX/…`) are a
  separate slice. The `ItemStat` enum is item-template-scoped for now; reuse for
  armor-set bonuses can follow if desired.
- Localized display names / translations for the stat keys — owned by the
  frontend, driven off the enumerated `@Schema` list.
