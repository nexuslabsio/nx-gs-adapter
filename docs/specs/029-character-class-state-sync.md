# 029 — character class state sync (exp/sp per class, active class on runtime)

**Date:** 2026-07-27
**Status:** design (not implemented)
**Related specs:**

- nx-gameservers `docs/specs/062-character-class-state.md` — platform side: the
  `gs_character_classes` table, the active-class projection on `gs_characters`, the REST contract.
- nx-telegram `docs/specs/031-character-classes-via-gameservers.md` — bot moves off direct DB reads.
- nx-cube `docs/specs/004-character-classes-cube.md` — cube rename.

## Problem

The wire carries a character's class state only partially. `CharacterDbDto` surfaces `level` (base
class) and a `subclasses` list of `(classId, level)`; neither `exp` nor `sp` is modeled for any class.
`CharacterRuntimeDto` carries `exp` but no class identity, so a consumer receiving it cannot tell which
class that experience belongs to — it silently belongs to the **active** class, which may be a subclass.

Platform consumers therefore cannot answer "what is the level and experience of the class this character
is currently playing", and they cannot show a per-class roster at all.

Source data is complete — the gap is purely in what the adapter reads and puts on the wire:

- `characters.level` / `.exp` / `.sp` — base class (`getStat().getBaseLevel/BaseExp/BaseSp`).
- `characters.classid` — active class; `characters.base_class` — base class.
- `character_subclasses (charId, class_id, exp, sp, level, class_index)` — one row per subclass.

## db-sync: exp/sp as unhashed ride-alongs

Add `exp` and `sp` to the DTOs and read them in `mapRow`, **without touching `hashedColumns()`**:

| source                                | new columns read in `mapRow` | added to hash |
| ------------------------------------- | ---------------------------- | ------------- |
| `characters` (primary source)         | `exp`, `sp` — base class     | no            |
| `character_subclasses` (child source) | `exp`, `sp`                  | no            |

Phase 2 of the engine issues `SELECT * FROM <table> WHERE <fk> IN (...)`, so `mapRow` can read any
column regardless of whether it participates in the Phase 1 CRC. Two consequences follow:

- **No new traffic.** `onlinetime` and `gear_score` are already hashed, so an online character already
  emits a CDC event on every full store (logout + periodic autosave); `exp`/`sp` ride that event.
  Freshness is therefore "as of last autosave" — minutes, not ticks.
- **No snapshot invalidation.** `hashedColumns()` is unchanged, so the CRC snapshot survives the deploy
  and no full resync storm happens. The flip side: an offline character whose hash never changes will
  never receive `exp`/`sp` on its own. A one-off force-resync of the `character` entity after rollout
  fills them in (platform-side deploy step, see the nx-gameservers spec).

Adding `exp` to the hash is not an option: it ticks on every kill, which is exactly the per-tick UPDATE
storm the adapter avoids by not modeling volatile columns.

`class_index` stays unread — the platform addresses a class by its `CharacterClass` token, not by the
engine's slot index.

### Wire changes

`CharacterSubclassDbDto` — additive, two nullable getters:

```java
@Nullable Long getExp();   // subclass experience, source `exp`
@Nullable Long getSp();    // subclass SP, source `sp`
```

`CharacterDbDto` — additive, two nullable getters carrying the **base class** figures:

```java
@Nullable Long getExp();   // base-class experience, source `characters.exp`
@Nullable Long getSp();    // base-class SP, source `characters.sp`
```

Both are `Long` on the wire even where a build stores `INT` — forks with BIGINT `sp` exist, and widening
the wire type later would be the breaking change.

Javadoc on all four getters must state (a) which class the figure belongs to and (b) that the value is
an unhashed ride-along whose freshness is bounded by the source's store cadence — otherwise the next
reader will assume it is CDC-fresh and re-derive the wrong conclusion.

The class-level Javadoc of `CharacterSubclassDbDto` currently says `exp`/`sp` are "intentionally not
modeled — they tick on every kill and would generate an UPDATE storm per cycle". That paragraph is
replaced: the columns are modeled but deliberately kept out of the hash, which is what actually
prevents the storm.

## runtime-sync: active class identity

`CharacterRuntimeDto` gains three fields, all hashed:

```java
@Nullable CharacterClass getClassId();  // active class — p.getClassId()
@Nullable Integer getLevel();           // active class level — p.getStat().getLevel()
@Nullable Long getSp();                 // active class SP — p.getStat().getSp()
```

`exp` already exists and already means the active class (`p.getStat().getExp()`); its Javadoc gets that
stated explicitly instead of left implicit.

Hashing `classId` / `level` / `sp` costs nothing: `exp` is already in the tick hash and changes far more
often than any of them, so no additional tick is generated in practice.

With `classId` on the wire the platform can route a runtime tick to the right per-class row instead of
guessing from the last db-sync snapshot, which is what makes a class switch visible immediately.

## Schema provider (bohpts-core)

- `CharacterPrimarySource.mapRow` — read `exp` / `sp` (`JdbcNulls.nullableLong`); `HASHED` unchanged.
- `CharacterSubclassesChildSource.mapRow` — read `exp` / `sp`; `HASHED` stays `("class_id", "level")`.
- `CharacterMapping.mapEntity` — pass both through to the builders.
- `CharacterRuntimeMapping.toDto` — add `classId` (via `BohptsCharacterClasses.fromClassId`), `level`,
  `sp`; `hash(...)` mixes the three new fields.

A subclass row whose `class_id` does not resolve to a canonical `CharacterClass` is still dropped before
assembly — unchanged.

## Compatibility

Purely additive on both channels: every new field is nullable, and an older platform consumer ignores
them. An older adapter against a newer platform simply leaves the new columns NULL. Adapter-api takes a
minor version bump; no coordinated cutover is required beyond the deploy ordering in the nx-gameservers
spec (platform first, then adapter-api to Maven Central, then bohpts-core, then force-resync).

## Tests

- `CharacterDbDtoTest` / `CharacterSubclassDbDtoTest` — new fields round-trip through builder /
  `toBuilder` / `equals` / `hashCode` / `toString`.
- `CharacterRuntimeDto` test — same, plus `hash(...)` changes when `classId` / `level` / `sp` change.
- Schema-provider tests — `mapRow` reads the new columns; `hashedColumns()` is unchanged (regression
  guard: adding a column there silently invalidates every tenant's snapshot).
