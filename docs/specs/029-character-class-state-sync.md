# 029 — character class state sync (per-class roster, active class on runtime)

**Date:** 2026-07-27
**Status:** design (not implemented)
**Related specs:**

- nx-gameservers `docs/specs/062-character-class-state.md` — platform side: the
  `gs_character_classes` table, the active-class projection on `gs_characters`, the REST contract.
- nx-telegram `docs/specs/031-character-classes-via-gameservers.md` — bot moves off direct DB reads.
- nx-cube `docs/specs/004-character-classes-cube/spec.md` — cube rename.

## Problem

The wire carries a character's class state only partially, and asymmetrically.

`CharacterDbDto` surfaces `level` (base class) flat on the character, plus a `subclasses` list of
`(classId, level)`. Neither `exp` nor `sp` is modeled for any class. `CharacterRuntimeDto` carries `exp`
but no class identity, so a consumer receiving it cannot tell which class that experience belongs to —
it silently belongs to the **active** class, which may be a subclass.

Two consequences:

- A consumer cannot answer "what is the level and experience of the class this character is currently
  playing", and cannot show a per-class roster at all.
- The main class and the subclasses arrive in different shapes, so assembling a uniform roster is left
  to every consumer — and the assembly rule is build-specific (see below), which is exactly the kind of
  fork knowledge the wire exists to absorb.

Source data is complete — the gap is in what the adapter reads and how it shapes it:

- `characters.level` / `.exp` / `.sp` — base class (`getStat().getBaseLevel/BaseExp/BaseSp`).
- `characters.classid` — active class; `characters.base_class` — base class.
- `character_subclasses (charId, class_id, exp, sp, level, class_index)` — one row per subclass.

## Wire shape: one roster, not "base + subclasses"

`CharacterSubclassDbDto` is replaced by `CharacterClassDbDto`, and `CharacterDbDto.getSubclasses()` by
`getClasses()` returning the **full** roster including the main class:

```java
public final class CharacterClassDbDto {
    private final CharacterClass classId;        // NOT NULL on the wire
    private final CharacterClassKind kind;       // NOT NULL on the wire — MAIN | SUB
    private final @Nullable Integer level;
    private final @Nullable Long exp;
    private final @Nullable Long sp;
}
```

```java
// CharacterDbDto
- @Nullable List<CharacterSubclassDbDto> getSubclasses()
+ @Nullable List<CharacterClassDbDto>    getClasses()
```

`EntityMapping.mapEntity` already receives both the primary row and the child rows, so the schema
provider assembles the roster itself: the `MAIN` entry from `characters.{base_class, level, exp, sp}`,
the `SUB` entries from `character_subclasses`.

**Why the roster and not flat base fields + a subclass list:**

- **Fork normalization belongs to the adapter.** Classic L2J keeps only subclasses in
  `character_subclasses` (`class_index > 0`), but forks exist that also store the base class there as
  `class_index = 0`. Under the flat-plus-list shape such a fork emits the main class twice — once flat,
  once in the list — and every consumer has to de-duplicate it. Under the roster shape the provider
  collapses it and the wire stays build-agnostic.
- **Dual class extends by enum value, not by contract change.** A later chronicle's dual class is
  another `CharacterClassKind`; the provider decides how its source represents it, and no consumer is
  touched.
- The platform table becomes a 1:1 mirror of the wire — the ingestor writes what arrived instead of
  re-deriving a roster.

`exp` / `sp` are **not** added flat on `CharacterDbDto` — they exist only inside roster entries, so no
new redundancy is introduced. The pre-existing flat `level` (base class, hashed) stays as-is: it is
consumed today and breaking it buys nothing. It overlaps with the `MAIN` roster entry's `level` by
construction.

`active` is deliberately **not** on the roster entry. The active class is already unambiguously given by
`CharacterDbDto.classId`; a second representation would be a second source of truth that can disagree
with the first.

`class_index` stays unread — the platform addresses a class by its `CharacterClass` token, not by the
engine's slot index.

### New enum

```java
package app.l2nx.gs.adapter.api.domain.character.clazz;

/**
 * Which class slot a character's class occupies — the single {@code MAIN} class every
 * character has, versus an additional {@code SUB} class. Schema providers normalize
 * their build's storage (base class on the character row, subclasses in a side table,
 * or a side table that also holds the base row) into this enum.
 */
public enum CharacterClassKind {
    MAIN,
    SUB
}
```

Sits next to `CharacterClass` / `CharacterClassTier` / `CharacterClassType`. Note the existing
`CharacterClassType` is the unrelated FIGHTER/MYSTIC division (carried by the game-data `ClassTemplate`
wire) — hence the distinct name.

## db-sync: exp/sp as unhashed ride-alongs

`exp` and `sp` are read in `mapRow` on both sources **without touching `hashedColumns()`**:

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

Javadoc on `CharacterClassDbDto.getExp()` / `.getSp()` must state that the value is an unhashed
ride-along whose freshness is bounded by the source's store cadence — otherwise the next reader assumes
it is CDC-fresh and re-derives the wrong conclusion. The current `CharacterSubclassDbDto` class-level
Javadoc says `exp`/`sp` are "intentionally not modeled — they tick on every kill and would generate an
UPDATE storm per cycle"; that paragraph is replaced, since the columns are now modeled but deliberately
kept out of the hash, which is what actually prevents the storm.

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

With `classId` on the wire the platform routes a runtime tick to the right per-class row instead of
guessing from the last db-sync snapshot, which is what makes a class switch visible immediately.

## Schema provider (bohpts-core)

- `CharacterPrimarySource.mapRow` — read `exp` / `sp` (`JdbcNulls.nullableLong`); `HASHED` unchanged.
- `CharacterSubclassesChildSource.mapRow` — read `exp` / `sp`; `HASHED` stays `("class_id", "level")`.
- `CharacterMapping.mapEntity` — assemble the roster: `MAIN` entry from the primary row's
  `base_class` / `level` / `exp` / `sp`, one `SUB` entry per `character_subclasses` row. A character
  whose `base_class` does not resolve to a canonical `CharacterClass` gets no `MAIN` entry (same
  drop-with-WARN rule that already applies to subclass rows).
- `CharacterRuntimeMapping.toDto` — add `classId` (via `BohptsCharacterClasses.fromClassId`), `level`,
  `sp`; `hash(...)` mixes the three new fields.

bohpts stores only subclasses in `character_subclasses`, so no de-duplication against the `MAIN` entry
is needed there — but the roster contract is what lets a future tenant that stores `class_index = 0`
handle it locally.

## Compatibility

Runtime channel: purely additive, every new field nullable.

db-sync channel: the JSON field `subclasses` becomes `classes`. The platform is deployed **before** the
schema provider, so in that window it receives events in the old shape and would see no roster at all.
Hence the transition release is additive rather than a swap:

- `CharacterDbDto` carries **both** `getClasses()` and a `@Deprecated getSubclasses()`, and
  `CharacterSubclassDbDto` stays in place (deprecated). Dropping the old field in the same release would
  make the new platform blind to the old adapter's payload for the whole window.
- Deprecation Javadoc states the removal gate explicitly: the field goes once every schema provider
  emits `classes` (for bohpts, the morning game-server restart that ships the new adapter).
- The platform mirrors this with a legacy ingest branch that reconstructs the roster from
  `baseClassId` + flat `level` + `subclasses` — see the nx-gameservers spec, section «Совместимость и
  cutover».

**Follow-up release (not this one):** remove `getSubclasses()` and `CharacterSubclassDbDto` from
adapter-api once the cutover is done. That one IS breaking and takes its own version bump.

The transition release itself is a minor version bump — every change in it is additive. Deploy ordering
is in the nx-gameservers spec: platform, then nx-telegram, then adapter-api to Maven Central, then
bohpts-core, then force-resync.

## Tests

- `CharacterDbDtoTest` — `classes` round-trips through builder / `toBuilder` / `equals` / `hashCode` /
  `toString`; the list stays unmodifiable.
- `CharacterClassDbDtoTest` (renamed from `CharacterSubclassDbDtoTest`) — same, plus `kind` is required.
- `CharacterRuntimeDto` test — new fields round-trip; `hash(...)` changes when `classId` / `level` / `sp`
  change.
- Schema-provider tests — the assembled roster carries exactly one `MAIN` entry plus one entry per
  subclass row; `hashedColumns()` is unchanged on both sources (regression guard: adding a column there
  silently invalidates every tenant's snapshot).
