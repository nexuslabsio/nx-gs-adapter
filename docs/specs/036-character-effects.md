# 036 — character effects (buffs and debuffs on the runtime channel)

**Date:** 2026-10-08
**Status:** implemented
**Related specs:**

- `nx-gameservers/docs/specs/009-characters.md` — platform side: ingest, storage, read API and the
  character screen that renders the effect bar.
- [`006-runtime-sync.md`](006-runtime-sync.md) — the runtime channel this rides on.

## Problem

A character screen on the platform shows vitals, position and inventory capacity, but not the effects
(buffs, songs, debuffs) the character carries — the first thing a player checks to judge another
player's state. Effects are volatile and engine-owned (durations tick), so db-sync cannot carry them;
they are runtime state like vitals.

Effects are shipped as a **field of `CharacterRuntimeDto`**, not as a separate runtime entity. They
have no identity of their own (a character's effect list is replaced as a whole), and a separate
entity would need its own cadence, hash, tombstone and consumer path for data that always travels
with the character row.

## Requirements

**Must:**

- [done] R1. `CharacterRuntimeDto` MUST carry `@Nullable List<CharacterEffect> effects`: the effects
  shown in the character's abnormal-status bar at snapshot time.
  - `null` on an offline tombstone = no state; the consumer applies its logout rules to what it stored
    (`offline`, R2).
  - `null` on a state-carrying row = the host does not report effects (older host). The consumer holds
    no current knowledge and MUST drop what it stored rather than show effects that no longer count
    down.
  - empty list = the character has no effects. A consumer MUST clear its stored effects.
- [done] R2. `CharacterEffect` MUST carry `int skillId`, `int skillLevel`, `SkillEffectCategory
category`, `@Nullable Integer remainingSec`, `CharacterEffectOffline offline`.
  - `(skillId, skillLevel, enchant)` is the key of one gd-sync skill record (a base level, or one step
    of an enchant route — `SkillLevel` / `SkillEnchantRoute`), so the platform joins the effect to the
    catalog (icon, name, description) without translation.
  - `remainingSec` is whole seconds left at snapshot time. `null` = the effect has no duration
    (toggle, aura). A host-internal "infinite" magic value (`-1`) MUST NOT reach the wire.
- [done] R3. List order MUST be the client's status-bar order: plain buffs by application time, then
  toggles, then songs/dances, then triggered, then debuffs. The position is the list index; there is
  no separate position field, so a consumer cannot get the two out of sync.
- [done] R4. The list MUST contain only effects the bar shows: icon displayed, not instant, not a
  ground signet, actually in use, not a healing-potion short buff. Passive skills are excluded.
- [done] R5. Tombstones (`online=false` with a `null` state) MUST carry `effects=null`. Offline-trader
  ticks (`online=false` with a real state) carry the real effects.
- [done] R6. The host SHOULD exclude `remainingSec` from the runtime hash and include `skillId`,
  `skillLevel`, `enchant`, `category`, `offline` and the list position. A hash that included `remainingSec` would
  republish every character with a timed buff on every tick. A re-cast or an in-place refresh of the same buff
  (a fresh timer, same ids, same position) is signalled through `RuntimeRow.stateStamp` (api 0.97.0,
  runtime-sync 0.4.0): the host derives it from a per-effect timer generation it bumps on every timer
  reset, the engine folds it into the
  change hash, and it never reaches the wire. A host-side application timestamp is not a safe stamp
  when the host resets it on every periodic tick of the effect.
- [done] R7. `CharacterEffect` MUST carry `@Nullable SkillEnchant enchant` (api 0.98.0), and `skillLevel` MUST
  be the base level of the record.
  - `SkillEnchant { int route, int level }`: the enchant route number and the step within it, both
    `> 0` — the same coordinates the gd-sync `SkillEnchantRoute` carries (`route`, `enchantLevel`). The
    enchanted record's base level is the skill's max base level (`SkillEnchantRoute.baseLevel`).
  - `null` = the effect's skill is not enchanted. No `0` / `0` sentinel.
  - A host whose engine encodes the enchant into one level number MUST decode it before the wire, with
    the same rule its gd-sync provider uses, so both channels key the same record. The encoding never
    reaches the platform.

**Non-goals:**

- Passive skills, instant skills, ground signets and short healing-potion buffs — not in the bar.
- Effect source (who cast it) and effect stats — the platform resolves the description from the
  skill catalog; the host ships only which skill at which level is active.
- Per-tick `remainingSec` precision. The value is a snapshot used to render "about N min left"; the
  consumer extrapolates between snapshots if it wants a ticking display.

### Edge cases

- A re-cast of the same buff resets the timer and keeps `skillId` / `skillLevel`: without the `stateStamp` (R6) the platform would keep showing the stale `remainingSec`.
- A character with only untimed effects (toggles) never changes `remainingSec`, so it is published
  only when the list actually changes.
- An enchanted buff whose enchant step is missing from the gd-sync catalog keeps its ids on the wire;
  the platform renders the bare ids rather than substituting the base record, whose description would
  understate the effect.

## Technical design

### Overview

Three new wire types and one new field. `SkillEffectCategory` is a static property of a skill, so it
lives with the skill vocabulary; `CharacterEffect` and `CharacterEffectOffline` are runtime-channel
types that live with the character model; `CharacterRuntimeDto` gains the list.

### Structure

- `app.l2nx.gs.adapter.api.domain.skill.SkillEffectCategory` — next to `SkillOperation`.
- `app.l2nx.gs.adapter.api.kafka.sync.runtime.character.model.CharacterEffect` and
  `CharacterEffectOffline` — next to `Activity`.
- `app.l2nx.gs.adapter.api.kafka.sync.runtime.character.CharacterRuntimeDto` — `effects` appended at
  the END of the canonical constructor (field, getter, builder, `toBuilder`, `equals` / `hashCode` /
  `toString`).

### Key components

- `SkillEffectCategory { BUFF, DEBUFF, SONG_DANCE, TOGGLE, TRIGGERED }` — closed enum.
  - `BUFF` — plain buffs, herbs, item and augmentation buffs, transformations, event-wide blessings.
  - `DEBUFF` — harmful effects.
  - `SONG_DANCE` — songs and dances. Hosts typically cannot tell the two apart, so they are one value.
  - `TOGGLE` — on/off skills with no duration.
  - `TRIGGERED` — effects granted by a triggered (chance / on-event) skill.
- `SkillEnchant { int route, int level }` — `app.l2nx.gs.adapter.api.domain.skill`, next to
  `SkillEffectCategory`: an enchant coordinate is a property of the skill record, reusable wherever a
  skill reference crosses the wire later.
- `CharacterEffectOffline { FROZEN, TICKING, DROPPED }` — what happens to the effect when the
  character logs out, so the platform can tell a player whether a buff survives logout:
  - `FROZEN` — persisted on logout; the timer stops while offline and resumes on login.
  - `TICKING` — persisted with an absolute end time; keeps running offline and may expire. The host
    configures which skills behave this way.
  - `DROPPED` — not persisted on logout (toggles, songs/dances when the host does not store dances,
    heal-over-time, cancel, signets, global skills, ...).

### Decisions

- **`category` is on the effect, and the enum is in `domain.skill`.** It is a static property of the
  skill, not of one application. Placing it beside `SkillOperation` lets it later become a field of
  the gd-sync `Skill` (gamedata, wiki) without a rename or a package move; until then each effect
  carries it so the platform does not wait on the catalog.
- **Host derives the category with a fixed precedence:** debuff, then triggered, then dance, then
  toggle, else `BUFF`. The same precedence decides the placement order (R3), so category and list
  position never disagree. The rule is host guidance, not wire: the contract only names the values.
- **Separate `CharacterEffectOffline` enum instead of a boolean "persisted".** Three real behaviours
  exist and the platform renders them differently ("paused while offline" vs "keeps running"); a
  boolean would drop a distinction the host already has.
- **`null` list vs empty list.** `null` = "no signal" and empty = "no effects" are different facts, but
  on a live tick both leave the consumer with nothing to show: a stored set is only correct while the
  host keeps refreshing it, so a host that stops reporting (e.g. rolled back to an older build) must not
  leave frozen-in-time effects behind. On a tombstone `null` is just "no state" and the stored set goes
  through the logout rules.
- **No position field.** List order is the position; a second field would be a second source of truth.
- **The host decodes the enchant, not the platform (R7).** How an engine numbers enchanted levels is
  core-specific; a platform that knew one core's encoding would misread the next core. gd-sync already
  ships the decoded coordinates, so the runtime channel matches it and the platform joins one key.
- **Rejected: `enchant` as two nullable ints on `CharacterEffect`.** They are meaningful only together;
  one nullable value object makes "both or neither" structural.
- **Rejected: a separate `character_effects` runtime entity.** See Problem — extra entity, cadence,
  hash and tombstone for data that is always read with the character.

### Integration points

- Platform consumer: the runtime-character ingest stores the list as a unit (replace on a live tick,
  `null` on a live tick clears; tombstone applies the `offline` rules).
- Host provider: fills `effects` on the runtime snapshot thread from the engine's own effect list,
  O(effects) per character per tick.

## Rollout

Purely additive: a new nullable constructor parameter and three new types, api minor bump. Older
consumers ignore the unknown field (the wire is unknown-field tolerant, like every other additive
runtime field); older hosts omit it and consumers read `null` (R1). Deploy order is the usual one:
platform consumer first, then the api release, then the host. No deprecation gate — nothing is renamed
or removed.

**Enchant (R7, api 0.98.0).** Additive: `enchant` is appended at the end of the constructor and
`skillLevel` keeps its name. An older host keeps sending its engine-encoded level with `enchant` absent;
the platform then resolves the nearest base record, exactly as before the change, until the host
restarts on a build that decodes. No field is renamed or removed, so no deprecation gate.

**Constructor rule.** The new parameter is appended at the end of the canonical constructor; no
overload keeping the old signature (see [`028`](028-character-inventory-capacity.md) — two visible
constructors break parameter-name binding for every consumer). Positional call sites add one `null`;
hosts use `builder()`.

## Links

- Runtime channel: [`006-runtime-sync.md`](006-runtime-sync.md)
- Sibling runtime-field slice: [`028-character-inventory-capacity.md`](028-character-inventory-capacity.md)
- Counterpart (cross-repo): `nx-gameservers/docs/specs/009-characters.md`
