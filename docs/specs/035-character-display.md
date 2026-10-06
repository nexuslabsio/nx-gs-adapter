# 035 — character display (appearance on the db channel)

**Date:** 2026-10-06
**Status:** implemented (api 0.94.0)
**Related specs:**

- nx-gameservers `docs/specs/083-character-display.md` — platform side: the `gs_character_display`
  table, ingest, the move of `sex` / `race` off `gs_characters`.
- Host integrations — each host's character entity mapping fills `display` from its own storage.

## Problem

The platform can name a character, its class and level, but cannot show what the character looks like.
A character preview (race × sex × face × hair style × hair color render) needs the appearance chosen at
character creation, and the name / title colors the player bought, none of which travel today.

`CharacterDbDto` carries `sex` and `race` flat next to non-visual fields; face, hair style, hair color and
colors are absent. Source data is persistent and complete on the host (character row + character
variables), so the gap is purely on the wire.

## Requirements

**Must:**

- [done] R1. The db channel MUST carry a character's appearance: race, sex, face, hair style, hair color.
- [done] R2. The db channel MUST carry the name color and title color the player set **persistently**
  (bought / chosen and stored by the host), not the color the host renders at a given moment.
- [done] R3. Colors MUST be expressed in one host-agnostic encoding: RGB `0xRRGGBB`.
- [done] R4. Every appearance field MUST be nullable: a host that cannot read a value sends `null`, and a
  character event without `display` (a host on an older api) stays valid — its appearance is simply not
  updated.

**Non-goals:**

- Colors the host overrides at render time (PvP / PK flags, nobility, offline-trader marker, access
  level) — they are derived state, not the character's appearance.
- Equipment, item skins, costumes, transformations — a full-look render is a separate feature;
  transformation is runtime-only on hosts.
- Runtime channel changes — appearance changes only through character creation, a paid service or an
  admin action, never per tick.

### Edge cases

- Value out of the creation range (corrupted row, custom tool) → the host sends `null` for that field,
  not a clamped value.
- A timed color whose expiry passed but the host has not purged the variable yet → treated as absent.
- Character without a stored color → `null` (= the default color); the wire never carries the host's
  default as if it were chosen.

## Technical design

### Overview

A nested `CharacterDisplayDbDto` on `CharacterDbDto.display`, filled by the host's character entity
mapping. Flat `CharacterDbDto.sex` / `race` move into it in the same release (breaking, minor bump on the
0.x line).

### Key components

- `kafka/sync/db/character/CharacterDisplayDbDto` — appearance value object, all fields
  `@Nullable`: `race` (`CharacterRace`), `sex` (`CharacterSex`), `face`, `hairStyle`, `hairColor`
  (`Integer`, host creation indices), `nameColor`, `titleColor` (`Integer`, RGB `0xRRGGBB`). Builder +
  positional constructor, `equals` / `hashCode`, same shape rules as `CharacterClassDbDto`.
- `CharacterDbDto.display` — `@Nullable CharacterDisplayDbDto`.
- `CharacterDbDto.sex` / `race` — removed; their values live in `display`.

### Decisions

- **Nested object on the existing character event, not a separate entity / topic.** The character row
  and its appearance arrive atomically, so the platform never sees appearance for a character it has
  not stored yet (no FK race). Appearance changes rarely, so re-sending it with the character costs
  nothing. A separate topic would also need a name that is not a string prefix of / prefixed by
  `…sync.db.character` (topic canon, nx-infra `docs/specs/001-kafka-clusters.md` §9).
- **`sex` / `race` move into `display`.** They are appearance; keeping them flat would split one concept
  across two places on the wire and on the platform. Moved in one release, without the usual
  two-release `@Deprecated` straddle: the owner prefers a short gap over a long-lived dual shape. The
  gap is bounded and self-healing — a host still on the older api keeps sending flat `sex` / `race`,
  which the new platform ignores, so only characters created or changed in that window miss their
  appearance until the host restarts on the new api and db-sync re-sends the rows whose hash changed.
- **Face / hair indices stay raw integers.** They are client asset indices (0-based, as chosen at
  creation); there is no universal enum across protocols. Their meaning is the preview asset key.
- **RGB, not the host encoding.** L2 hosts store colors as BGR ints; the host mapping converts. The
  wire stays protocol-neutral and directly usable by web consumers.
- **Only persistent colors.** What the client renders is `persistent color` overridden by host rules
  that live in host config; mirroring those rules on the platform would be fork knowledge.

## Rollout

One release on every side; the platform goes first.

1. api `0.94.0`: `CharacterDisplayDbDto`, `CharacterDbDto.display`; flat `sex` / `race` removed.
2. nx-gameservers on the new api: reads `display` only, moves stored `sex` / `race` into its display
   table in the same deploy. Events from hosts on the older api carry no `display` and update no
   appearance.
3. Hosts bump the api and fill `display`; on restart db-sync re-sends characters, because the appearance
   columns join the row hash.

## Open questions

- [assumed: 0-based creation indices are the same across supported protocols for face / hair] —
  confirmed for the first integrated host; revisit when a host with a different client protocol joins.

## Links

- nx-gameservers `docs/specs/083-character-display.md`
