# Class / race / type canonicalization

- Date: 2026-06-13
- Status: **implemented** (full migration per D5; D4 = global seeded `gd_class_names`)
- Primary repo: `nx-gs-adapter` (contract). Cross-repo impact: `bohpts-core`, `nx-gamedata`, `nx-wiki`.

## 1. Goal

Make the playable-class vocabulary (class, race, fighter/mystic) **canonical and
source-agnostic** in `nx-gs-adapter-api`, and keep the adapter free of any
host/source-specific detail or human display text. **The gs contract carries only enum
values; display names and i18n live consumer-side** (nx-gamedata / nx-wiki — DB or config).

## 2. Guiding principle (per review direction)

- `nx-gs-adapter-api` = closed canonical enums only. No numeric source ids baked into the
  enum, no display strings, no localization.
- The source-id → enum translation is a **host detail** → lives in the host provider
  (bohpts), not in the shared contract.
- Display names (EN) and translations (RU/UK) are **platform data** → live in
  nx-gamedata / nx-wiki (DB table or config bundle), keyed by the canonical enum token.

## 3. Current state

- ✅ Already done: race / type / tier are canonical enums on `ClassTemplate` — `race` →
  `CharacterRace`, `type` → `CharacterClassType {FIGHTER, MYSTIC}`, `tier` →
  `CharacterClassTier {BASE, FIRST, SECOND, THIRD}` (renamed from `classType`/`classLevel`; tier
  was an `Integer 0..3`). Wire serializes by name; nx-gamedata stores tokens (VARCHAR columns
  `type`/`tier`); nx-wiki DTOs use the enums (Swagger derives `allowableValues`).
- `CharacterClass` enum exists in `domain/character` — 103 constants, **each carrying a
  numeric `id` + `byId(int)` + `getId()`**. Used by:
    - `CharacterDbDto` / `CharacterSubclassDbDto` (character db-sync wire) — as a **field type**
      (`classId` / `baseClassId`), serialized by name.
    - bohpts `CharacterMapping` — maps source DB `classid`/`base_class`/subclass `class_id` →
      enum via `CharacterClass.byId(int)` (the **only** functional `byId` caller).
    - `CharacterClassTest` (asserts id uniqueness + `byId` round-trip).
    - Olympiad events carry a **raw `int classId`** (not the enum) — unaffected.
- **Token spelling is already canonical** (verified against the fandom Classes page:
  `SORCERER`, `SWORDSINGER`, `BLADEDANCER`, `SPELLHOWLER`, `EVA_TEMPLAR`→"Eva's Templar",
  etc.). The "grammatical errors" the wiki shows come from the host's runtime `getName("en")`
  client strings (e.g. host `ClassId` is literally misspelled `sorceror`, `shillenElder`),
  **not** from this enum. → No constant renames needed; the fix is to stop using the client
  strings and resolve names from the canonical token consumer-side.

## 4. Decisions

### D1 — `CharacterClass` becomes id-less

Remove `id`, `byId(int)`, `getId()`, and the `BY_ID` table. Constants stay (tokens already
canonical). The enum is now a pure agnostic vocabulary.

### D2 — source-id → enum mapping moves to the host

Because `byId` is gone, the numeric-id → `CharacterClass` translation lives in **bohpts**
(the source detail belongs to the host). One shared helper, e.g.
`l2e.gameserver.l2nx.data.BohptsCharacterClasses.fromClassId(ClassId)` — a `switch` over the
host `ClassId` enum returning the matching `CharacterClass` (103 cases; dummy/unknown → null).
Reused by:

- `CharacterMapping` (character-sync) — replaces `CharacterClass.byId(code)`
  (`ClassId.getClassId(code)` → `fromClassId`).
- `BohptsClassTemplateProvider` (this feature) — `fromClassId(cid)`.

### D3 — class template is token-keyed, no numeric id

`ClassTemplate` (gd wire) carries canonical `CharacterClass` values only — no source ints. The
free-text English `name` is removed (it was the misspelled client string).

```
ClassTemplate {
  CharacterClass clazz;          // canonical class identity (always present for a playable class — §7.3)
  CharacterClass parentClazz;    // parent class it advances from (null for a base class)
  CharacterRace race;            // canonical
  CharacterClassType type;       // FIGHTER / MYSTIC
  CharacterClassTier tier;       // BASE / FIRST / SECOND / THIRD
}
```

- No `int id` / `parentClassId` (§7.2). Storage is token-keyed: `gd_class_templates` PK
  `(tenant_id, server_id, clazz)`, `parent_clazz` nullable. `clazz` =
  `CharacterClass.name()`.
- gd-sync envelope `pk` is a framework `long`; use `clazz.ordinal()` as the synthetic, diagnostic
  pk (the consumer keys by the payload token, not by `pk`; reconciliation keys by `sync_id`).
- `clazz` is also the key for display-name / i18n resolution downstream (D4).

### D5 — class→skills join is token-based; column naming synchronized to `clazz`

The classes page joins a class to its skills, so both gd_* tables must name the token column the
same. We standardize on **`clazz`** (the value is a canonical machine token; the human name
lives separately in `gd_class_names.name`, so `clazz` ≠ `name` is the clearer pairing — the
existing `class_name` is a misnomer for a token).

- `gd_class_templates.clazz` (new) and `gd_skill_template_classes.clazz` (**renamed**
  from `class_name`) — join `gd_class_templates.clazz = gd_skill_template_classes.clazz`.
- `SkillClassLearn` (skill wire) carries `CharacterClass clazz` instead of `int classId` +
  `String className` — same enum-only principle as `ClassTemplate.clazz`. The skill provider fills
  it via the shared `fromClassId(cid)` (was the misspelled `toUpperSnake(ClassId.name())` →
  `SORCEROR`; now canonical `SORCERER`).
- `gd_skill_template_classes`: drop `class_id` (int) + `class_name`; single `clazz` token
  column; PK `(tenant_id, server_id, skill_template_id, clazz)`. **No numeric class id
  remains anywhere.**
- `fromClassId` is shared by **three** host providers: `CharacterMapping` (character-sync),
  `BohptsClassTemplateProvider`, `BohptsSkillTemplateProvider`.

**Lighter alternative** (smaller skill-side scope): rename only the DB column
`gd_skill_template_classes.class_name → clazz` and canonicalize its value, but keep
`SkillClassLearn.classId`/`className` on the skill wire untouched. Columns then match; the wire
doesn't change. Recommendation: do the **full** enum migration — it applies the same principle you
chose for the class template and removes the last numeric class id; the lighter option leaves the
int `class_id` + `String className` redundancy on the skill stream.

### D4 — display names + translations live in gd_* (nx-gamedata) ✅ decided

The adapter sends no names — only `clazz`. Class translations are stored in **gd_*
(nx-gamedata)**, keyed by the canonical token.

Proposed shape (confirm in review):

- A **global** lookup table `gd_class_names(clazz PK, name JSONB)` — `name` is a
  `LocalizedText` (ru/en/uk). **Not** tenant/server-scoped: canonical class names are universal,
  so one seeded row per class is enough (no per-server duplication).
- **Seeded via Liquibase** from the canonical fandom names (EN + RU/UK), keyed by the
  `CharacterClass` token. Static set (~103) — a seed, not a sync target; the adapter never writes
  it.
- `gd_class_templates` stores `clazz` (the token). nx-wiki resolves the display name by
  joining `gd_class_templates.clazz → gd_class_names.name`, returning `LocalizedText` (same
  shape as item/skill names; the frontend picks the locale).

Later, if per-tenant class renaming is ever needed, add a tenant/server-scoped override on top of
the global seed (same pattern as item/skill localization overrides). Not now.

## 5. Affected components

| Repo                     | Change                                                                                                                                                                                                                                                             |
|--------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `nx-gs-adapter-api`      | `CharacterClass`: drop `id`/`byId`/`getId`/`BY_ID`. `ClassTemplate`: `clazz: CharacterClass`, drop `name`. Update `CharacterClassTest` (remove id/byId asserts; keep count/uniqueness-of-name).                                                                    |
| `bohpts-core`            | New `BohptsCharacterClasses.fromClassId(ClassId)` switch. `CharacterMapping` uses it (replaces `byId`). `BohptsClassTemplateProvider` emits `clazz` (drops `getName`-based `name`). CRLF. ⚠️ won't compile until adapter-api released + pinned (version lockstep). |
| `nx-gamedata`            | `gd_class_templates`: `name` column → `clazz VARCHAR` (token). Domain/mapper/adapter: store `clazz.name()`; drop `name`. (Revise the not-yet-deployed `v2.5.0` migration.)                                                                                         |
| `nx-wiki`                | Class DTOs: carry `clazz: CharacterClass` + resolved display `name` (from D4 source). Query service resolves the name. Skills/tree unchanged otherwise.                                                                                                            |
| frontend (`nx-wiki-web`) | Out of scope; api-changelog updated (class identity now an enum + resolved name).                                                                                                                                                                                  |

> Per §7.1, nx-gamedata also adds the global seed table `gd_class_names(clazz PK, name JSONB)`
> (Liquibase-seeded EN/RU/UK, not sync-written); nx-wiki resolves the localized display name by
> joining `gd_class_templates.clazz → gd_class_names.name`.

## 6. Breaking changes & migration

- **`CharacterClass.byId`/`getId` removal** — breaks `CharacterMapping` (migrated in D2) and
  `CharacterClassTest` (updated). No other functional caller. Olympiad unaffected (raw int).
- **Wire**: `CharacterClass` constant **names are unchanged**, so the character-sync wire value
  is unchanged — **not** a wire-breaking rename. The break is purely source-level (`byId` gone).
- **Skill `class_name` value canonicalization** — `BohptsSkillTemplateProvider` switches from the
  host token (`SORCEROR`) to the canonical token (`SORCERER`). On the next skill re-sync,
  `gd_skill_template_classes.class_name` repopulates with canonical tokens. Any consumer
  filtering/joining on the old host spelling must switch to the canonical token. (nx-wiki's
  skill-detail "classes that learn this" just displays it — fine.) No schema change; value only.
- **Class-template wire reshape** (`int id`/`parentClassId` → `clazz`/`parentClazz`) is
  internal-only — the classes-page pipeline isn't released yet (canonicalization ships first, §7.4).
- **Release order (version lockstep)**: publish `nx-gs-adapter-api` (+ `gd-sync-core` if its
  ClassTemplate ref changes) → bump pins in bohpts-core → bohpts compiles. Same gate already
  noted for the classes-page feature.
- **nx-gamedata migration**: `v2.5.0` not yet deployed → edit in place (token-keyed
  `gd_class_templates` + new seeded `gd_class_names`), no forward migration needed.

## 7. Resolved (from review)

1. **Display names → gd_* (nx-gamedata).** Global seeded `gd_class_names(clazz, name JSONB)`,
   Liquibase-seeded EN/RU/UK; nx-wiki joins for the localized name. (See D4.)
2. **No numeric id — classes are token-keyed.** Perf is not a constraint: at L2 scale (~103
   classes; ~10²–10⁴ class-skill rows/server) an equality lookup/join on an indexed short
   `VARCHAR` token vs `int4` is within noise (index traversal dominates, not the comparator). So
   we drop the int entirely: `gd_class_templates` is keyed by `clazz`; the class→skills join
   is token-based (D5). Only the skill provider's `class_name` value is canonicalized (via
   `fromClassId`); the skill stream's `class_id` column stays untouched (out of scope).
3. **`clazz` is never null for a playable class.** A non-canonical/custom class is a contract gap
   fixed by **adding the constant** to `CharacterClass` (+ `fromClassId`), not by emitting null.
   The host provider logs a WARN when a playable `ClassId` has no mapping (signal to add it).
4. **Sequencing: canonicalization first**, then the classes-page pipeline release.

## 8. Implementation notes

- D4 implemented as the global seeded `gd_class_names(clazz PK, name JSONB)` table; the
  Liquibase seed fills canonical **EN** for all 103 classes (`{"en":"…"}`); RU/UK are added later
  by `UPDATE`/merge (the wiki returns `LocalizedText`, frontend falls back to `en`).
- Full migration chosen (D5): `SkillClassLearn` → `CharacterClass clazz`,
  `gd_skill_template_classes` collapsed to `clazz`, no numeric class id anywhere.
- ⚠️ Touches the **live** skill-detail API ("classes that learn this skill"):
  `SkillClassLearnDto` `classId`/`className` → `clazz`. Documented for the frontend.
- Final naming: `ClassTemplate.classType`/`classLevel` → `type`/`tier`; tier is the
  `CharacterClassTier {BASE, FIRST, SECOND, THIRD}` enum (was `Integer 0..3`). DB token columns are
  `clazz` / `parent_clazz` (drop the `class_code` form) to match the wire field names.
- The three class enums (`CharacterClass`, `CharacterClassType`, `CharacterClassTier`) live in
  subpackage `app.l2nx.gs.adapter.api.domain.character.clazz`; `CharacterRace` stays in
  `…domain.character`.
