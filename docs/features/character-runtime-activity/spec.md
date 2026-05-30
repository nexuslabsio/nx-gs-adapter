# Character runtime activity (AI status + custom activity)

> Owner: @n1rmata

## Problem

The platform wants to show *"what is this character doing right now"* on a live
admin / dashboard view. Today `CharacterRuntimeDto` (the per-tick runtime-sync
channel) carries vitals, vitality, world coordinates and a presence marker — but
nothing about the character's current behaviour.

Two distinct, **orthogonal** signals are wanted:

- **AI status** — the engine-native control intention the core puts a player
  character in (idle / moving / attack / cast / pick-up / interact / …). Present
  in some form on essentially every L2 core, since this reactive state machine is
  how the server translates client input into world actions.
- **Custom activity** — a *build-specific* sustained activity that lives outside
  the engine AI state machine. The archetype is fishing: a self-contained
  mini-engine (own immobilize + tick loop, no AI intention) on the cores that
  ship it — and some servers don't ship fishing at all, replacing it with a
  different activity (reading a book, mining, …). The set genuinely varies per
  build.

The design must stay **build-agnostic**: the wire contract and the platform
storage must not encode "fishing" (or any one core's activity vocabulary) as a
first-class concept, and AI status must tolerate cores whose intention set
differs.

Audience: platform-side dashboard consumers; host-side authors hooking the
runtime character mapping.

## Requirements

> Parent / sibling features:
> - [`runtime-sync`](../runtime-sync/spec.md) — the FNV-1a snapshot+diff engine
    > and `RuntimeEntityMapping` SPI this rides. UNCHANGED.
> - [`character-core-extension`](../character-core-extension/spec.md) /
    > [`character-online-hero`](../character-online-hero/spec.md) —
    > `CharacterRuntimeDto` / `CharacterDbDto` shape. Extended additively.

**Must:**

- [done] R1. `CharacterRuntimeDto` MUST add `@Nullable String aiStatus` — the
  engine AI control intention as an **open string**. Additive + nullable; hosts
  that do not report it leave it `null`. Open string (not an enum) so a core with
  a divergent intention set is not a breaking contract change.

- [done] R2. `nx-gs-adapter-api` MUST ship `WellKnownAiStatuses` documenting the
  canonical lower_snake_case values — `idle` / `active` / `rest` / `attack` /
  `cast` / `moving` / `follow` / `pick_up` / `interact` (the L2 `CtrlIntention`
  set, name lower-cased). Hosts MAY emit additional non-canonical values;
  consumers treat unknowns as opaque (display raw, no behaviour routed on it).
  Mirrors the existing `WellKnownBossStatuses` pattern.

- [done] R3. `CharacterRuntimeDto` MUST add `@Nullable String customActivity` —
  the build-specific sustained activity as an **open string**. `null` = "no
  special activity". Independent of `aiStatus`: any combination is legal
  (`aiStatus=idle` + `customActivity=fishing`; `aiStatus=moving` +
  `customActivity=null`; both null; both set). The wire encodes **no precedence**
  between the two — combining them is purely a consumer concern.

- [done] R4. `nx-gs-adapter-api` MUST ship `WellKnownCustomActivities` with the
  commonly-seen values (`fishing`, `reading`), explicitly documented as a
  non-exhaustive, non-required set: a build with none of them is valid, and a
  host needs no API release to emit a brand-new activity string.

- [done] R5. Both fields are additive. The single canonical `CharacterRuntimeDto`
  constructor is **extended** to 15 args (NOT a second overload) — matching how
  `CharacterDbDto` grew, and deliberately avoiding two public constructors, which
  would make the consumer's parameter-name Jackson creator selection
  (`USE_PROPERTIES_BASED`) ambiguous. Builder + getters are the supported
  surface; the only positional callers are this module's own tests (updated).
  Wire-compatible: a producer on the old api simply omits the two new JSON keys,
  which deserialize as `null`.

- [done] R6. The bohpts runtime mapping (`CharacterRuntimeMapping`) MUST populate
  both fields from live state:
    - `aiStatus` = `player.getAI().getIntention().name().toLowerCase(ROOT)`
      (1:1 with `WellKnownAiStatuses`; `null` when the AI / intention is
      unavailable);
    - `customActivity` = `WellKnownCustomActivities.FISHING` when
      `player.isFishing()`, else `null` (fishing is the only sustained activity
      this core models outside the AI machine today);
    - both MUST be mixed into the hand-written FNV-1a `hash(dto)` so a change to
      either triggers a diff/publish (the engine diffs on this hash — an unmixed
      field would never surface a change). Offline tombstones carry neither.

- [done] R7. `nx-gameservers` MUST persist both fields on `gs_characters` as
  `ai_status VARCHAR(32)` + `custom_activity VARCHAR(64)`, written by the
  runtime-sync upsert path (`CharacterRuntimeIngestor` → `UPSERT_RUNTIME_SQL`).
  Stored as the raw open string (no enum / CHECK) so unknown build-specific
  values survive round-trip. NOT timestamp-gated — they ride the unconditional
  runtime-column overwrite (same as vitals / coordinates); newest runtime tick
  wins. db-sync never writes them.

**Should:**

- [done] R8. The `GET /v1/gameservers/characters` read API MUST expose both
  fields on `CharacterListItemDto` (display-only — no new filter or sort) so the
  dashboard can render them. Plumbed through `CharacterReadRepository`
  (`CharacterListRow` + SELECT + row mapper) and `CharacterQueryService`.

**Could:**

- [todo] R9. A sibling `customActivityMeta` (open `Map<String,String>` for extra
  context like fish-count / session-duration) — explicitly NOT shipped (YAGNI;
  the dashboard wants a label, not a payload). The open string leaves room to add
  it later without a breaking change.

**Non-goals:**

- **An `activity`/`fishing` enum on the wire or a CHECK-constrained column.**
  Rejected — it would bake one core's activity vocabulary into a build-agnostic
  contract. Open string + `WellKnown*` constants is the deliberate choice.
- **A `FISHING` (or any activity) value on `aiStatus`.** Fishing is not an engine
  intention; it is a separate, orthogonal axis. The two fields stay independent.
- **Coupling / precedence between `aiStatus` and `customActivity` on the wire or
  in DB.** Display priority (if any) is a consumer decision.
- **Filtering / sorting the character list by activity.** Display-only for now;
  add an index + filter if a real use case appears.
- **History / analytics of activity over time.** This is live-state overlay only;
  a discrete event stream would be a separate feature if ever needed.
- **A CDC (`CharacterDbDto`) column.** Activity is volatile/live — it belongs on
  the runtime channel, not the poll-based CDC hash.

### Edge cases

- **Core without fishing** → `customActivity` stays `null` until that core's
  mapping emits its own activity string (e.g. `reading`); no contract change.
- **Unknown `aiStatus` / `customActivity` value at the consumer** → stored and
  displayed as the raw string; no behaviour is gated on it.
- **`getAI()` null mid-transition** → `aiStatus = null` (guarded), row still
  emitted with the other fields.
- **`PICK_UP` intention** → canonical `pick_up` (snake_case, consistent with
  `in_combat` in `WellKnownBossStatuses`).
- **Activity churn** → `aiStatus` flips with combat/movement, same cost profile
  as the `x`/`y`/`z` already on the runtime channel; both ride the existing tick
  diff, no new channel. A core that wants to dampen churn can coarsen its mapping
  (e.g. collapse `attack`/`cast` → `combat`) without any contract change.
- **Runtime arrives before db-sync** → skeleton `gs_characters` row gets the
  activity columns; db columns stay `null` until db-sync catches up (existing
  overlay behaviour, unchanged).

## Open questions

- [resolved: both `aiStatus` and `customActivity` modeled as **open strings +
  `WellKnown*` constants**, not enums. Mirrors `BossRespawnEntry.status`; keeps
  the contract build-agnostic and lets hosts extend without an API release.]
- [resolved: **two independent fields**, not one unified "state". Faithful to the
  engine (fishing = `idle` AI + `fishing` activity); no precedence baked into the
  wire — the consumer decides how to combine.]
- [resolved: name `customActivity` (not `activity` / `occupation` / `pastime`).
  The `custom` prefix self-documents that unknown, build-specific values are
  expected, and removes the conceptual overlap with `aiStatus`.]
- [resolved: carried on the **runtime channel** (`CharacterRuntimeDto`), not CDC
  and not a discrete event — it is volatile live state and the purpose is live
  dashboard display.]
- [resolved: **not timestamp-gated** in the DB upsert — rides the unconditional
  runtime-column overwrite like vitals/coords. Presence (`online`) keeps its
  `last_seen_at` gate; activity does not need cross-source reconciliation (only
  the runtime path writes it).]
- [assumed: exposing the fields on the existing `/characters` read API
  (display-only) is in scope for "dashboard". Drop R8 if the dashboard reads
  `gs_characters` by another path.]
- [assumed: `customActivity` stays a pure label (no metadata payload) — R9 YAGNI.]

## Versioning

- `nx-gs-adapter-api` — **minor** bump (additive nullable `CharacterRuntimeDto`
  fields + two new `WellKnown*` constant classes; back-compat constructor kept;
  non-breaking).
- `nx-gs-adapter-core` / `nx-gs-db-sync-core` / `nx-gs-runtime-sync-core` /
  `nx-gs-kafka` — no contract change (the runtime engine hashes whatever the
  mapping mixes).
- `bohpts-core` — `CharacterRuntimeMapping` populates + hashes the two fields.
- `nx-gameservers` — Liquibase `v2.3.0_character_activity.sql`, runtime upsert,
  read API.

## Links

- Rides: [`runtime-sync`](../runtime-sync/spec.md)
- Sibling: [`character-online-hero`](../character-online-hero/spec.md),
  [`character-core-extension`](../character-core-extension/spec.md)
- Pattern precedent: [`events-raid`](../events-raid/spec.md)
  (`BossRespawnEntry.status` open string + `WellKnownBossStatuses`)
- Host source: `bohpts-core`
  `l2e.gameserver.l2nx.sync.runtime.CharacterRuntimeMapping`
- Consumer: `nx-gameservers` `CharacterRuntimeIngestor` / `CharacterRepository`
  / `CharacterReadRepository`
