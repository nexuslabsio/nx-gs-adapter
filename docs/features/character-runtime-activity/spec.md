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

- [done] R3. `CharacterRuntimeDto` MUST add `@Nullable CustomActivity
  customActivity` — the build-specific sustained activity as a **structured
  object**, not a bare string, because activities carry extra info (fishing:
  elapsed time, penalty tier/multiplier, time-to-next-tier). `CustomActivity` is
  a thin agnostic envelope: `String type` (REQUIRED discriminator) + `@Nullable
  Map<String,String> metadata` (open, stringified values — same shape as
  `BossRespawnEntry.metadata`). `null` customActivity = "no special activity".
  Independent of `aiStatus` (any combination legal); **no precedence** between
  the two on the wire — combining them is a consumer concern. Only `type` is
  typed; everything else (even `elapsed_seconds`) is an open metadata key, so the
  contract never names a per-activity field.

- [done] R4. `nx-gs-adapter-api` MUST ship `WellKnownCustomActivities` (the
  `type` discriminator values `fishing` / `reading`) **and**
  `WellKnownCustomActivityMetadata` (the canonical metadata keys: common
  `elapsed_seconds`; fishing `penalty_multiplier` / `penalty_tier` /
  `seconds_to_next_tier` + tier values `none` / `tier1` / `tier2`). Both
  documented as non-exhaustive / open: a host MAY emit its own type or keys
  without an API release; consumers ignore unknowns.

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
    - `customActivity` = a `CustomActivity{type=fishing, metadata=…}` when
      `player.isFishing()`, else `null`. Metadata is pulled live from
      `FishingPenaltyManager`: `elapsed_seconds` (= `getElapsedMs/1000`, the
      paused-accumulator active-fishing clock) plus, when
      `Config.FISHING_PENALTY_ENABLE`, `penalty_tier` / `penalty_multiplier` and
      `seconds_to_next_tier` derived from the config tier thresholds (mirroring
      `getChanceMultiplier`'s logic; `seconds_to_next_tier` omitted at the worst
      tier);
    - the hand-written FNV-1a `hash(dto)` MUST mix `aiStatus` and the
      `CustomActivity` (`type` + each `metadata` entry, insertion-ordered) so a
      change triggers a diff/publish. `elapsed_seconds` advances each snapshot →
      a fishing character republishes every runtime tick (~10s), accepted (char
      population is < 10k). Offline tombstones carry neither.

- [done] R7. `nx-gameservers` MUST persist both fields on `gs_characters` as
  `ai_status VARCHAR(32)` + `custom_activity JSONB`, written by the runtime-sync
  upsert path (`CharacterRuntimeIngestor` → `UPSERT_RUNTIME_SQL`). The ingestor
  serializes `CustomActivity` to JSON via `JsonMapper` and binds it
  `CAST(:customActivity AS jsonb)`. Stored **opaque** — the platform never models
  per-activity keys, so any build's metadata round-trips. NOT timestamp-gated —
  rides the unconditional runtime-column overwrite (same as vitals / coordinates);
  newest tick wins. db-sync never writes them.

**Should:**

- [done] R8. The `GET /v1/gameservers/characters` read API MUST expose both
  fields on `CharacterListItemDto`. `aiStatus` is a plain string; `customActivity`
  is passed through as **raw JSON** via `@JsonRawValue` on a `String` field (the
  repo reads the jsonb column with `rs.getString` and forwards it verbatim — no
  parsing, the platform stays agnostic to fishing's keys). Display-only, no new
  filter or sort.

- [done] R10. The `nx-telegram` bot MUST show fishing info in the character
  detail view, **only while the character is fishing**, positioned **above the
  HP/CP/MP block**. Renders from `customActivity` when `type=fishing`: time spent
  fishing (`elapsed_seconds`), current `penalty_multiplier`, and the countdown to
  the next penalty tier (`seconds_to_next_tier`; omit the countdown line at the
  worst tier). Hidden entirely when not fishing / no activity.

**Could:**

- [todo] R9. Typed metadata (promoting `elapsed_seconds` etc. to typed fields, or
  a `Map<String,Object>`) — explicitly NOT shipped. Chose a fully-open
  `Map<String,String>` (stringified values) for maximum agnosticism and to match
  the existing event `metadata` pattern; consumers parse keys they care about.

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
- **Unknown `aiStatus` value / unknown `customActivity.type` or metadata key at
  the consumer** → `aiStatus` stored/displayed raw; `customActivity` stored as
  opaque JSONB and re-emitted raw by the read API; no behaviour gated on either.
- **`getAI()` null mid-transition** → `aiStatus = null` (guarded), row still
  emitted with the other fields.
- **`PICK_UP` intention** → canonical `pick_up` (snake_case, consistent with
  `in_combat` in `WellKnownBossStatuses`).
- **Activity churn** → `aiStatus` flips with combat/movement; a fishing
  character republishes every tick because `elapsed_seconds` advances. Both ride
  the existing tick diff (no new channel). Accepted — character population is
  < 10k and republishing runtime rows is cheap. (A churn-free alternative —
  carrying a `startedAt` instant and deriving elapsed on the consumer — was
  rejected: the penalty clock is a *paused accumulator* of active fishing time,
  not wall-clock since start, so no single timestamp yields it.)
- **Runtime arrives before db-sync** → skeleton `gs_characters` row gets the
  activity columns; db columns stay `null` until db-sync catches up (existing
  overlay behaviour, unchanged).

## Open questions

- [resolved: `aiStatus` is an **open string** (`WellKnown*` constants), not an
  enum. Mirrors `BossRespawnEntry.status`; keeps the contract build-agnostic.]
- [resolved: `customActivity` is a **structured `CustomActivity` object**
  (`type` + open `Map<String,String> metadata`), NOT a bare string — activities
  carry extra info (fishing elapsed/penalty). Chose a fully-open metadata map
  over typed fields (even `elapsed_seconds` is a key) for maximum agnosticism,
  matching the event `metadata` pattern. The map key is `metadata` (not
  `attributes`) to align with the other event DTOs.]
- [resolved: stored as **JSONB** on `gs_characters.custom_activity` (opaque);
  read API re-emits raw via `@JsonRawValue`. The platform never models
  per-activity keys. The unreleased `v2.3.0` changeset was edited in place
  (VARCHAR → JSONB) rather than adding a follow-up migration.]
- [resolved: live `elapsed_seconds` (republish every ~10s tick per fisher) is
  fine — char population < 10k. A `startedAt`-timestamp churn-free model doesn't
  fit the paused-accumulator penalty clock.]
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

- `nx-gs-adapter-api` — **minor** bump (additive `CharacterRuntimeDto.aiStatus`
    + `customActivity` fields, new `CustomActivity` type, `WellKnownAiStatuses` /
      `WellKnownCustomActivities` / `WellKnownCustomActivityMetadata`; single
      canonical 15-arg constructor; wire-compatible — old producers omit the keys).
- `nx-gs-adapter-core` / `nx-gs-db-sync-core` / `nx-gs-runtime-sync-core` /
  `nx-gs-kafka` — no contract change (the runtime engine hashes whatever the
  mapping mixes).
- `bohpts-core` — `CharacterRuntimeMapping` populates + hashes both fields.
- `nx-telegram` — character detail view renders the fishing block (R10).
- `nx-gameservers` — Liquibase `v2.3.0_character_activity.sql`, runtime upsert,
  read API.

## Amendment — `customActivities` array + autofarming

> Supersedes the singular-`customActivity` shape described in R3–R8 above.
> A character can be in several sustained activities at once (e.g. autofarming
> while fishing), so the single object becomes a **list**, and a new
> `autofarming` activity is added.

- **A1. `CharacterRuntimeDto.customActivity` (single `CustomActivity`) →
  `customActivities` (`@Nullable List<CustomActivity>`).** Wire field is now a
  JSON **array** of `{type, metadata}` objects. `null` / omitted / empty = "no
  special activity". The list is defensively copied + unmodifiable. `CustomActivity`
  itself is unchanged (`type` + open `metadata`). Builder/getter/hash all move to
  the plural. The 15-arg canonical constructor keeps its arity (last arg type
  changes `CustomActivity` → `List<CustomActivity>`).
- **A2. New activity `autofarming`** (`WellKnownCustomActivities.AUTOFARMING`) —
  server-side auto-hunt. Time-limited builds carry the remaining purchased
  auto-farm time on the new metadata key
  `WellKnownCustomActivityMetadata.SECONDS_REMAINING` (`seconds_remaining`),
  omitted when the farm is unlimited / free.
- **A3. bohpts `CharacterRuntimeMapping`** now collects a list:
  `resolveFishing(p)` (unchanged logic) + `resolveAutofarming(p)`. Autofarming is
  emitted while `player.getFarmSystem().isAutofarming()`; `seconds_remaining`
  comes from `AutoFarmOptions.getFarmEndTaskDelay(SECONDS)` (the scheduled
  farm-end countdown; `-1` → unlimited → key omitted). `hash(dto)` iterates the
  list (fixed order fishing→autofarming) so the diff/publish trigger is stable.
- **A4. `nx-gameservers`** — column `gs_characters.custom_activity` →
  `custom_activities` (still JSONB; now holds the array). Migration `v2.3.0`
  edited in place (assumed not yet deployed — otherwise a follow-up
  `ALTER … RENAME COLUMN` migration is needed). `CharacterRepository` serializes
  the `List<CustomActivity>`; read API field `customActivities` stays
  `@JsonRawValue` raw-JSON passthrough (now an array).
- **A5. `nx-telegram`** — `CharacterStats.activity` → `activities`
  (`List<CharacterActivity>`); adapter parses the JSONB array; char-info screen
  renders one block per activity (fishing as before + a new `autofarming` block
  showing remaining time). New localization keys `chars.info.autofarming.{title,
  remaining}` (ru/en/uk).

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
