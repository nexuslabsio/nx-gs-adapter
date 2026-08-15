# Character online-time + hero

> Owner: @n1rmata

## Problem

`CharacterDbDto` (CDC) today carries identity / progression / ownership /
lifecycle plus the `character-core-extension` additions (`accountName`,
`nobless`, `scheduledDeletionAt`, `online`). Two fields from the deferred
backlog are still missing and now have a consumer:

- **Online time** — a generic admin "Characters" page wants accumulated
  time-on-server per character. Deferred originally because `onlinetime` is a
  volatile column written only at logout/save and CDC excluded it to avoid an
  UPDATE storm.
- **Hero status** — the `olympiad-events` consumer wants to resolve "is this
  char a hero" by joining on `charId`; the admin page wants a hero badge.

A CDC boolean answers *"is this char a hero right now"* but throws away the
**history** — who was crowned, when, in which class, in which olympiad cycle.
That timeline is durable, scarce, high-value data and needs a discrete event
emitted at the crowning moment, not just a current-state flag.

This slice drains `onlineTime` + `hero` from
[`character-deferred-fields.md`](013-character-core-extension.md#deferred-fields-and-filters-backlog) and adds a
`HeroGrantedEvent` for the historical record.

Audience: platform-side consumers (admin character page, olympiad/hero
dashboards); host-side authors hooking the CDC character mapping + the hero
crowning path.

## Requirements

> Parent / sibling features:
> - [`character-core-extension`](013-character-core-extension.md) —
    > `CharacterDbDto` shape, CDC + runtime presence. UNCHANGED.
> - [`db-sync`](003-db-sync/spec.md) / [`cdc-engine`](005-cdc-engine/spec.md) —
    > CDC mapping, `hashedColumns`, whole-DTO emit on change. UNCHANGED.
> - [`olympiad-events`](015-olympiad-events.md) — the `events.olympiad`
    > family this extends (existing `OlympiadMatchResultEvent`). UNCHANGED.

**Must:**

- [todo] R1. `CharacterDbDto` MUST add `@Nullable Long onlineTimeSeconds` —
  accumulated total online time in seconds. Additive + nullable; providers
  whose source lacks the column leave it `null`. Plain `Long` seconds (NOT
  `java.time.Duration`) — matches the existing plain-number fields
  (`pvpCounter` / `karma`) and avoids forcing every consumer to register a
  binder adapter.

- [todo] R2. Bohpts `CharacterMapping` MUST surface `onlinetime`:
    - add `onlinetime` to the `CharacterPrimarySource` `HASHED` list;
    - read via `JdbcNulls.nullableLong(rs, "onlinetime")`, thread through
      `CharacterRow` + `mapEntity`;
    - update the class Javadoc — it currently lists `onlinetime` among
      volatile columns *intentionally excluded*; that note is reversed with
      the rationale below.

  **Semantics / accepted cost:** `CharacterDAO.storePlayer` rewrites
  `onlinetime` to the **live total** (`getOnlineTime()` + current-session
  delta) on every full store, i.e. at logout + periodic autosave. The column
  therefore advances at autosave cadence — *not* per tick. Including it in the
  hash emits one whole-`CharacterDbDto` UPDATE per online char per autosave
  cycle. Bandwidth is trivial (~5–10k DTOs per autosave interval); no consumer
  does expensive per-character-UPDATE work, so the churn is accepted. The
  online-time value for an online char is stale by up to one autosave interval
  — live ticking, if ever needed, is a platform-side derivation
  (`baseline + (now − loginTs)` from the presence stream) and is out of
  adapter scope.

- [todo] R3. `CharacterDbDto` MUST add `@Nullable Boolean hero` — current hero
  status: `true` when the character is a recognized hero in the active cycle.

- [todo] R4. Bohpts `CharacterMapping` MUST surface `hero` via a new `heroes`
  child source:
    - `tableName "heroes"`, `fkColumn "charId"` (the `heroes` table is one row
      per character);
    - `hashedColumns` = `["played"]` **only** — the flag that drives the
      boolean. `class_id` / `count` are deliberately NOT hashed: only the
      `hero` boolean is surfaced on the CDC `CharacterDbDto`, so a re-crowning
      must not churn the character record;
    - project `hero = (played == 1)`, matching the engine's own current-hero
      query (`Hero.GET_HEROES … WHERE heroes.played = 1`); `null` when the
      character has no `heroes` row.

  Impl note: confirm `played` vs `active` against bohpts' exact `Hero`
  lifecycle before finalizing the projected column.

- [todo] R5. `nx-gs-adapter-api` MUST ship
  `kafka.events.olympiad.HeroGrantedEvent` as a second concrete event on the
  (now multi-event) `olympiad` family — additive; existing
  `OlympiadMatchResultEvent` is untouched. Fields:
    - `UUID eventId` — REQUIRED. UUIDv7; upper 48 bits encode the crowning
      timestamp → consumers extract `occurredAt` from the id. No separate
      `crownedAt`. Null-checked in the constructor.
    - `long charId` — REQUIRED.
    - `int classId` — the class the character was crowned hero with.
    - `@Nullable Long clanId` — clan affiliation snapshot at crowning, read
      from `characters.clanid` for every crowned hero (online + offline);
      `null` only when the character has no clan.
    - `int olympiadCycle` — the cycle these heroes were crowned for.

  POJO + hand-written Builder + `equals`/`hashCode`/`toString` + Java-8 source,
  `-parameters` preserved for parameter-name deserialization.

- [todo] R6. `nx-gs-adapter-core.events.EventTypeRegistry` MUST bind
  `HeroGrantedEvent`: family `"olympiad"`, message-type `"HeroGrantedEvent"`,
  partition-key extractor returning `charId` (8-byte big-endian) — the **same**
  key as `OlympiadMatchResultEvent`, so a character's olympiad + hero timeline
  co-locates on one partition in occurrence order.

- [todo] R7. `bohpts-core` MUST ship
  `l2e.gameserver.l2nx.events.olympiad.HeroGrantedPublisher` — a game-loop-safe
  static facade mirroring `OlympiadMatchResultPublisher` (volatile
  `@Nullable NxEvents events`, `bind(handle)` / `bind(null)`, error-swallowing
  `publish`). Bound on `BohptsEventsModule.onConnect`, released on
  `onDisconnect`. No new `AdapterModule` ServiceLoader entry — rides the
  existing `bohpts-events` module.

- [todo] R8. `bohpts-core` MUST hook `Hero.computeNewHeroes(...)` to emit one
  `HeroGrantedEvent` per crowned hero. The hook fires for online **and**
  offline winners — `charId` / `classId` come from the per-hero
  `StatsSet`, `olympiadCycle` from the crowning context. Any uncaught
  `Throwable` is logged at ERROR and swallowed — never propagates into the
  crowning path.

- [todo] R9. The `olympiad` Kafka topic retention MUST be raised in
  `nx-infra/komodo` so the once-per-cycle, scarce hero grants survive a
  consumer outage (hero grants share the high-volume match-result topic per the
  family decision below). Target: **≥ 7 days** — long enough that an outage
  spanning a crowning does not permanently lose heroes (Kafka is a replay
  buffer; permanence is consumer-side). Document the rationale next to the
  topic config; operator may tune down if storage is tight.

- [todo] R12. While in `CharacterDbDto`, rename the existing misspelled field
  `nobless` → `noblesse`. The wire key changes `nobless` → `noblesse`; the
  source DB column stays `nobless` (the mapping reads it unchanged). Breaking
  for the platform character-CDC consumer, which MUST read `noblesse`.

**Should:**

- [todo] R10. Consider additionally emitting `HeroGrantedEvent` from
  `Hero.activateHero(...)` for admin / manual hero grants that bypass
  `computeNewHeroes`, guarded against double-emit when both paths run. Defer
  unless admin-granted heroes are a real operational case.

**Could:**

- [todo] R11. "Times hero" counter — explicitly NOT synced (YAGNI). Neither a
  CDC `CharacterDbDto.heroCount` field nor a `HeroGrantedEvent.count` is
  carried; if a consumer ever needs it, derive by counting `HeroGrantedEvent`s
  per `charId`, or add a field then.

**Non-goals:**

- **Hero-diary events** (raid-boss-killed, castle-taken — bohpts tracks these
  in `Hero`). A separate event family if/when a consumer needs them.
- **Hero "revoked" / cycle-reset events.** Derivable from the next crowning +
  the CDC `hero` flip; bulk-reset events would be N-events at the cycle
  boundary, not warranted (same reasoning the `olympiad-events` spec used).
- **Live ticking online-time on the runtime channel.** Rejected — it changes
  every tick for every online char, exactly the UPDATE storm
  `CharacterMapping` was written to avoid. The platform derives the live value.
- **Platform-side live online-time composition + offline backfill of chars
  never seen by the adapter.** Consumer concern (`nx-gameservers`).

### Edge cases

- **No `heroes` row** → `hero = null` (treated as not-a-hero downstream).
- **Cycle reset** (`UPDATE heroes SET played = 0, active = 0`) flips every past
  hero's `played` to 0 → one CDC UPDATE per past hero at the cycle boundary
  (`hero → false`). Bounded by total historical hero count, once per cycle.
- **Re-crowned in consecutive cycles** → `played` goes `1 → 0` (reset)
  `→ 1` (crowning). If both happen between two CDC ticks the net hash is
  unchanged (`1 → 1`) → no spurious flip.
- **Offline winner at crowning** → event still emits; `clanId` is resolved
  from `characters.clanid` exactly as for online winners.
- **Adapter disconnected at crowning** → `HeroGrantedPublisher` is a no-op
  (`events` ref null); the grant is lost on the wire. Rare (crowning is a
  scheduled moment) and consistent with every other host publisher's contract.
- **`onlinetime` source default 0** → `onlineTimeSeconds = 0`.
- **Online char online-time staleness** → up to one autosave interval; the DB
  column only advances at `store()`. Acceptable for the admin page; live value
  is platform-derived.

## Open questions

- [resolved: online-time via CDC with `onlinetime` **in the hash**. Rejected a
  live runtime field (per-tick churn) and carrying it on
  `CharacterPresenceEvent` (more platform composition). `store()` writes the
  live total each save, so freshness = autosave cadence and the per-online-char
  full-DTO churn per autosave is accepted.]
- [resolved: `onlineTimeSeconds` as `Long` seconds. Rejected `Duration` —
  binder friction (Gson serializes it as a `{seconds,nanos}` object) for no
  benefit.]
- [resolved: `hero` as a CDC `Boolean` from a `heroes` child source hashed on
  `played` only. Rejected a `heroCount` CDC field and a `HeroGrantedEvent.count`
  (YAGNI — derive by counting events per charId if ever needed).]
- [resolved: `HeroGrantedEvent` rides the existing `events.olympiad` family,
  NOT a new `events.hero` family. The retention-independence argument (scarce
  grants vs high-volume match results, per the raid-kill precedent) was weighed
  and the fewer-topics option chosen; mitigated by raising the `olympiad` topic
  retention (R9). Splitting into `events.hero` later would be a breaking api
  change.]
- [assumed: `played = 1` is the canonical "current hero" flag per `GET_HEROES`;
  confirm vs `active` during implementation.]
- [resolved: `clanId` read from `characters.clanid` in the crowning hook for
  every crowned hero (uniform online + offline) — no platform-side temporal
  join. `publishHeroGrants` already runs on the cycle-end DB-touching thread,
  so one extra indexed lookup per hero (~dozen per cycle) is negligible.]
- [resolved: `olympiadCycle` = `Olympiad.getCurrentCycle()` read in
  `publishHeroGrants` is the just-completed cycle the heroes won —
  `_currentCycle++` runs *after* `computeNewHeroes` in
  `Olympiad.ValidationEndTask.run()`, so there is no off-by-one.]

## Versioning

- `nx-gs-adapter-api` — **minor** bump (additive nullable `CharacterDbDto`
  fields + new `HeroGrantedEvent`; non-breaking).
- `nx-gs-adapter-core` — **minor** bump (one `EventTypeRegistry` binding).
- `nx-gs-db-sync-core` / `nx-gs-kafka` — no contract change.
- `bohpts-core` — `CharacterMapping` (online-time + heroes child source), new
  `HeroGrantedPublisher`, `Hero.computeNewHeroes` hook.

## Links

- Parent: [`character-core-extension`](013-character-core-extension.md)
- Backlog drained: [`character-deferred-fields.md`](013-character-core-extension.md#deferred-fields-and-filters-backlog)
- Event family extended: [`olympiad-events`](015-olympiad-events.md)
- CDC mechanism: [`db-sync`](003-db-sync/spec.md), [`cdc-engine`](005-cdc-engine/spec.md)
- Host sources: `bohpts-core` `l2e.gameserver.l2nx.sync.db.CharacterMapping`,
  `l2e.gameserver.l2nx.events.olympiad.*`, `l2e.gameserver.model.entity.Hero`
  (`computeNewHeroes` / `GET_HEROES` / `heroes` table)
