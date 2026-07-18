# Castle ownership + siege history sync

> Owner: @n1rmata

## Problem

The platform wants to show, per game-server, **who owns each castle and when its
next siege is**, plus a **history of past sieges (who won)**. Today nothing about
castles or sieges crosses the adapter boundary.

Two signals with different shapes:

- **Castle current state** — a tiny, slowly-changing set (≈9 castles per server):
  owning clan + the scheduled next-siege time. This is *current state*, not a
  stream of facts: the dashboard only needs the latest value, and a castle absent
  from a newer view should disappear. → **periodic full snapshot**, exactly like
  `BossRespawnSnapshotEvent` / `GameEventSnapshotEvent`.
- **Siege outcomes** — a discrete fact each time a siege ends: which castle, when
  it started, the outcome (captured / defended / draw), the winning clan, and the
  attacker / defender clans that registered. This is append-only history. →
  **discrete per-occurrence event**, exactly like `RaidKillEvent`.

Both ride **one new event family `castle`** (topic `<tenant>.gs.events.castle`),
multiplexed by the `Nx-Message-Type` header — the same multi-event-family shape
the `raid` topic already uses (`RaidKillEvent` + `BossRespawnSnapshotEvent`).

The design stays **build-agnostic**: castles/sieges are near-universal in L2 but
the contract must not assume a particular core's siege model. The siege outcome
is an **open string** (well-known `captured` / `defended` / `draw`) and both
events carry an open `metadata` map, mirroring every other event DTO.

Audience: platform-side consumers (nx-gameservers ingest into `gs_castles` /
`gs_siege_history`, read downstream off the DB); host-side authors hooking the
castle/siege managers.

## Channel decision (db vs runtime vs events)

> Sibling features:
> - [`events-raid`](014-events-raid.md) — the `raid` multi-event family
    > (discrete `RaidKillEvent` + periodic `BossRespawnSnapshotEvent`). The exact
    > template this feature copies.
> - [`db-sync`](003-db-sync/spec.md) — CDC entity stream. NOT used here (see below).

Castle state is **not** db-sync. db-sync (`CastleDbDto` over CDC) would poll a
SQL table and diff it; but the next-siege time is computed in-memory (cron-derived
by the siege scheduler) and castle ownership lives in `clan_data.hasCastle`, not a
single castle column — a snapshot read straight off the in-memory `CastleManager`
is both simpler and fresher. With ≈9 rows per server the snapshot is tiny. Siege
outcomes are inherently discrete facts, not entity state — they belong on the
events channel like raid kills and olympiad matches. So: **events family `castle`,
two message types**, no db-sync entity, no runtime entity.

## Requirements

**Must:**

- [done] R1. `nx-gs-adapter-api` ships event family `kafka.events.castle` with two
  concrete event DTOs:
    - `CastleSnapshotEvent` — periodic FULL snapshot: `UUID eventId` (UUIDv7,
      REQUIRED — `occurredAt` derives from it), `List<CastleSnapshotEntry> castles`
      (null → empty), `@Nullable Map<String,String> metadata`.
    - `SiegeFinishedEvent` — one per ended siege.
      Both are hand-written Java-8 POJOs (builder + `toBuilder` + `equals`/`hashCode`/
      `toString`, JSpecify `@Nullable`, `-parameters`), mirroring
      `BossRespawnSnapshotEvent` / `RaidKillEvent` exactly. No framework annotations.

- [done] R2. `CastleSnapshotEntry`: `int castleId` (REQUIRED — stable per-castle
  upsert key), `@Nullable String name` (host resolves via `Castle.getName(null)`;
  there is no castle catalog on the platform, so the name IS carried on the wire,
  like `RaidKillEvent.bossName` / `GameEventEntry.name`), `@Nullable Long
  ownerClanId` (source sentinel `0` → `null`), `@Nullable Instant nextSiegeAt`
  (absolute scheduled next-siege moment; `null` when unknown / unscheduled),
  `@Nullable Map<String,String> metadata`.

- [done] R3. `SiegeFinishedEvent`: `UUID eventId` (UUIDv7, REQUIRED), `int castleId`
  (REQUIRED — partition key, 8-byte big-endian, so per-castle siege history lands
  on one partition in order), `@Nullable String castleName`, `@Nullable Instant
  siegeStartedAt` (scheduled start of the siege that just ended), `String outcome`
  (REQUIRED, open string — `WellKnownSiegeOutcomes`), `@Nullable Long winnerClanId`
  (the clan that holds the castle after the siege — captor on `captured`, defender
  on `defended`; `null` on `draw` = castle not won), `List<Long> attackerClanIds` /
  `List<Long> defenderClanIds` (the full participant set, registered on each side
  at siege end; null → empty). When `winnerClanId != null` the producer guarantees
  the winner appears in its side's list (attacker on `captured`, defender on
  `defended`) even if the engine reclassified the captor out of the attacker list
  at siege-end — so the participant set always includes the winner.
  `@Nullable Map<String,String> metadata`.

- [done] R4. `WellKnownSiegeOutcomes` constants (open, non-exhaustive, mirrors
  `WellKnownBossStatuses`): `CAPTURED="captured"` (a different clan took the
  castle), `DEFENDED="defended"` (the prior owner held), `DRAW="draw"` (castle
  unowned at siege end — no winner). Hosts MAY emit other values; consumers treat
  unknowns as opaque.

- [done] R5. `nx-gs-adapter-core` `EventTypeRegistry` registers both types under
  family key `"castle"`: `CastleSnapshotEvent` → round-robin partition key
  (`null`, like other snapshots); `SiegeFinishedEvent` → partition key
  `castleId` (8-byte big-endian). `Nx-Message-Type` = the simple class name
  (automatic). **Platform follow-up (out of this repo):** nx-tenants must return
  `castle → <tenant>.gs.events.castle` in the `/connect` `messagingTopics.events`
  map, and the Kafka topic must be created — else the family is "disabled" and
  `publish` is a silent no-op.

- [done] R6. Host (bohpts-core) ships:
    - `CastleSnapshotPublisher` (static `bind(NxEvents)` + `tick()`) — builds a
      `CastleSnapshotEvent` from `CastleManager.getCastles()` every 30 s
      (`scheduleAtFixedDelay`, `RateLimitedWarn` on failure), mirroring
      `BossRespawnSnapshotPublisher`.
    - `SiegeFinishedPublisher` (static `bind` + `publish(SiegeFinishedEvent)`),
      mirroring `RaidKillPublisher`.
    - `SiegeFinishedRecorder` — an `OnSiegeStatusListener` whose
      `onEnd(siege, winClan, defClan)` assembles the event and publishes it.
      Registered on every castle's `Siege` via `castle.getSiege().addListener(...)`
      in `BohptsEventsModule.onConnect` (the core's provided extension point; no
      engine edit). `winClan != null` → `captured` (winner = winClan); else
      `defClan != null` → `defended` (winner = defClan); else `draw` (no winner).
    - `BohptsEventsModule` binds both publishers, schedules the snapshot tick,
      registers/unregisters the recorder.

- [done] R7. Consumer (nx-gameservers) ingests family `castle`:
    - `CastleEventConsumer` (`@KafkaListener` on `^.*\.gs\.events\.castle$`, group
      `nx-gameservers-events-castle`) dispatches by `Nx-Message-Type`.
    - `CastleSnapshotEvent` → `CastleSnapshotIngestor` → `gs_castles` full-scope
      replace gated by a per-scope watermark (`gs_castles_sync`), mirroring
      `BossRespawnIngestor`.
    - `SiegeFinishedEvent` → `SiegeHistoryIngestor` → append into `gs_siege_history`
      (+ `gs_siege_history_participants` child), idempotent on `event_id`, mirroring
      `RaidKillIngestor`.

- [dropped] R8. **No read API.** Castle / siege data is ingestion-only —
  `gs_castles`, `gs_siege_history` (+ participants) are projected for downstream
  consumers reading the DB directly; nx-gameservers exposes no `/castles` REST
  surface. Consequently **no `CASTLES_*` permission** is defined (removed from
  `Permissions` + registry; no nx-users seed needed).

**Won't (this iteration):**

- No siege *schedule* history beyond outcomes (no "registration opened" events).
- No per-clan siege standings aggregation (CubeJS concern downstream).
- No castle tax / treasury / functions on the wire (not requested; add later as
  open `metadata` keys if needed — the envelope is already open).
- No nx-telegram surfacing this round (only the three repos + the new topic).

## Wire shapes

```
CastleSnapshotEvent {
  eventId: UUIDv7,
  castles: [ CastleSnapshotEntry { castleId, name?, ownerClanId?, nextSiegeAt?, metadata? } ],
  metadata?: { ... }
}                                   // Nx-Message-Type: CastleSnapshotEvent, key: null (round-robin)

SiegeFinishedEvent {
  eventId: UUIDv7, castleId, castleName?, siegeStartedAt?,
  outcome: "captured"|"defended"|"draw", winnerClanId?,
  attackerClanIds: [..], defenderClanIds: [..], metadata?: { ... }
}                                   // Nx-Message-Type: SiegeFinishedEvent, key: castleId (8-byte BE)
```

## Storage (nx-gameservers)

`gs_castles` (current-state projection, full-scope replace + dedicated
`gs_castles_sync` watermark — identical pattern to `gs_boss_respawns`):
`(tenant_id, server_id, castle_id)` PK, `name`, `owner_clan_id`, `next_siege_at`,
`ingested_at`.

`gs_siege_history` (append-only): `(tenant_id, server_id, event_id)` PK,
`castle_id`, `castle_name`, `siege_started_at`, `finished_at` (=
`UUIDv7.extractCreatedAt(event_id)`), `outcome`, `winner_clan_id`, `ingested_at`.
`gs_siege_history_participants` child: `(…, event_id, side, line_no)` PK,
`clan_id`, FK → `gs_siege_history … ON DELETE CASCADE`. `side ∈ {ATTACKER,
DEFENDER}`. Idempotent on `event_id` (`ON CONFLICT DO NOTHING`).

## Edge cases / resolved decisions

- **Snapshot reads in-memory state.** `castle.getSiege().getSiegeStartTime()` is
  read post-server-start, when every castle's `Siege` is already created by the
  siege manager — so `getSiege()` returns the cached instance, no scheduling side
  effect. Assumed: `Siege` instances are stable across cycles (this core reuses
  them; `endSiege()` does not null `_siege`), so registering the
  `OnSiegeStatusListener` once at connect is sufficient. (Assumed question A1.)
- **Owner sentinel.** `Castle.getOwnerId()==0` ⇒ no owner ⇒ `ownerClanId=null`.
- **Draw.** No winner; `winnerClanId=null`, `outcome="draw"`.
- **At-least-once.** Snapshot ingest dedups via the `gs_castles_sync` watermark
  (newest `occurredAt` wins, out-of-order older snapshots dropped). Siege history
  dedups on `event_id` (`ON CONFLICT DO NOTHING`).
- **Name on the wire.** Unlike bosses (resolved from an NPC catalog), there is no
  castle catalog on the platform, so the host carries `Castle.getName(null)`.
- **Republish volume.** ≈9 castles × one snapshot / 30 s is negligible.

## Assumed questions (for review)

- **A1.** Siege-end hook via the core's `OnSiegeStatusListener` (registered on each
  castle's `Siege` at connect) rather than an engine edit in `Siege.endSiege()`.
  Cleaner (uses the provided extension point) but assumes `Siege` instances are
  stable across cycles. Alternative: a one-line `SiegeFinishedRecorder.record(...)`
  call inside `endSiege()` (mirrors how `RaidKillRecorder` is invoked from
  `Attackable.onDeath`) — bulletproof against instance churn but touches the
  engine. Chose the listener; easy to switch.
- **A2.** Snapshot cadence 30 s (matches the other snapshot publishers). Castle
  state changes rarely; could be much slower. Kept consistent.
- **A3.** Siege history carries attacker/defender **clan ids** (the full
  participant set at end, winner guaranteed included on its side). Did not include
  per-clan kill/death counts (available on `SiegeClan` siblings) — out of the
  "who won" ask; easy to add as metadata later.
- **A4.** `winnerClanId` = the post-siege holder (captor or successful defender).
  Did not add a separate `previousOwnerClanId` — at `onEnd` time the castle owner
  is already updated to the new holder and the prior owner isn't cleanly exposed.
- **A5.** No castle tax/treasury on the wire (not requested). Trivial to add as
  `metadata` keys + a `WellKnownCastleMetadata` constants file if wanted.

## Release follow-ups (out of the three-repo scope)

- nx-tenants: register the `castle` events family in the `/connect` response.
- Kafka: create topic `bohpts.gs.events.castle` (nx-infra doc updated).
- nx-gs-adapter-api: cut a new `api/vX.Y.Z`; bump the pinned
  `nx-gs-adapter-api` in `bohpts-core/core/build.gradle` so the host compiles
  against the new symbols (nx-gameservers uses the composite build — no bump).
- nx-users: nothing — ingestion-only, no `CASTLES_*` permission to seed.
