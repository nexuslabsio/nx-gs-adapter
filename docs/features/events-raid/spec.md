# Events — Raid kill

> Owner: @n1rmata

## Problem

Operators, clan leaders, and platform-side dashboards need durable per-kill records
for raid bosses — from world-grand-bosses (Antharas / Valakas / Baium /
Frintezza) down to instance bosses farmable by one party (Freya solo, daily
zones) and regular raid bosses spawned in the open world. Today only the
`feature.analytics.epicboss` rail emits a sparse START/END phase event to the
web-admin via RabbitMQ — it carries no damage breakdown, no drop list, no
participant identities. Clans cannot audit "who actually killed Valakas / what
fell / who in our clan contributed how much"; the platform cannot drive
leaderboards, drop analytics, or balance reviews.

Existing event families (`premiumpurchase`, `serveronline`, `privatestore`,
`character`) already prove the host-push wire pattern: per-family Kafka topic,
UUIDv7 `eventId`, `Nx-Message-Type` header dispatch, `NxEvents.publishX(...)`
SPI, game-loop-safe enqueue. Raid-kill stats are the next family to ride that
rail — single-event per kill, multi-aggregate payload.

Boss scope is intentionally broad: any `Attackable.isRaid() && !isRaidMinion()`
death by a player. World grand bosses and instance bosses share the same wire
shape; differentiation is the `bossKind` enum (`RAID` / `GRAND_BOSS` /
`INSTANCE_BOSS`), letting consumers split dashboards downstream.

Audience: platform-side consumers (dashboards / leaderboards / clan analytics);
host-side authors hooking raid-death paths.

## Requirements

> Sibling feature carrying the wire dispatch plumbing:
> - [`messaging`](../messaging/spec.md) — `MessagingTopics.events.<family>`
    > topic addressing, `Nx-Server-Id` connection-scoped header,
    > `Nx-Message-Type` per-record header, UUIDv7 idempotency. UNCHANGED.

**Must:**

- [todo] R1. `nx-gs-adapter-api.kafka.events.raid.RaidKillEvent` MUST ship as
  the Phase-1 concrete event (single-event family — no abstract base, future
  spawn / despawn facts ride their own families if added). Final POJO with the
  following fields:
    - `UUID eventId` — REQUIRED. UUIDv7; the upper 48 bits encode the kill
      occurrence timestamp — platform consumers extract `occurredAt` from the
      time-ordered prefix. No separate `killedAt` field.
    - `int bossNpcId` — REQUIRED. L2 NPC template id (e.g. 29028 = Valakas).
    - `@Nullable String bossName` — display name at kill time. Optional;
      platform resolves via its name catalog when null.
    - `@Nullable Integer bossLevel` — level at spawn.
    - `RaidBossKind bossKind` — REQUIRED. `RAID` / `GRAND_BOSS` / `INSTANCE_BOSS`.
    - `@Nullable Long instanceId` — instance world id when killed inside a
      reflection / instance zone; `null` for open-world kills.
    - `@Nullable RaidActor lastHit` — final-blow character + affiliation
      snapshot. `null` when the last hit came from a non-player source
      (trap, owner-less summon, kill curse). Narrative only — does NOT
      confer drop rights in L2.
    - `@Nullable RaidActor dropOwner` — host-side `mainDamageDealer` whose
      group received drop protection. Group-first semantics: when
      `dropOwner.partyId` (or `commandChannelId`) is non-null, drop rights
      belong to that group — consumers MUST aggregate analytics by
      `partyId` / `commandChannelId`, not by `dropOwner.charId` (which is
      the L2 representative of the group and is unstable across kills of
      the same party). When both group ids are null the kill was solo and
      `dropOwner.charId` IS the drop owner directly. `null` when no
      resolvable player damager (admin kill, instant kill with empty
      aggro list).
    - `List<RaidActor> participants` — REQUIRED, may be empty (raid
      killed without aggro accrual — degenerate but possible via instant-kill
      admin commands). Each entry is the same `RaidActor` shape used for
      `lastHit` / `dropOwner`. Participation = any player on the aggro list
      with `damage > 0` OR `hate > 0` (captures healers / tanks /
      aggro-skill users who never landed damage themselves), unioned with
      Party / CommandChannel members of those players (covers pure buffers
      grouped with the actual fighters). Multiple aggro entries for the
      same character (main attacks + summon) are aggregated — one
      `RaidActor` per `charId`, damage summed across entries. Ordering not
      enforced on wire; producers SHOULD emit sorted by `damageDealt` desc
      as a convenience — support entries (damage=0) park at the tail.
    - `List<RaidDropItem> drops` — REQUIRED, may be empty (raid configured
      without drops, or all drops auto-looted before the recorder caught them).

  POJO + hand-written Builder + `equals`/`hashCode`/`toString` + Java-8 source.
  Constructor parameter names preserved (`-parameters`) for Gson / Jackson
  parameter-name deserialization. `eventId` validated as non-null in the
  constructor; everything else accepted as nullable / nullable-list-normalized.

- [todo] R2. `nx-gs-adapter-api.kafka.events.raid.RaidActor` MUST ship as the
  reusable actor sub-DTO used by `lastHit`, `dropOwner`, and every
  `participants` entry. Names (char / clan) are intentionally NOT carried —
  the platform joins on `charId` / `clanId` against CDC-synced character /
  clan catalogs. Fields:
    - `long charId` — REQUIRED. Character's `objectId`.
    - `@Nullable Long clanId` — clan affiliation at the event moment.
    - `@Nullable Long allyId` — alliance affiliation.
    - `@Nullable UUID partyId` — UUIDv7 party identity (host-minted on Party
      construction, stable across leader changes within the same group
      instance, reset on disband / restart).
    - `@Nullable UUID commandChannelId` — UUIDv7 CC identity, same
      lifecycle rules as `partyId`.
    - `long damageDealt` — REQUIRED. Accumulated damage from the host aggro
      list (summed across main + summon entries for the same character).
      `>= 0`; `0` is a valid edge — surfaces healers / tanks /
      aggro-skill users (hate-only on the aggro list), Party / CC
      teammates added by group extension (pure buffers), and `lastHit`
      KS-style final blows where prior damage came from others.

  POJO + Builder. No top-level damage-share-percent field — consumer trivially
  derives `damageDealt / sum(participants.damageDealt)` per event.

- [todo] R3. `nx-gs-adapter-api.kafka.events.raid.RaidDropItem` MUST ship as
  the per-drop record with:
    - `int itemId` — REQUIRED. L2 item template id.
    - `long count` — REQUIRED, `>= 1`. Stack quantity.
    - `@Nullable Integer enchantLevel` — only populated when the drop is an
      enchantable type (weapon / armor). `null` for adena, materials, etc.

  POJO + Builder. Drops are "what fell" — claim-tracking (who picked the item
  up) is intentionally NOT modelled in v1; a follow-up `RaidDropClaimedEvent`
  on the same family can pivot to multi-event when needed (matches the
  `privatestore` family's purchase + snapshot pattern).

- [todo] R4. `nx-gs-adapter-api.kafka.events.raid.RaidBossKind` enum MUST ship
  with three values — `RAID`, `GRAND_BOSS`, `INSTANCE_BOSS` — and Javadoc
  enumerating the host-side detection rule for each (open-world `isRaid()` →
  `RAID`; `instanceof GrandBossInstance` → `GRAND_BOSS`;
  `getReflection().isDefault() == false` → `INSTANCE_BOSS`). Order matters in
  the host detection cascade: `INSTANCE_BOSS` first, then `GRAND_BOSS`, then
  fall-through to `RAID`.

- [todo] R5. `nx-gs-adapter-api.spi.NxEvents` MUST accept `RaidKillEvent`
  through the single generic `void publish(Object event)` method (the
  per-family methods were collapsed in this slice — see Open questions). The
  game-loop-safety contract is unchanged: null event → silent WARN + drop,
  unregistered subtype → drop + WARN, family disabled (no topic configured) →
  drop + DEBUG, never blocks beyond the bounded-queue enqueue, never throws.

- [todo] R6. `nx-gs-adapter-core.events.EventTypeRegistry` MUST gain a binding
  for `RaidKillEvent`: family `"raid"`, message-type `"RaidKillEvent"`,
  partition-key extractor returning the boss NPC id (8-byte big-endian) — same
  per-boss raid kills land on one partition for ordered consumption.

- [todo] R7. `nx-gs-adapter-core.events.NxEventsImpl` MUST dispatch
  `RaidKillEvent` through the existing `publish(Object)` method — the
  per-family registration in R6 is the only addition needed.

- [todo] R8. `nx-tenants.api.rest.adapter.AdapterController#connect` MUST add
  a `"raid"` entry to `MessagingTopics.events` map, resolving to
  `<slug> + ".gs.events.raid"`. Adapter-side wire address comes from this
  field — no other config change.

- [todo] R9. The new Kafka topic `bohpts.gs.events.raid` MUST be documented
  in `nx-infra/komodo/l2nx/prod-kafka/tenants/bohpts.md` with 2 partitions,
  replication factor 1, retention 10_800_000 ms (3 hours) matching the
  platform-wide default for event topics. Long-term persistence is a
  consumer-side concern (TSDB / PostgreSQL / etc.); the Kafka topic only
  acts as a short-term replay buffer.

- [todo] R10. `bohpts-core` MUST extend `l2e.gameserver.l2nx.events.BohptsEventsModule`
  to bind a new `RaidKillPublisher` static facade alongside the existing
  publishers (`PremiumPurchasePublisher`, `ServerOnlineSnapshotPublisher`,
  `PrivateStorePurchasePublisher`, `PrivateStoreSnapshotPublisher`,
  `CharacterPresencePublisher`). No new `AdapterModule` ServiceLoader entry —
  rides the same `bohpts-events` module.

- [todo] R11. `bohpts-core` MUST ship a `RaidKillRecorder` singleton that:
    - Owns a `ConcurrentMap<Integer, RaidFightState>` keyed by `Attackable.getObjectId()`
      — state lazily-instantiated on the first `recordDrop` call for a given
      raid instance, GC'd after `recordKill` flushes.
    - Exposes `recordDrop(Attackable raid, int itemId, long count, int enchantLevel)`
      — called from `Attackable.rollRewards` per emitted reward, gated by the
      recorder on `isRaid() && !isRaidMinion()`.
    - Exposes `recordKill(Attackable raid, Creature killer)` — called from
      `Attackable.onDeath` at the end of the method (after `super.onDeath`
      which runs `calculateRewards` → `doItemDrop` → `rollRewards`). Snapshots
      the aggro list, builds the `RaidKillEvent` payload, and forwards to
      `RaidKillPublisher`.
    - Skips publication for raid minions and for raid kills where no `Playable`
      is on the aggro list (admin-killed; auto-spawn cleanup; etc.).
    - Defensive: any uncaught `Throwable` in `recordDrop` / `recordKill` is
      logged at ERROR (with stack trace) and swallowed. ERROR rather than
      DEBUG because these catch blocks fire only on truly anomalous failures
      (class-init failure, regression in the recorder itself) — operators
      MUST see them. The Attackable hook callsites add a second outer
      `try/catch (Throwable)` layer with the same ERROR-and-swallow contract,
      guaranteeing the game thread cannot be interrupted by any failure
      mode of the integration code.

- [todo] R12. `bohpts-core` MUST modify `Attackable`:
    - Inside `rollRewards`, after the per-reward `dropItem` / `doAutoLoot`
      call, invoke `RaidKillRecorder.getInstance().recordDrop(this, drop._itemId,
      drop._count, 0)` — `enchantLevel = 0` is a safe default; rolled raid
      drops are unenchanted in the stock L2 drop tables.
    - At the end of `onDeath(Creature killer)`, invoke
      `RaidKillRecorder.getInstance().recordKill(this, killer)`. The recorder
      itself filters on `isRaid() && !isRaidMinion()` and on `killer != null`.

**Should:**

- [todo] R13. `RaidKillRecorder` SHOULD additionally cover champion-template
  reward drops (`Attackable.doItemDrop` champion branch) — same `recordDrop`
  call site, gated on champion-on-raid edge case which is rare but exists.
  Phase-2 unless prioritized.

**Could:**

- [todo] R14. Fight-duration tracking (`engagedAt` + `durationMs`) — requires
  tapping the aggro-list first-damage moment. Add as nullable wire fields in a
  follow-up; absent in v1 because the existing aggro list does not record
  per-attacker first-hit timestamps without an additional touchpoint in
  `Attackable.addDamageHate`.

- [todo] R15. Per-participant healing-done / hits-landed / deaths-during-fight.
  Useful for raid post-mortems but requires deep instrumentation
  (healing tracker, packet-level hit counter). Out of scope for v1.

- [todo] R16. Drop claim tracking (`RaidDropClaimedEvent`) — who picked which
  item off the ground. Multi-event family pivot when prioritized.

- [todo] R17. Raid points awarded breakdown (per-clan reputation gain). Stock
  L2 raid-points distribution is computed at kill time; capturing here would
  duplicate `RaidBossPointsManager` logic. Defer.

**Non-goals:**

- **Permanent platform-side storage.** v1 ships the wire contract + producer.
  Consumer / persistence layer (PostgreSQL, TimescaleDB, ClickHouse —
  unselected) is a separate slice. Kafka topic retention (7d) is the
  short-term durability buffer; permanence is downstream.
- **Aggregated `damageByClan` / `damageByParty` wire rollups.** Consumer
  trivially aggregates from `participants` (`GROUP BY clanId / partyId`);
  redundant wire payload not justified at low event volume.
- **Adapter-side scheduler.** Raid kills are event-driven; no periodic tick.
- **Pre-kill events (spawn / engaged).** Stock `epicboss` analytics rail
  already covers START / END phase notifications via RabbitMQ; the L2NX
  raid family scope is the kill fact only. Phase-2 if user prioritizes.

### Edge cases

- **Last hit by NPC** (summon owned by a dead player, trap damage, kill curse,
  command-channel buff-bot bug). `killer` arg to `onDeath` is non-null but
  `killer.isPlayable()` is false. Recorder still emits the kill event because
  participants list is computed from the aggro list (which has the players who
  contributed); `lastHit` stays `null`. `dropOwner` may still resolve via
  `getTopDamager` over the aggro list.
- **Admin-killed raid.** GM `//kill` triggers `doDie` with `killer = gmPlayer`.
  Aggro list typically empty (no fight occurred). Recorder emits the kill
  event with `lastHit` populated from the GM (and `dropOwner` falling back to
  the same GM) but `participants = []`.
  Consumers can detect via `participants.isEmpty()` and flag as anomalous.
- **Raid despawn (no kill).** `onDespawn` clears aggro list; `onDeath` is NOT
  invoked. No kill event. By design.
- **Raid killed mid-disconnect.** Module's `onDisconnect` releases the
  `NxEvents` handle; subsequent `recordKill` calls into the recorder still
  build the payload but `RaidKillPublisher` becomes a no-op (`events` ref is
  null). Loss is acceptable — adapter disconnects are rare and visible in
  heartbeat.
- **Concurrent recorder state.** Two raid kills on different bosses can
  overlap in time. Recorder's per-boss `RaidFightState` is keyed by
  `Attackable.getObjectId()`; entries are isolated. State map cleanup happens
  inside `recordKill` after publish — exactly-once cleanup, no double-publish
  risk.
- **Recorder state leak via stale entry.** If a raid takes damage (calls
  `recordDrop`) but is never killed (despawn, server restart), the
  `RaidFightState` entry persists until JVM exit. Cap is implicit (raid count
  on a typical server is < 100 instances); GC after `onDespawn` is a Should-
  level improvement, not a v1 blocker.
- **Drop event recorded for non-raid.** Recorder's `recordDrop` performs an
  `isRaid() && !isRaidMinion()` gate at entry — no-op for regular monsters.
  Cheap branch on a hot path; profiled equivalent of the existing
  `if (isMonster() && getReflection().isDefault())` checks already in
  `doItemDrop`.
- **Player on aggro list but offline at kill time.** `info.getAttacker().getActingPlayer()`
  returns `null` when the player has logged out / disconnected mid-fight.
  Participant entry is skipped — the wire event records only currently-online
  contributors. Acceptable: offline players see no rewards either.

## Open questions

- [resolved: `NxEvents` collapsed from N per-family methods to one
  generic `publish(Object event)`. Runtime type routes via
  `EventTypeRegistry`. Per-family delivery semantics (partition key,
  ordering, family-disabled fallback) moved onto the concrete event DTO
  Javadoc + the registry. Breaking change for the api jar (host callsites
  in bohpts migrated in lock-step). This slice bumps api 0.27.0 → 0.28.0
  and core 0.18.0 → 0.19.0 over the last released tags
  (`api/v0.27.0`, `core/v0.18.0`).]
- [resolved: single event type `RaidKillEvent` (rejected abstract base for
  multi-event spawn/engaged/kill/despawn). KISS: only kill is wire-shipped in
  v1; if future events land they ride their own families or pivot the `raid`
  family to multi-event then.]
- [resolved: partition-key = `bossNpcId` big-endian (rejected `null` and
  `instanceId`). Per-boss history (e.g. "all Valakas kills") lands on one
  partition for ordered consumption.]
- [resolved: 7-day topic retention (rejected platform-default 3h). Raid
  kills are scarce and high-value; one Antharas kill per week MUST survive a
  consumer outage. Operator can shorten if storage is tight.]
- [resolved: enchant level = 0 for v1. Raw drops from L2 reward tables are
  unenchanted; the field stays nullable so the wire schema supports
  enchanted-drop variants without an api bump.]
- [assumed: drops captured only from `Attackable.rollRewards` main path
  (covers `RewardType.NORMAL` rewards). Champion / event / dynamic-event /
  VIP drops are rare on raids; capturing those is R13 (Should).]
- [resolved: `partyId` / `commandChannelId` are UUIDv7 minted by the host
  on Party / CommandChannel construction (single field + getter on each
  group class). Stable across leader changes within the same group instance;
  reset on disband / server restart. Replaces the earlier `"p-<leaderId>"` /
  `"cc-<leaderId>"` opaque-string scheme — cross-event analytics now reliably
  identifies the same group across kills.]
- [resolved: `lastHit` + `dropOwner` collapsed from 12 flat fields into two
  `@Nullable RaidActor` references on `RaidKillEvent`. `dropOwner` follows
  group-first semantics — `partyId` / `commandChannelId` are the canonical
  identity; the embedded `charId` is the L2 `mainDamageDealer` representative
  (unstable across kills of the same party — for narrative, not aggregation).
  Solo kills (no party) surface the owner directly via `dropOwner.charId`.]
- [resolved: `RaidParticipant` deleted; `RaidActor` absorbs `damageDealt` and
  serves all three actor-ref sites (lastHit / dropOwner / each participants
  entry). One source of truth for actor shape, no nesting on the wire.
  Char / clan names dropped from the wire — platform joins on
  `charId` / `clanId` against CDC catalogs (`bohpts.gs.sync.db.character`,
  `bohpts.gs.sync.db.clan`). `bossName` kept for now (no NPC CDC stream
  exists today; remove when a gamedata service lands).]

## Links

- Sibling reference (host-push publisher pattern + module wiring):
  [`docs/features/events-online-snapshot/spec.md`](../events-online-snapshot/spec.md)
- Legacy (decommissioned) rail emitting only START / END phase notifications
  with a single `bossId` and no damage / drop / participant detail:
  `bohpts-core/core/src/main/java/l2e/gameserver/feature/analytics/epicboss/EpicBossAnalyticsService.java`
    + `AnalyticsMapper.toEpicBossEventV1`. Not used at runtime any more; the new
      rail is a strict superset (does NOT coexist or replay legacy events).
