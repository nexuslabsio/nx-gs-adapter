# Death, Server-Status & Ratings events

> Owner: @n1rmata

Cross-repo feature. Wire contracts originate in `nx-gs-adapter` (this repo);
platform-side work spans `bohpts-core`, `nx-tenants`, `nx-gameservers`,
`nx-telegram`, and the Kafka topic runbooks in `nx-infra`. The feature is split
into three independently-shippable milestones (A, B, C); each adds a new outbound
event shape plus its platform consumers.

## Problem

Three gaps in the bohpts event pipeline, each reproducing or extending behaviour
that the legacy Telegram bot had:

1. **Unattended death** — players who leave a character fishing/farming unattended
   want to know the moment that character dies, the way the legacy bot pinged
   them. Today the only "your character is in trouble" signal is the involuntary
   *disconnect* notification (`events.character`, `logout_reason=disconnect`);
   an in-world death while autofarming or on an auto-macro produces no signal at
   all. bohpts-core already tracks both unattended modes (autofarm via
   `Player.getFarmSystem().isAutofarming()`, auto-macro sessions via
   `l2e.gameserver.instancemanager.AutoMacroManager`) but nothing leaves the JVM
   on death.

2. **Server lifecycle** — operators and players have no in-bot signal that a
   server came up or is going down. The platform only sees periodic population
   snapshots (`events.serveronline`) and a liveness heartbeat; there is no
   discrete "started" / "stopping" fact, no GM-only-startup awareness, and no
   place in the bot's information menu showing uptime / current online split.
   Maintenance restarts (the daily ~06:30 MSK restart) would spam a naive
   start/stop notifier twice a day.

3. **Fisher ratings** — bohpts-core runs a fishing championship with a live
   ranked leaderboard (`l2e.gameserver.instancemanager.games.FishingChampionship`),
   but the ranking never leaves the game server, so the platform cannot store,
   chart, or surface it. More ranked leaderboards (PvP, raids, …) are expected
   later, so the platform-side store must not be fishing-specific.

Audience: bohpts players (death + server-status notifications via the Telegram
bot), operators (server-status menu section), and platform-side consumers
(ratings store for future surfaces).

## Requirements

> Sibling feature carrying the wire dispatch plumbing — UNCHANGED by this spec:
> [`messaging`](008-messaging/spec.md) — `MessagingTopics.events.<family>` topic
> addressing, `Nx-Server-Id` connection-scoped header, `Nx-Message-Type`
> per-record header, UUIDv7 idempotency, host-pushed `NxEvents.publish(...)` +
> `EventTypeRegistry` binding pattern.

### Milestone A — Unattended-death notification (autofarm / auto-macro)

**Must:**

- [todo] R1. `nx-gs-adapter-api.kafka.events.character.CharacterDeathEvent` MUST
  ship as a new message type on the **existing `character` family** (joining
  `CharacterPresenceEvent`, which is UNCHANGED), making `character` a multi-event
  family dispatched by `Nx-Message-Type`. Fields:
    - `UUID eventId` — REQUIRED. UUIDv7; upper 48 bits encode the death
      occurrence timestamp. Platform consumers dedupe on this id.
    - `long charId` — REQUIRED. The character that died. Partition key
      (8-byte big-endian) so a character's presence + death history land in one
      partition in occurrence order, identical to `CharacterPresenceEvent`.
    - `@Nullable Map<String,String> metadata` — open string→string map carrying
      killer info under the `WellKnownDeathMetadata` keys: `killer_type`
      (a `WellKnownKillerTypes` value, R2), `killer_id` (the killer's char
      object-id for a `player` killer, or NPC template-id for a `monster`/`boss`
      killer; absent for `self`), and `farm_mode` (a `WellKnownFarmModes` value —
      `autofarm` / `auto_macro` — classifying the unattended mode the character
      was in; open string, absent when the host does not classify). No killer
      name on the wire — the platform resolves it from `killer_id` against its
      character / NPC catalogs.

  POJO + hand-written Builder + `equals`/`hashCode`/`toString` + Java-8 source,
  constructor parameter names preserved for Gson `-parameters` deserialization.
  No location field — deliberately omitted (see Non-goals).

- [todo] R2. `nx-gs-adapter-api.kafka.events.character.WellKnownKillerTypes` MUST
  ship a constants class (mirrors `WellKnownSiegeOutcomes`) enumerating canonical
  `killer_type` values as `lower_snake_case` open strings; non-exhaustive,
  consumers treat unknown values as opaque. Initial set:
  `MONSTER = "monster"`, `PLAYER = "player"`, `BOSS = "boss"`, `SELF = "self"`.
  Companion `WellKnownDeathMetadata` ships the `metadata` keys
  `KILLER_TYPE = "killer_type"` / `KILLER_ID = "killer_id"` /
  `FARM_MODE = "farm_mode"`; companion `WellKnownFarmModes` ships the canonical
  `farm_mode` values `AUTOFARM = "autofarm"` / `AUTO_MACRO = "auto_macro"`.
  Adding a constant is a non-breaking minor-version change.

- [todo] R3. `nx-gs-adapter-core.events.EventTypeRegistry` MUST register
  `CharacterDeathEvent` → family `"character"`, message-type
  `"CharacterDeathEvent"`, partition-key extractor returning `charId` as 8-byte
  big-endian (same extractor used for `CharacterPresenceEvent`). Host publishes
  through the existing generic `NxEvents.publish(Object)` — no new SPI method.

- [todo] R4. `bohpts-core` MUST publish a `CharacterDeathEvent` from the player
  death path **only when the dying character was unattended** at time of death —
  on autofarm (`Player.getFarmSystem().isAutofarming()`) or on an auto-macro
  session (`AutoMacroManager.isMacroActive`). Attended deaths emit nothing. A new
  `l2e.gameserver.l2nx.events.character.CharacterDeathPublisher` is bound in
  `BohptsEventsModule.onConnect` / released in `onDisconnect`, alongside the
  existing `CharacterPresencePublisher`. Killer type + killer id are resolved on
  the game thread and written into `metadata` (`killer_type` from
  `WellKnownKillerTypes`; `killer_id` = char object-id for a player killer, NPC
  template-id for a monster/boss killer), along with `farm_mode` classifying the
  unattended mode (`autofarm` wins when both modes are active simultaneously).
  Any uncaught `Throwable` is logged at DEBUG and swallowed (game-loop safety,
  identical to the other publishers).

- [todo] R5. `nx-gameservers` `CharacterPresenceEventConsumer` (topic
  `*.gs.events.character`) MUST dispatch on `Nx-Message-Type` and **skip**
  `CharacterDeathEvent` records (no-op, not an error) so the new message type
  does not break the existing presence ingest. nx-gameservers stores nothing for
  deaths in this milestone (Non-goal: death history).

- [todo] R6. `nx-telegram` MUST deliver a death notification to the linked
  character's owners, gated by a new per-linked-character toggle:
    - New `LinkedCharacterNotificationType.DEATH`.
    - The character-family consumer dispatches on `Nx-Message-Type`; on
      `CharacterDeathEvent` it resolves subscribers with DEATH enabled and sends
      a localized message, reusing the existing `NotificationDispatcher` dedup
      (`tg_notification_dedup`, keyed by `eventId`).
    - DEATH toggle added to the character-notifications menu (the same place
      LOGIN / DISCONNECT / MAIL / PRIVATE_STORE_SALE are toggled).
    - The consumer resolves the killer NAME from `metadata.killer_id`:
      `killer_type=player` → char name via `CharacterRepository`; `monster`/`boss`
      → NPC name via the wiki npc-name resolver (per subscriber language, inside
      the render function). `notifications.yml` ru + en + uk templates; PvP/PvE
      line when the killer resolves, HTML-escaped; killer-less generic otherwise.
    - The message template is selected per `metadata.farm_mode`: `autofarm` /
      `auto_macro` pick a mode-specific headline ("Character X died on
      autofarm" / "…on auto-macro"); absent / unknown mode falls back to the
      mode-less headline. The dying character's name rides in the headline (it
      must survive the Telegram push preview), so the footer carries only the
      server name.

### Milestone B — Server-status section & start/stop notifications

**Must:**

- [todo] R7. `nx-gs-adapter-api` MUST add two new message types to the **existing
  `serveronline` family** (joining `ServerOnlineSnapshotEvent`, UNCHANGED),
  making it a multi-event family dispatched by `Nx-Message-Type`:
    - `ServerStartedEvent` — `UUID eventId` (REQUIRED, UUIDv7) +
      `@Nullable Map<String,String> metadata` carrying `gm_only` ("true"/"false")
      under the `WellKnownServerStartMetadata.GM_ONLY` key.
    - `ServerStoppingEvent` — `UUID eventId` (REQUIRED, UUIDv7) +
      `@Nullable Map<String,String> metadata` carrying the same `gm_only` key
      (`WellKnownServerStartMetadata.GM_ONLY`). No stop-reason on the wire. The
      host always reports `gm_only`; the platform (not the host) decides to mute
      the stop notification for GM-only runs.

  Both POJOs follow the api conventions (builder, Java-8, `-parameters`).
  Partition key `null` (round-robin); consumers group by `Nx-Server-Id` and order
  by the UUIDv7 `eventId` timestamp.

- [todo] R8. `nx-gs-adapter-core.events.EventTypeRegistry` MUST register
  `ServerStartedEvent` and `ServerStoppingEvent` → family `"serveronline"`,
  message-types `"ServerStartedEvent"` / `"ServerStoppingEvent"`, partition-key
  extractor returning `null`.

- [todo] R9. `bohpts-core` MUST emit `ServerStartedEvent` once the world is fully
  loaded, stamping `metadata.gm_only = Config.SERVER_GMONLY`, and
  `ServerStoppingEvent` on the graceful-shutdown path (`Shutdown.run`). Wiring
  lives in `BohptsEventsModule`.

- [todo] R10. `bohpts-core` MUST suppress emission of **both**
  `ServerStartedEvent` and `ServerStoppingEvent` during the daily maintenance
  restart window, **hard-coded to 06:00–07:00 Europe/Moscow** (server time) in
  `ServerLifecyclePublisher`. The platform applies no restart-related logic.
  Outside the window, start/stop emit normally.

- [todo] R11. `nx-gameservers` `ServerOnlineEventConsumer` MUST dispatch on
  `Nx-Message-Type`:
    - `ServerOnlineSnapshotEvent` → existing online-bucket ingest, UNCHANGED.
    - `ServerStartedEvent` → upsert a new `gs_server_status` row
      (`started_at` from `eventId`, `gm_only` read from `metadata`,
      `status='online'`).
    - `ServerStoppingEvent` → update `gs_server_status` (`status='stopping'`,
      `stopped_at`).

  New Liquibase migration creates `gs_server_status`
  (PK `(tenant_id, server_id)`), watermark-gated by `eventId` timestamp so
  out-of-order/replayed records do not regress state.

- [todo] R12. `nx-telegram` MUST add a **"Статус сервера"** section to the
  information menu (`InfoMenuHandler` / `InfoMenuRenderer` hub), mirroring the
  existing Events / Bosses / Castles sections:
    - Display: 🟢/🔴 status, uptime (`now − started_at`), and current online by
      bucket (`total` / `unique` / `offline_trade` / `fishing`).
    - Data read directly from the shared `nexus` DB via new read-only adapters:
      `gs_server_status` (status / started_at) + the existing online-bucket
      table(s) populated by `ServerOnlineIngestor`.
    - A 🔔/🔕 toggle for START/STOP notifications, persisted via the existing
      `ActivitySubscriptionService` under a new `ActivityType.SERVER_STATUS`
      (server-scoped `activity_key`, e.g. the serverId).
    - Gated by a new `MenuFeatures` flag `info-server` (default OFF) — see R26.

- [todo] R13. `nx-telegram` MUST deliver server START/STOP notifications via a
  **new event-driven path** (the existing time-driven `ActivityNotificationScheduler`
  does not fit push events):
    - A new Kafka consumer on the `*.gs.events.serveronline` family filtering
      `ServerStartedEvent` / `ServerStoppingEvent` (ignoring snapshots), fanning
      out to `SERVER_STATUS` subscribers of that server.
    - **Both STARTED and STOPPING notifications MUST be suppressed when
      `gmOnly = true`** (read from the event `metadata`, regardless of the
      subscriber's toggle) — GM-only runs are operator tests with frequent
      restarts. The filter lives on the platform (telegram), not in the host:
      bohpts always emits both facts with `gm_only` stamped.
    - Dedup by `eventId` (reuse the notification dedup machinery).
    - `info.yml` ru/en/uk templates for server up / server down (legacy copy:
      start = "update the client" reminder, stop = "23 техника устанавливают
      обновления").

- [todo] R26. The whole server-status feature MUST be gated by a new per-bot
  `MenuFeatures` flag `info-server`, **default OFF**. It gates all three entry
  points: the "📡 Статус сервера" hub button, the SS/TS section callbacks
  (bounce when off), and the event-driven START/STOP notifier (skip when off).
  Implemented as a flat sibling flag (config key `info-server`), not a nested
  `info.server`, so the live `bots.yml` `info: true` binding is not broken and
  no config-shape migration is required. Ships to prod OFF; flipped to `true`
  after review.

### Milestone C — Fisher ratings sync

> ⚠️ **SUPERSEDED (2026-06-17).** This events-based `ratings` family (`RatingSnapshotEvent` /
> `RatingEntry` under `kafka.events.ratings`) has been **removed**. Ratings now sync per-character via
> the db-sync CDC stream — `kafka.sync.db.rating.RatingDbDto` on `<tenant>.gs.sync.db.rating` — with
> rank computed at read time. See the generic-ratings design spec in
> `nx-gameservers/docs/specs/007-generic-ratings-sync-design.md`. The milestone
> below is kept for history only; the DTOs it references no longer exist.

**Must:**

- [todo] R14. `nx-gs-adapter-api` MUST ship a **new `ratings` family** under
  `kafka.events.ratings`:
    - `RatingSnapshotEvent` — `UUID eventId` (REQUIRED, UUIDv7),
      `String ratingType` (REQUIRED, open string), `List<RatingEntry> entries`
      (REQUIRED, full ranked snapshot).
    - `RatingEntry` — `long charId`, `long score`, `int rank`. No character name
      (platform joins `gs_characters`).
    - `WellKnownRatingTypes` constants class: `FISHING = "fishing"` (the first
      ranked leaderboard; future rating types add constants here without an api
      release).

  POJOs follow api conventions (builder, Java-8, `-parameters`). Partition key
  `null` (round-robin); full-snapshot semantics, ordered per-server by the
  UUIDv7 `eventId` timestamp.

- [todo] R15. `nx-gs-adapter-core.events.EventTypeRegistry` MUST register
  `RatingSnapshotEvent` → family `"rating"`, message-type
  `"RatingSnapshotEvent"`, partition-key extractor returning `null`.

- [todo] R16. `nx-tenants` `AdapterController.connect()` MUST add
  `"rating" → "{slug}.gs.events.rating"` to the `messagingTopics.events` map,
  so `/connect` advertises the new family to adapters. This is the only change
  that requires a `/connect` contract addition in this spec (Milestones A and B
  reuse existing families). Wire family / topic is singular `rating` (the Java
  package is `events.ratings`).

- [todo] R17. `nx-infra` MUST provision the `rating` topic in **both** places:
    - Add `"gs.events.rating"` to the `STANDARD_TOPICS` array in
      `komodo/l2nx/prod-kafka/scripts/create-tenant.sh` so new tenants get it on
      creation.
    - Document the `bohpts.gs.events.rating` topic in
      `komodo/l2nx/prod-kafka/tenants/bohpts.md` (prod: 2 partitions, 3h
      retention) and `komodo/l2nx/dev-kafka/tenants/test1.md` (dev: 1 partition,
      1h retention), each with the `docker exec … kafka-topics --create` runbook
      command, matching the existing per-family sections.

- [todo] R18. `bohpts-core` MUST publish a `RatingSnapshotEvent` every 1 minute
  via a new `RatingSnapshotPublisher` (scheduled `scheduleAtFixedDelay`, bound in
  `BohptsEventsModule`), reading `FishingChampionship.snapshotCurrentTop(1000)`,
  mapping each `SnapshotEntry{rank, charId, name, points}` → `RatingEntry`
  (dropping `name`, `points` → `score`), publishing with
  `ratingType = WellKnownRatingTypes.FISHING`. Fewer than 1000 active fishers
  yields a shorter list. Game-loop safety identical to the other publishers.

- [todo] R19. `nx-gameservers` MUST add a `rating` domain slice with a new
  `RatingsEventConsumer` (group `nx-gameservers-events-ratings`) that scope-
  replaces a **single unified** `gs_ratings` table on each snapshot:
    - Columns: `tenant_id`, `server_id`, `rating_type`, `char_id`, `score`,
      `rank`, `updated_at`. PK `(tenant_id, server_id, rating_type, char_id)`.
    - Replace is scoped to `(tenant_id, server_id, rating_type)` and
      watermark-gated by the `eventId` timestamp (stale snapshots ignored).
    - Character names resolved by JOIN to `gs_characters` on read — never stored
      in `gs_ratings` (consistent with the shared-DB no-snapshot convention).
    - New Liquibase migration creates `gs_ratings`.

### Milestone D — Private-store current-adena in trade notification

**Must:**

- [todo] R23. `nx-gs-adapter-api` MUST ship `WellKnownPrivateStoreMetadata` in
  `events.privatestore` with key `STORE_OWNER_ADENA = "store_owner_adena"`. The
  existing `PrivateStorePurchaseEvent.metadata` map carries it; no new field.

- [todo] R24. `bohpts-core` `PrivateStorePurchasePublisher` MUST stamp
  `metadata.store_owner_adena` = the **store-opener's** adena balance after the
  deal closed (decimal string). Store-opener = `ASK ? seller : buyer` (ASK = sell
  store opened by the seller; BID = buy store opened by the buyer) — i.e. the
  notification recipient. Read via `Player.getAdena()` at publish time.

- [todo] R25. `nx-telegram` `PrivateStoreEventsConsumer` / formatter MUST read
  `metadata.store_owner_adena` and append a "current adena" line to the trade
  notification (the recipient's balance now, since CDC character sync lags).
  ru/en/uk keys; line omitted gracefully when the key is absent.

**Should:**

- [todo] R20. Death-notification template SHOULD distinguish PvP (`player`) from
  PvE (`monster`/`boss`) killers when `killerType` is present, for a richer
  message; absent type falls back to a generic death line.

**Could:**

- [todo] R21. Per-character death *history* storage in nx-gameservers (a
  `gs_character_deaths` table) — useful for analytics, out of scope here.
- [todo] R22. Additional rating types (PvP, raid contribution) on the same
  `ratings` family / `gs_ratings` table — the schema is built for it; no further
  api changes needed beyond a new `WellKnownRatingTypes` constant.

**Non-goals:**

- **Death location / coordinates.** Deliberately omitted from
  `CharacterDeathEvent` — the notification value is "your fishing character
  died", not where. Adding a location field later is a non-breaking change.
- **Emitting attended deaths.** The adapter emits only unattended deaths
  (autofarm / auto-macro; lowest volume, exact legacy-bot behaviour). Emitting
  all deaths with a flag is a rejected alternative.
- **New `/connect` families for death or server-status.** Both ride existing
  families (`character`, `serveronline`) as new message types — no `/connect`
  change. Only `ratings` is a new family.
- **Platform-side restart-window logic.** The maintenance-window suppression is
  entirely bohpts-core's responsibility (R10). The platform never special-cases
  restart times.
- **Server-status broadcast to a fixed ops channel.** Server start/stop
  notifications are per-user opt-in via the menu toggle, not a channel broadcast.

### Edge cases

- **Autofarm death during reconnect.** The `NxEvents` façade has stable identity
  across reconnect; a death event built before reconnect and published after
  lands on the freshly-reconnected producer (existing AtomicReference swap).
- **Hard crash (no graceful shutdown).** `ServerStoppingEvent` is not emitted;
  liveness falls to the existing heartbeat-timeout mechanism. The bot's
  STOP notification therefore covers graceful shutdowns only — acceptable, as a
  crashed server has no JVM to emit anything.
- **GM-only startup.** `ServerStartedEvent` is still emitted (with `gmOnly=true`)
  and still upserts `gs_server_status` so the menu shows accurate status/uptime;
  only the bot's *notification* is suppressed.
- **Start/stop inside the maintenance window.** No events emitted at all, so the
  menu's `status` may briefly lag reality during the daily restart — acceptable;
  the next snapshot/heartbeat reconciles.
- **Fewer than 1000 fishers / empty leaderboard.** Snapshot carries a shorter
  (possibly empty) `entries` list; nx-gameservers scope-replace empties the
  `(server, fishing)` slice — no stale rows linger.
- **Ratings replay / out-of-order snapshot.** Watermark gate on `eventId`
  timestamp discards a snapshot older than the last applied one.
- **`FishingChampionship` cache size.** `snapshotCurrentTop` caches only the top
  100 (`_cachedTop100`); requesting 1000 forces a rebuild from `_current`
  (a `values()` copy + partial sort) each tick. Bounded and acceptable at the
  1-minute cadence.
- **Death event on the `character` topic seen by nx-gameservers.** Must be
  skipped, not parsed as a presence event (R5) — otherwise the presence ingest
  would error on the unknown shape.

## Open questions

- [resolved: adapter emits only unattended deaths (autofarm / auto-macro,
  classified by `metadata.farm_mode`) — user confirmed; lowest volume, matches
  legacy bot.]
- [resolved: death + server-status ride existing `character` / `serveronline`
  families as new message types; only `ratings` is a new family — user
  confirmed.]
- [resolved: GM_ONLY carried in `ServerStartedEvent.metadata` (`gm_only`), bot
  suppresses the start notification; maintenance restart suppressed at source by
  bohpts-core, window hard-coded 06:00–07:00 MSK (no config knob) — user
  confirmed.]
- [resolved: killer info moved off typed fields into `CharacterDeathEvent.metadata`
  (`killer_type` + `killer_id`); platform resolves the killer name from the id;
  killer types trimmed to monster/player/boss/self (raid→boss, siege removed) —
  user confirmed.]
- [resolved: private-store trade notifications carry the store-opener's post-deal
  adena in `PrivateStorePurchaseEvent.metadata` (`store_owner_adena`) so the bot
  can show current balance ahead of the lagging CDC sync — user requested
  (Milestone D).]
- [resolved: server start/stop notification is a per-user toggle inside a new
  "Статус сервера" info-menu section (uptime + online buckets), mirroring
  Events/Castles/Bosses — user confirmed.]
- [resolved: ratings is a unified `gs_ratings` table with a `rating_type`
  discriminator (open string, `WellKnownRatingTypes`), not a table per rating —
  user confirmed.]
- [resolved: `RatingEntry` carries no `charName`; source stamps `rank`; top 1000;
  `ratingType` value is `"fishing"` (`WellKnownRatingTypes.FISHING`) — user
  confirmed.]
- [resolved: GM-only source is `Config.SERVER_GMONLY`; fisher leaderboard source
  is `FishingChampionship.snapshotCurrentTop(int)` — confirmed in bohpts-core.]
- [resolved: `ratings` goes in BOTH `create-tenant.sh` `STANDARD_TOPICS` AND the
  per-tenant `.md` runbook docs — user confirmed.]
- [ ] `gs_ratings` `score` column type: `BIGINT` assumed (championship `points`
  is a `long`); confirm during planning.

## Links

- Sibling feature (events runtime + per-family fanout):
  [`docs/specs/008-messaging/spec.md`](008-messaging/spec.md)
- Reference family + publisher pattern (snapshot-style, host-pushed):
  [`docs/specs/011-events-online-snapshot/spec.md`](011-events-online-snapshot/spec.md)
- Reference family + publisher pattern (multi-event family, discrete fact +
  snapshot): [`docs/specs/014-events-raid.md`](014-events-raid.md)
- bohpts-core sources: `l2e.gameserver.Config.SERVER_GMONLY`,
  `l2e.gameserver.instancemanager.AutoFarmManager`,
  `l2e.gameserver.instancemanager.games.FishingChampionship`,
  `l2e.gameserver.l2nx.events.BohptsEventsModule`
- nx-tenants `/connect`:
  `app.l2nx.tenants.api.rest.adapter.AdapterController.connect()`
- Kafka topic runbooks: `nx-infra/komodo/l2nx/{prod,dev}-kafka/tenants/`
  </content>
  </invoke>
