# nx-gs-adapter-api

Subproject of the `nx-gs-adapter` monorepo. See [`../CLAUDE.md`](../CLAUDE.md) for repo-wide
conventions (per-module slash-namespaced versioning, Maven Central publishing flow, license,
shared `:nx-gs-log`).

## Purpose

Contracts-only artifact: pure Java interfaces and POJOs that define the wire shape exchanged
by the L2NX game-server adapter and its consumers. Published as
`app.l2nx:nx-gs-adapter-api`. Consumed by:

- `:nx-gs-adapter-core` (sibling subproject) — adapter-side producer of `ConnectRequest` /
  consumer of `ConnectResponse` / `HeartbeatMessage`
- `nx-tenants` (separate repo, composite-includes this monorepo) — platform-side handler of
  `POST /api/tenants/servers/connect`

## Package layout

- `app.l2nx.gs.adapter.api.rest` — REST request/response DTOs (`ConnectRequest`,
  `ConnectResponse`, `LoginServerConnectResponse`, `KafkaCredentials`, `SyncTopics`,
  `MessagingTopics`)
- `app.l2nx.gs.adapter.api.kafka` — Kafka message payloads + header contract
  (`HeartbeatEvent`, `NxHeaders`)
- `app.l2nx.gs.adapter.api.kafka.sync.db.<entity>` — per-entity wire DTOs for
  the db-sync stream. Shipped entities: `character` (`CharacterDbDto`,
  `CharacterSubclassDbDto`, `CharacterInstanceCooldownDbDto`,
  `CharacterLockDbDto`; CharacterDbDto carries optional `accountName`,
  `nobless`, `scheduledDeletionAt` on top of the identity / progression set, all
  three from generic L2J columns — see
  `docs/features/character-core-extension/`; plus optional `gearScore: Integer`
  — the active-class gear score, a snapshot at last character store, `null` when
  the build computes no gear score; plus optional `fame: Long` — character fame
  (reputation) points, `null` when the source build reports none; plus optional
  `accessLevel: String` — opaque
  GM/access level, numeric text on int-based builds (`"7"`) or role name on
  string-role builds, `null` when not surfaced; plus optional
  `locks: List<CharacterLockDbDto>` — one entry per active character lock
  (`lockType` ∈ `WellKnownCharacterLockTypes` = IP / HWID / ITEM, `lockValue?`),
  derived from build-specific `character_variables`, `null` when not synced),
  `clan` (`ClanDbDto` +
  `ClanSkillDbDto`; ClanDbDto carries optional `icon: byte[]` for the clan crest
  as decoded PNG bytes — schema providers do the source-format → PNG
  conversion in `mapEntity`), `alliance` (`AllianceDbDto{allyId, allyName,
  icon: byte[]}` — same icon convention as clan), `item` (`ItemDbDto`,
  `ItemAttributeDbDto`), `rating`
  (`kafka.sync.db.rating.RatingDbDto{ratingType, season?, charId, points,
  metadata?}` + `WellKnownRatingTypes` — `lower_snake_case` open-string rating
  types, first `fishing`; one unified topic carries every rating kind
  discriminated by `ratingType`, rank is NOT on the wire — consumers window it at
  read time). CharacterRuntimeDto (`kafka.sync.runtime.character`)
  carries an optional `online: Boolean` presence marker — wire convention:
  `null`/omitted = ONLINE (byte-budget default), explicit `false` = one-shot
  OFFLINE tombstone, explicit `true` allowed but redundant. Consumers MUST
  treat omitted / `null` as `true` for back-compat. CharacterRuntimeDto also
  carries an optional `exp: Long` — the character's current raw (absolute,
  cumulative) experience total. Volatile runtime state, so it rides the runtime
  channel; `null` on cores that don't expose it and on offline tombstones.
  Consumers combine it with the per-server level→exp table (see
  `events.leveldata` below) to compute "% progress within the current level".
- `app.l2nx.gs.adapter.api.kafka.sync.gd.<entity>` — per-entity wire DTOs for the
  gd-sync (static game-data) stream, payload of `GameDataSyncEvent<T>`. Gear-score
  additions (build-agnostic; `null` when the build computes no gear score):
  `itemtemplate.ItemTemplate` carries optional `gearScore: Integer` (item base
  contribution) + `gearScoreEnchantProfile: String` — an open `UPPER_SNAKE` profile
  key (canonical constants in `WellKnownGearScoreEnchantProfiles`:
  `WEAPON` / `NONWEAPON` / `SPECIAL`; build-defined, null when absent), a reference
  into the ruleset's `ENCHANT_PROFILE` group (rule `key` == this value), not an
  inline table; `skill.Skill` carries optional `gearScoreContributions:
  List<GearScoreContribution>` where `GearScoreContribution{kind, value, classIds?}`
  has `kind` in `UPPER_SNAKE` (`OWNED` / `PER_LEVEL` / `ENCHANT`) and `classIds`
  null = all classes. New singleton entity `gearscore` —
  `GearScoreRuleset{enabled, groups: List<GearScoreRuleGroup>}`,
  `GearScoreRuleGroup{category (UPPER_SNAKE: LEVEL | ATTRIBUTE | AUGMENT |
  ENCHANT_PROFILE | SET_BONUS | AURA | ACHIEVEMENT | SKILL), label: LocalizedText,
  description?, rules: List<GearScoreRule>}`, `GearScoreRule{key, label?, value?,
  unit? (UPPER_SNAKE: PER_POINT | PERCENT | FLAT | PER_LEVEL | PER_STEP), cap?,
  scaling?: List<GearScoreScalingStep>}`, `GearScoreScalingStep{from, to?, value}`.
  The wiki renders the ruleset groups as tables; per-entity values
  (`ItemTemplate.gearScore`, `Skill.gearScoreContributions`) are the live numbers.
- `app.l2nx.gs.adapter.api.kafka.events.<family>` — outbound discrete-fact event
  DTOs grouped by family. Single-event families take the concrete type on the
  publish method directly; multi-event families bind on an abstract base and
  dispatch on the platform via the `Nx-Message-Type` header. Host code
  publishes through a single generic `NxEvents.publish(Object event)` method;
  the adapter-core `EventTypeRegistry` routes by the runtime class of the
  payload (family + partition-key extractor + `Nx-Message-Type` header value).
  Adding a new event type means appending one `register(...)` call there —
  the SPI surface stays one method regardless of how many families ship.
  Shipped families:
    - `events.premiumpurchase` — `PremiumPurchaseEvent` (final) +
      `PurchaseItem` / `PurchaseService` / `Payment` + `WellKnownServices`
      constants. Single-event family; per-fact, host-pushed via
      `NxEvents.publish(...)`. Partition key: `characterId`.
    - `events.serveronline` — `ServerOnlineSnapshotEvent` (final, UUIDv7
      `eventId` + open `Map<String, Long> buckets`) +
      `WellKnownServerOnlineBuckets` lower_snake_case constants split into
      required (`total` — full character presence, `unique` — distinct
      active humans by host-defined identity tuple) and optional canonical
      (`offline_trade`, `fishing`); hosts MAY publish arbitrary
      non-canonical keys. Periodic snapshots, host-pushed via
      `NxEvents.publish(...)` on a host-managed cadence. Partition key:
      `null` (round-robin). Multi-event family — also carries the discrete
      server-lifecycle facts `ServerStartedEvent` (UUIDv7 `eventId` + open
      `metadata`; canonical keys `gm_only` / `auto_restart` via
      `WellKnownServerStartMetadata`) and `ServerStoppingEvent` (UUIDv7 `eventId`
        + open `metadata`; same keys — graceful-shutdown signal, no stop-reason on
          the wire), dispatched by `Nx-Message-Type`. The host always reports both
          keys; the platform mutes its "server is up" / "server is stopping"
          notification on a GM-only run (operator tests) or an automatic scheduled
          restart (`auto_restart=true` — the host tags its daily maintenance restart
          and keeps emitting the fact so the platform still persists it). Both
          lifecycle facts use partition key `null`.
    - `events.character` — `CharacterPresenceEvent` (one event per login /
      logout, distinguished by the `online: boolean` field — `true`=login,
      `false`=logout). Carries UUIDv7 `eventId`
      (REQUIRED, derive `occurredAt`), `charId` (REQUIRED),
      `online` (REQUIRED), optional `accountName` / `ip` / `hwid`.
      Partitioned by `charId` so per-character presence history lands in
      one partition in occurrence order. One of three sources feeding
      `gs_characters.online` on the platform (others: CDC
      `CharacterDbDto.online`, runtime `CharacterRuntimeDto.online`);
      timestamp-based last-writer-wins on the consumer. Multi-event family
      — also carries `CharacterDeathEvent` (UUIDv7 `eventId` + `charId`
      partition key + open `metadata`), dispatched by `Nx-Message-Type`.
      Killer info rides `metadata` (`WellKnownDeathMetadata`): `killer_type`
      (a `WellKnownKillerTypes` value — `monster` / `player` / `boss` /
      `self`), `killer_id` (the killer's char object-id for `player`, NPC
      template-id for `monster` / `boss`), and `farm_mode` (a `WellKnownFarmModes`
      value — `autofarm` / `auto_macro` — classifying the unattended mode); the
      platform resolves the killer name from the id, no name on the wire. bohpts
      emits a death event only when the dying character was unattended — on
      autofarm or on an auto-macro (legacy-bot "your unattended character died"
      signal); no location on the wire. This family ALSO carries
      `LevelExpTableSnapshotEvent` (Java package `events.leveldata`; see below) —
      the per-server level→exp table, dispatched by `Nx-Message-Type` on this same
      topic rather than a dedicated one.
    - `events.privatestore` — `PrivateStorePurchaseEvent` (closed-deal facts)
        + `PrivateStoreSnapshotEvent` (per-`(itemId, side)` order book) +
          `TradeLine` / `Offer` line types + `PrivateStoreSide` enum +
          `WellKnownElements` constants. Multi-event family (no abstract base);
          both subtypes ride one topic, host-pushed via `NxEvents.publish(...)`
          with the concrete subtype. Partition keys: snapshot → `itemId`,
          purchase → `null` (round-robin, no single natural per-entity key).
    - `events.raid` — `RaidKillEvent` (final) + `RaidActor` /
      `RaidDropItem` sub-DTOs + `RaidBossKind` enum (`RAID` /
      `EPIC` / `INSTANCE_BOSS`). Multi-event family (kill fact +
      boss-respawn snapshot, see below); one `RaidKillEvent` per
      `Attackable.isRaid() && !isRaidMinion()` death. Carries UUIDv7
      `eventId` (REQUIRED, derive `occurredAt`), `bossNpcId` (REQUIRED,
      partition key as 8-byte big-endian), `bossKind` (REQUIRED), boss
      identity (incl. `bossName` until an NPC CDC stream exists), two
      `@Nullable RaidActor` refs (`lastHit` — final-blow character;
      `dropOwner` — L2 `mainDamageDealer` with group-first semantics:
      `partyId` / `commandChannelId` are canonical, `charId` is the
      unstable representative for narrative only), `participants`
      (`List<RaidActor>` — damage breakdown from aggro list), `drops`.
      `RaidActor` carries `charId` + affiliation ids + `damageDealt`;
      char / clan names are NOT included — platform joins on the
      character / clan CDC streams. Party / CC identities are
      `@Nullable UUID` (UUIDv7 minted by the host on group construction,
      stable across leader changes within the same group instance, reset
      on disband / restart). Topic retention 3h (platform default for
      event topics; long-term persistence is consumer-side).
      Second message type in this family: `BossRespawnSnapshotEvent`
      (final, UUIDv7 `eventId` + `List<BossRespawnEntry> bosses` +
      `WellKnownBossStatuses` constants). Periodic FULL snapshot of every
      tracked raid boss (grand / epic + open-world), host-pushed via
      `NxEvents.publish(...)` on the same `raid` topic (dispatched by
      `Nx-Message-Type`). Each `BossRespawnEntry` carries `npcId` (REQUIRED —
      platform resolves the boss name from this id; names are NOT on the wire),
      `level?`, `kind` (reuses `RaidBossKind` — only `RAID` / `EPIC`
      emitted), `status` (REQUIRED, open string; canonical
      `alive` / `in_combat` / `dead` via `WellKnownBossStatuses`),
      `nextRespawnAt?` (`Instant`, set when dead + known), and an open
      `metadata` map. Partition key: `null` (round-robin; `RaidKillEvent`
      keeps its `bossNpcId` key — per-type keys like `privatestore`). Platform
      keeps last-known per server (mark-and-sweep) and counts the respawn down
      locally.
    - `events.gameevents` — `GameEventSnapshotEvent` (final, UUIDv7
      `eventId` + `List<GameEventEntry> events`) + `WellKnownGameEventMetadata`
      constants. Single-event family; periodic FULL snapshot of every
      configured recurring event (TvT and others), host-pushed via
      `NxEvents.publish(...)`. Each `GameEventEntry` carries `code`
      (REQUIRED, stable build-agnostic id), `name?`, `enabled` (REQUIRED),
      `status?` (open lifecycle phase — `WellKnownGameEventStatuses`:
      `waiting` / `registration` / `in_progress` mapped from the engine state
      machine; replaced the former boolean `running`), `nextStartAt?` (`Instant`),
      and an open `metadata` map whose canonical keys today are `event_kind=tvt`
      and `event_kind=solo_boss` (`WellKnownGameEventMetadata`). Partition key:
      `null` (round-robin).
      Build-agnostic — TvT is one mapping, not a contract assumption.
    - `events.castle` — `CastleSnapshotEvent` (final, UUIDv7 `eventId` +
      `List<CastleSnapshotEntry> castles`) + `SiegeFinishedEvent` (final) +
      `WellKnownSiegeOutcomes` constants. Multi-event family (snapshot +
      discrete fact, like `raid`); both ride one `castle` topic, dispatched
      by `Nx-Message-Type`. `CastleSnapshotEvent` is a periodic FULL snapshot
      of every castle — each `CastleSnapshotEntry` carries `castleId`
      (REQUIRED), `name?`, `ownerClanId?` (host maps no-owner sentinel → null),
      `nextSiegeAt?` (`Instant`), `registrationEndsAt?` / `siegeEndsAt?`
      (`Instant`, next siege's registration-close and end, derived host-side from
      the castle template), open `metadata`. Partition key: `null`
      (round-robin). `SiegeFinishedEvent` is one per ended siege: `eventId`
      (UUIDv7, REQUIRED), `castleId` (REQUIRED, partition key 8-byte BE),
      `castleName?`, `siegeStartedAt?`, `outcome` (REQUIRED, open string;
      canonical `captured` / `defended` / `draw` via `WellKnownSiegeOutcomes`),
      `winnerClanId?` (post-siege holder; null on draw),
      `attackerClanIds` / `defenderClanIds` (registered clans), open `metadata`.
      Build-agnostic — outcome is an open string, not a contract assumption.
    - `events.leveldata` — `LevelExpTableSnapshotEvent` (final, UUIDv7 `eventId`
        + `List<LevelExpEntry> levels` + optional open `Map<String,String>
      metadata`) + `LevelExpEntry` (`int level` + `long requiredExp` — the
          absolute / cumulative exp required to be at that level). The Java package
          is `events.leveldata`, but the event RIDES the `character` family/topic
          (`<tenant>.gs.events.character`), dispatched by `Nx-Message-Type` — the
          level table is synced once on server start / datapack reload, not worth its
          own topic/consumer/group. Periodic FULL
          snapshot of the server's level→required-exp progression table, host-pushed
          via `NxEvents.publish(...)` (bohpts emits it on server startup + datapack
          reload, reading L2J `ExperienceData`). Mirrors `BossRespawnSnapshotEvent`
          exactly (hand-written immutable + builder + getters, Gson-friendly,
          JSpecify `@Nullable`). Partition key: `null` (round-robin); platform
          scope-replaces per `(server)` keeping the newest snapshot. Combined with
          `CharacterRuntimeDto.exp` to compute "% progress within current level":
          `pct = (exp - requiredExp[level]) / (requiredExp[level + 1] - requiredExp[level])`.
- `app.l2nx.gs.adapter.api.kafka.commands` — inbound command marker `NxCommand`,
  reply envelope `CommandResult<R>`, structured `ErrorCode` enum. Concrete
  command DTOs ship under `kafka.commands.<group>.*` (group = code-org bucket:
  `character` / `item` / `mail` / `account` / `sync`); the topic remains single,
  the package split is for Javadoc / IDE discovery only. The `sync` group hosts
  the force-resync pair `ResyncEntitiesCommand` / `ResyncRowsCommand` (pks cap
  `MAX_PKS=1000`, optional `cascade`); their completion signal
  `events.sync.ResyncCompletedEvent` (UUIDv7 `eventId` + `resyncId` +
  `entityName` + adapter-clock `cycleStartedAt` / `completedAt`) is the single
  message type of the `sync` event family, partition key `null`.
- `app.l2nx.gs.adapter.api.kafka.ops` — operational telemetry payloads
  (`HeartbeatEvent`, `ModuleStatus`, `EntityStats`, `PoolStats`, `EventsStats`,
  `CommandsStats`)
- `app.l2nx.gs.adapter.api.spi` — SPIs: Tier-1 `AdapterModule`, Tier-2
  `DbSchemaProvider` / `RuntimeStateProvider` (`EntityMapping` carries the
  optional `parentRefs()` default — `ParentRef` cross-entity ownership
  declarations consumed by the db-sync force-resync cascade) + the gd-sync catalog
  providers (`ItemTemplateProvider` / `SkillProvider` / … `snapshot()` →
  `Collection<T>`) including the singleton `GearScoreRulesetProvider`
  (`entityName()` = `"gearscore"`, `snapshot()` → `Optional<GearScoreRuleset>`,
  empty when the build has no gear-score system), Tier-3
  `JdbcConnectionSource`,
  context bundle `ConnectContext` (now includes `io()` returning an
  adapter-owned `java.util.concurrent.Executor` for module-level blocking IO),
  capabilities `NxEvents` and `NxCommands` (consumed by host hooks;
  implementations live in adapter-core), per-invocation `CommandContext`
  (also exposes `io()` for handler-level blocking IO hops) + handler SAM
  `CommandHandler<C, R>` + game-thread hop helper `HostExecutor`

## Contracts worth calling out

- **`ConnectContext.io()` / `CommandContext.io()` (binary-breaking for external
  implementers).** `CommandContext.io()` is `abstract` on the interface; any
  external implementer (test doubles, alternate adapters) MUST implement it.
  `ConnectContext` gained an `io` field + getter and now has a 10-arg canonical
  constructor with a 9-arg back-compat constructor preserved for sources that
  built it positionally. Callers MUST hop blocking IO (JDBC, HTTP) onto these
  executors instead of running on the game thread or the Kafka consumer thread.
  Sanctioned exception: the db-sync resync handlers run their cascade-resolution
  JDBC synchronously on the consumer thread — the ack reply needs the resolved
  counts and the `ResyncRowsCommand.MAX_PKS` cap bounds the stall.
- **`SyncEvent` DELETED payload.** `payload=null` on `DELETED` ops no longer
  claims Kafka-tombstone semantics: topics use bounded retention, not log
  compaction, so consumers MUST explicitly handle the `DELETED` op (do not
  rely on the null-value tombstone protocol).
- **Identifier validation.** Any SQL identifier passed via `EntityMapping`,
  `PrimarySource`, or `ChildSource` (tableName / pkColumn / fkColumn /
  hashedColumns) MUST match `^[A-Za-z_][A-Za-z0-9_]{0,63}$`. Schema-qualified
  names (`schema.table`), quoted identifiers, and anything outside that pattern
  are rejected at engine start — no runtime quoting / escaping is performed.
- **Timestamps are UTC `Instant` only.** Every timestamp field in any wire
  DTO (`kafka.sync.*`, `kafka.events.*`, `kafka.commands.*`, `kafka.ops.*`,
  `rest.*`) MUST be `java.time.Instant` — never `OffsetDateTime`,
  `ZonedDateTime`, `LocalDateTime`, `LocalDate`, `LocalTime`, `java.util.Date`,
  `java.util.Calendar`, or `java.sql.{Date,Time,Timestamp}`. Rationale: the
  platform operates strictly on UTC; any zoned / local type risks a
  host-timezone leak. `Instant` is timezone-free by construction (UTC-equivalent
  moments) and serializes as ISO-8601 with the `Z` suffix
  (e.g. `"2026-06-01T12:00:00Z"`) — wire format is unambiguous.
  Schema providers MUST read source columns through
  `JdbcNulls.nullableInstantFromEpochMillis(rs, col)` /
  `instantFromEpochMillisOrSentinel(rs, col, sentinel)` (in `:nx-gs-commons`)
  rather than `rs.getTimestamp(...).toLocalDateTime(...)` style calls.
  Enforced at build time by `WireTimestampConformanceTest` (reflective
  classpath scan; fails the build on any violation in the scanned packages).

## Constraints

- **Java 8 source + target** — no `var`, no `Stream.toList()`, no records, no `Map.of`, no
  text blocks, no switch expressions, no pattern matching. Stream API + lambda + Optional
  are fine.
- **Zero runtime dependencies** — pure JDK only. No Spring, no Lombok, no Jackson, no Gson.
  JSON serialization is the consumer's responsibility (any binder works — Gson, Jackson,
  etc.). One exception: `org.jspecify:jspecify` is allowed for nullability annotations
  (`@Nullable` / `@NonNull`); JSpecify uses `RetentionPolicy.CLASS`, so it carries no
  runtime cost — annotations live in classfiles for static tooling but are not loaded at
  runtime. Wired as `api(libs.jspecify)` so consumers receive the annotations
  transitively and can run their own static nullness checking against the wire types.
- **POJOs, not records** — final fields + private constructor + static `builder()` +
  `equals/hashCode`. Records are Java 14+. Stick to plain classes.
- **Builder pattern** — every multi-field DTO ships with a hand-written `Builder` (no
  Lombok).
- **Public API → Javadoc mandatory** — every public type carries Javadoc; field-level JSON
  wire names documented next to the field.
- **Encode units in field names, not in comments.** Any field/getter carrying a physical
  unit puts the unit in its name: `Sec` (seconds), `Ms` (milliseconds), `Percent` (percent).
  E.g. `respawnSec`, `respawnRandomSec`, `reuseDelayMs`, `chancePercent`, `groupChancePercent`.
  Do NOT document a unit in a comment when it can live in the name. Non-physical counts/ids
  (`level`, `weight`, world coordinates, stat values) stay unsuffixed.
- **No framework annotations** — `@Component`, `@JsonProperty`, `@NotBlank` etc. are
  forbidden. The artifact is consumed by both Spring and non-Spring sides; framework
  coupling stays out of contracts.
- **Constructor parameter names preserved** (`-parameters` javac flag) so Jackson and other
  parameter-name-binding deserializers can build the POJOs without `@JsonProperty`
  annotations.

## Versioning

Slash-namespaced tag `api/vX.Y.Z` releases this module independently. Fallback when no
`-Pnx-gs-adapter-api.version=...` is passed: the literal in this module's `build.gradle.kts`.
Release flow lives in the monorepo root — see [`../CLAUDE.md`](../CLAUDE.md).

## Testing

- **Naming**: `methodName_shouldExpectedBehavior` or `methodName_shouldExpectedBehavior_whenCondition`
- JUnit 5, no Mockito (no behavior to mock — pure DTOs)
- Test what builders / equals / hashCode actually guarantee, not framework code
