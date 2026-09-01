# Character log events

> Owner: @n1rmata

## Problem

The platform sees a character's _current_ state — class, noblesse, subclasses — through the CDC and
runtime sync rails, but never the _moment_ a character crossed a threshold. Reconstructing "took the
2nd profession at T" out of state means snapshot diffing: expensive, blind across restarts, and
unable to tell a real transition apart from a backfill or a manual admin edit. Consumers that must
react to the transition itself — a referral programme paying per achieved goal, achievement systems,
progression analytics — have nothing to subscribe to.

The host already knows every such moment exactly. `Player.setClassId`, `Player.addSubClass` and
`Player.applyNobleState` are single choke points every progression path funnels through. What bohpts
does today instead is write a portal-facing `character_referral_status` row from 27 scattered call
sites: a state table rather than an event stream, carrying four of the five thresholds (no 1st
profession) and readable only by whoever can reach the game database.

Audience: platform consumers that need discrete character-progression facts; host authors hooking
progression paths.

## Requirements

> Sibling feature carrying the wire dispatch plumbing:
>
> - [`messaging`](008-messaging.md) — `MessagingTopics.events.<family>` topic addressing,
>   `Nx-Server-Id` connection-scoped header, `Nx-Message-Type` per-record header, UUIDv7
>   idempotency. UNCHANGED.

**Must:**

- [done] R1. `nx-gs-adapter-api.kafka.events.characterlog.CharacterLogEvent` MUST ship as the single
  concrete event of a new `characterlog` family, carrying a discriminated dynamic payload rather
  than a class per threshold:

  - `UUID eventId` — REQUIRED. UUIDv7; the upper 48 bits encode the occurrence timestamp, so there
    is no separate `occurredAt` field.
  - `long charId` — REQUIRED. The character the fact is about, and the Kafka partition key, so one
    character's facts land on one partition in occurrence order.
  - `String type` — REQUIRED. Open token classifying the fact. Canonical values in
    `WellKnownCharacterLogTypes`; hosts MAY publish tokens the platform does not know yet.
  - `@Nullable Map<String, String> metadata` — OPTIONAL open string→string map of per-type
    attributes. Canonical keys in `WellKnownCharacterLogMetadata`. Consumers ignore keys they do not
    understand.

  POJO + hand-written Builder + `equals` / `hashCode` / `toString`, Java-8 source, constructor
  parameter names preserved (`-parameters`) for Gson / Jackson parameter-name deserialization.
  `eventId` and `type` validated non-null in the constructor; `metadata` defensively copied and
  frozen, `null` preserved as `null` (matching `CharacterDeathEvent`).

- [done] R2. `WellKnownCharacterLogTypes` MUST ship the launch token set: `FIRST_CLASS`,
  `SECOND_CLASS`, `THIRD_CLASS`, `SUBCLASS_ADDED`, `NOBLESSE`. Adding a constant is a non-breaking
  minor-version change; a host emitting an unlisted token is valid by construction.

- [done] R3. `WellKnownCharacterLogMetadata` MUST ship the canonical keys, all decimal strings:
  `class_id`, `class_level` (1 / 2 / 3), `class_index` (0 = main class, >0 = subclass slot),
  `char_level`, `subclass_index`. Per-type expectations are documented on the constants, not
  enforced on the wire.

- [done] R4. `EventTypeRegistry` MUST bind `CharacterLogEvent` to family key `characterlog` with
  `charId` as the partition-key extractor.

- [wip]  R5. `bohpts-core` MUST publish from three choke points, replacing scattered per-call-site
  branches:

  - `Player.setClassId(int)` — sample `getClassId().level()` before the change, compare after; on an
    increase emit `FIRST_CLASS` / `SECOND_CLASS` / `THIRD_CLASS` for the new level, with `class_id`,
    `class_level`, `class_index` and `char_level`. A level that did not increase (sideways admin
    edit, subclass slot rewrite) emits nothing.
  - `Player.addSubClass(int, int)` — emit `SUBCLASS_ADDED` with `class_id`, `subclass_index` and
    `char_level`, on a successful add only.
  - `Player.applyNobleState(boolean, boolean)` — emit `NOBLESSE` only on a `false → true`
    transition, sampled before the field is written, and only when the caller asked for the change to
    persist. `CharacterDAO` restores the flag on every login with `store=false`; publishing there
    would turn each login of a noble into a fresh grant. Revocation emits nothing.

  Two blanket skips apply to all three: phantoms (server-driven bots with no account behind them),
  and characters not yet registered in the world. The second is what separates restored state from a
  real transition — `CharacterDAO` reverts a subclass-relogin exploit with
  `setClassId(getBaseClass())` during load, which without the guard reads as a freshly taken
  profession.

- [wip]  R6. The publisher MUST report drops through `EventDrops`, without rate limiting. These are
  non-repeatable facts with no next tick, and a suppressed warning would hide exactly the case the
  helper exists for — see `nx-gameservers/docs/specs/070-event-publishing-reliability.md` § 5, which
  splits publishers into tick-snapshot and non-repeatable classes.

- [done] R7. `nx-tenants` MUST advertise the family in the adapter connect response as
  `messagingTopics.events.characterlog` → `<slug>.gs.events.character.log`. Without the entry every
  `publish` is a silent no-op at DEBUG.

**Non-goals:**

- Referral eligibility, reward rules and payout. This slice ships the fact stream only; the referral
  engine is a separate feature in `nx-gameservers` and MUST NOT leak into the host.
- Backfilling facts that predate the release. The journal starts empty; reconstructing history from
  CDC state belongs to whichever consumer needs it.
- Retiring `CharacterReferralStatusDAO` and its 27 call sites. The portal reads that table; removing
  it is a separate change with its own blast radius.
- Character creation / deletion / rename. Those already arrive over the `gs.sync.db.character` CDC
  rail and are recoverable from state.

### Edge cases

- **A profession taken on an active subclass.** `Player.setClassId` also rewrites the active subclass
  slot, so a 3rd profession taken on a subclass produces a real event. It is emitted with
  `class_index > 0`; whether that counts is the consumer's business, not the host's.
- **Admin `//setclass`.** `EditChar` funnels through the same choke point, so an admin-granted
  profession emits a normal event. Intentional: the platform should see it, and gating on the caller
  would make the host guess intent.
- **Auto-profession on high-rate servers.** A server that grants a profession at character creation
  emits `FIRST_CLASS` for every new character. That is a truthful fact; a consumer that finds it
  noisy filters it per server.
- **Grants made during character creation.** `RequestCharacterCreate` can hand out noblesse before the
  character has entered the world, and the in-world guard drops that publish. Accepted: a character
  born noble has crossed no threshold, and paying the guard's cost here buys immunity to the whole
  class of restore-time false positives.
- **`_subclassLock`.** `setClassId` returns early while the lock is held. Sampling happens after that
  guard, so a locked-out call emits nothing — matching the fact that no change occurred.

## Technical design

### Overview

One new event family with one concrete event type, whose payload is a discriminated open map rather
than a class per fact. Three publisher entry points in the host, one registry binding in
adapter-core, one topic entry in `nx-tenants`.

### Structure

- `nx-gs-adapter-api/…/kafka/events/characterlog/CharacterLogEvent.java` — wire DTO.
- `nx-gs-adapter-api/…/kafka/events/characterlog/WellKnownCharacterLogTypes.java` — `type` tokens.
- `nx-gs-adapter-api/…/kafka/events/characterlog/WellKnownCharacterLogMetadata.java` — metadata keys.
- `nx-gs-adapter-core/…/core/events/EventTypeRegistry.java` — one `register(...)` line.
- `bohpts-core`: `l2e/gameserver/l2nx/events/characterlog/CharacterLogPublisher.java` — host facade,
  bound by `BohptsEventsModule` on handshake.
- `nx-tenants`: `api/rest/adapter/AdapterController.java` — one `Map.entry` in the events map.

### Key components

`CharacterLogPublisher` — static facade over the `NxEvents` handle, same shape as
`CharacterDeathPublisher`: `bind(handle)` on handshake, publish methods that never throw into the
host path. It owns the transition logic (level comparison, `false → true` for noblesse) so the call
sites stay one-liners and cannot forget a branch.

### Decisions

**One event type with a dynamic payload, not a class per fact.** A `CharacterSecondClassEvent`
hierarchy would give compile-time field safety, but every new fact would then need a synchronised
release of api, host and platform before it could flow. With an open `type` plus an open metadata
map the host ships a new fact immediately, the platform stores it, and a consumer learns to read it
later — against data already accumulated rather than from zero. The price is that a typo in a key is
not caught by the compiler; it is paid down with `WellKnown*` constants on both sides and, on the
consumer, a declared required-key set per type whose violations are logged and counted rather than
silently skipped. This mirrors the existing `CharacterDeathEvent` / `WellKnownDeathMetadata` and
runtime `Activity` idiom rather than inventing a third convention.

**`Map<String,String>`, not `Map<String,Object>`.** Every open payload in this API is already a flat
string map, numbers included (`killer_id` is documented as a decimal string). Gson on the Java-8 host
and Jackson 3 on the platform both handle it with no polymorphic adapters and no
`RuntimeTypeAdapterFactory`.

**Its own family rather than a message type on `character`.** `character` carries presence, a
high-volume login/logout stream; consumer lag there would delay facts that gate payouts, and the two
would share one lag metric. A separate topic also lets the consumer group be scaled and reset
independently.

**Topic `<tenant>.gs.events.character.log`, family key `characterlog`.** The dotted topic reads as a
namespace, leaving room for `character.*` siblings later. The family key stays a single token because
it is a JSON map key in the connect response — a dot there is parsed as nesting by JSON-path readers,
including the `nx-tenants` integration tests. Consumer topic patterns stay anchored (`$`), so
`^.*\.gs\.events\.character$` keeps ignoring the new topic.

**No account login on the wire.** The platform resolves character → account through
`gs_characters.account_name`, indexed since its `v1.4.1`. Carrying it would duplicate a fact the
consumer already owns, against the same rule that keeps names off `RaidActor`.

**Transition detection in the publisher, not at the call sites.** The 14 `setClassId` call sites each
carry their own `if (level() == 2) … else if (level() == 3)` branch today, and not one has a branch
for level 1 — which is exactly why the 1st profession is missing everywhere. Comparing before/after
inside the single choke point makes that omission structurally impossible.

### Extension points

New fact types are additive: a constant in `WellKnownCharacterLogTypes`, keys in
`WellKnownCharacterLogMetadata`, a publisher entry point. No registry, topic or platform change.
Likely next: `HERO`, `CLAN_LEADER`, `MARRIED`, level milestones.

## Rollout

Release order is forced by the handshake and the artifact graph:

0. **Create the Kafka topic.** `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false` on the prod broker, so
   `<tenant>.gs.events.character.log` must exist before the first publish — otherwise every send
   fails and the fact is lost. The suffix is added to `STANDARD_TOPICS` in
   `nx-infra/komodo/l2nx/{prod,dev}-kafka/scripts/create-tenant.sh`, which covers future tenants;
   existing ones need the topic created by hand (recipe in that tenant's page under
   `prod-kafka/tenants/`). Same partitions / retention as its `character` sibling.
1. `nx-gs-adapter` — `nx-gs-adapter-api` + `nx-gs-adapter-core`. Publish and wait for the artifacts
   to resolve before anything builds against them.
2. `nx-tenants` — advertise the family. Until this ships, host publishes are silent no-ops, so it
   must land before the host.
3. `nx-gameservers` — consumer and table, idle until messages arrive.
4. `bohpts-core` — publisher, rebuilt against the new adapter version.

**Shipped 2026-09-01.** Steps 0-3 are live: topic created, `api/v0.85.0` + `core/v0.37.0` on Central,
`nx-tenants v1.27.0` advertising the family, `nx-gameservers v0.123.0` consuming it (migration
applied, consumer group subscribed, zero lag, journal empty as expected). Step 4 is green on the
`test` branch and waiting on PR #2901 into `release`; until that merges and the game servers restart,
the journal stays empty by design.

Steps 3 and 4 may swap: the topic keeps three hours of retention, so a consumer arriving slightly
late still catches everything. Publishing before step 2 loses events silently, which is why that
ordering is not optional.

## Open questions

- [ ] `[assumed: emitting on a subclass slot is correct]` A 3rd profession taken on a subclass emits
      with `class_index > 0`. If a consumer turns out to need main-class-only semantics on the wire,
      the filter belongs to it, not to the host.

## Links

- `nx-gameservers/docs/specs/071-character-log.md` — platform-side ingest, storage and consumers.
- `nx-gameservers/docs/specs/070-event-publishing-reliability.md` — why R6 uses `EventDrops`.
- [`messaging`](008-messaging.md), [`events-raid`](014-events-raid.md) — the wire pattern this family
  follows.
