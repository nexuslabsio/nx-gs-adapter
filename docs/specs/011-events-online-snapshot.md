# Events — Online snapshot

> Owner: @n1rmata

## Problem

Operators and platform-side dashboards need to chart server population over time —
both raw "how many players are online" and the breakdown by activity bucket
(offline-trade vs. real play vs. fishing vs. phantoms). Today bohpts-core renders
this overlay only inside the game-client character-list window: a custom UI
iterates `GameObjectsStorage.getPlayers()` on demand, applies inline predicates
(`isInOfflineMode`, `isFishing`, `isFakePlayer`), and draws the counts. Nothing
leaves the JVM. The platform sees nothing.

The premium-purchase rail (`events.premiumpurchase` family) shipped in Phase 3 already
proves the wire-level pattern for discrete in-game facts: per-family Kafka topic,
abstract base class, `Nx-Message-Type` header dispatch, UUIDv7 `eventId`,
host-pushed via `NxEvents.publishX(...)`. Online stats are the next family to
ride that rail — but as periodic snapshots rather than per-fact deltas. Stat
cardinality (counts per bucket) is build-agnostic: bohpts has offline-trade,
fishing, phantoms; Lucera/Essence forks have a different set; client-specific
forks add custom buckets. The wire schema MUST accommodate all of these without
api releases for every new bucket.

Audience: platform-side consumers (dashboards / TSDB ingest); host-side authors
plugging in a snapshot-builder.

## Requirements

> Sibling feature carrying the wire dispatch plumbing:
>
> - [`messaging`](008-messaging.md) — `MessagingTopics.events.<family>` > topic addressing, `Nx-Server-Id` connection-scoped header, > `Nx-Message-Type` per-record header, UUIDv7 idempotency. UNCHANGED by > this slice.

**Must:**

- [todo] R1. `nx-gs-adapter-api.kafka.events.serveronline.ServerOnlineSnapshotEvent` MUST ship as
  the abstract base for the `online` family — empty body, `protected` no-arg
  constructor, type-bound for `NxEvents.publishServerOnlineSnapshot(ServerOnlineSnapshotEvent)`. Mirrors
  `events.premiumpurchase.PremiumPurchaseEvent` exactly.

- [todo] R2. `nx-gs-adapter-api.kafka.events.serveronline.ServerOnlineSnapshotEvent` MUST
  ship as the Phase-1 concrete subtype with the following fields:
  - `UUID eventId` — REQUIRED. UUIDv7; the upper 48 bits encode the snapshot
    occurrence timestamp. Platform consumers dedupe on this id.
  - `@Nullable Map<String, Long> buckets` — bucket-key → count map. Every
    snapshot MUST carry the required canonical keys `TOTAL` and `UNIQUE`;
    hosts SHOULD additionally publish `OFFLINE_TRADE` / `FISHING` when the
    concept applies, and MAY publish arbitrary host-specific keys. Null at
    the constructor normalizes to an empty map; getter returns an
    unmodifiable view.

  No top-level `total` field — buckets can overlap (e.g. a fishing player is
  also in `UNIQUE`), so `TOTAL` cannot be derived as `sum(buckets)`.
  The host publishes `WellKnownServerOnlineBuckets.TOTAL` as an explicit map
  entry.

  POJO + hand-written Builder + `equals`/`hashCode`/`toString` + Java-8 source.
  Constructor parameter names preserved for Gson `-parameters` deserialization.

- [todo] R3. `nx-gs-adapter-api.kafka.events.serveronline.WellKnownServerOnlineBuckets` MUST
  ship a constants class enumerating the canonical bucket keys observed in
  L2 game-server forks. The canonical set is intentionally narrow — these
  are the keys cross-tenant dashboards aggregate on. Wire values are
  `lower_snake_case` (consistent with `WellKnownServices`):

  **Required** (host MUST publish):
  - `TOTAL` → `"total"` — total character presence (includes offline-trade
    and phantoms).
  - `UNIQUE` → `"unique"` — distinct active human players, deduplicated by
    a host-defined identity tuple (bohpts: HWID + IP among
    `!isInOfflineMode() && !isFakePlayer()`).

  **Optional canonical** (host SHOULD publish when concept applies;
  consumers MUST tolerate absence):
  - `OFFLINE_TRADE` → `"offline_trade"` — players parked in offline-trade mode.
  - `FISHING` → `"fishing"` — characters currently fishing.

  Hosts MAY publish arbitrary non-canonical keys; the platform treats unknown
  keys as opaque strings. Adding a new canonical constant is a non-breaking
  minor-version change.

  Soft invariant when both keys present: `total >= unique + offline_trade`.
  Consumers MUST NOT reject snapshots that violate it (transient race during
  the tick walk can produce minor drift).

- [todo] R4. `nx-gs-adapter-api.spi.NxEvents` MUST gain a single new method
  `void publishServerOnlineSnapshot(ServerOnlineSnapshotEvent event)` mirroring `publishPremiumPurchase` exactly:
  null event → silent no-op + WARN log, unregistered subtype → drop + WARN,
  family disabled (no topic in `MessagingTopics.events.serveronline`) → drop + DEBUG,
  game-loop-safety contract (never blocks beyond enqueue, never throws).

- [todo] R5. `nx-gs-adapter-core.events.EventTypeRegistry` MUST gain a binding
  for `ServerOnlineSnapshotEvent`: family `"serveronline"`, message-type
  `"ServerOnlineSnapshotEvent"`, partition-key extractor returning `null` (round-robin
  partitioning; consumer groups by `Nx-Server-Id` header and orders by the
  UUIDv7 `eventId` timestamp).

- [todo] R6. `nx-gs-adapter-core.events.NxEventsImpl` MUST implement
  `publishServerOnlineSnapshot(ServerOnlineSnapshotEvent)` with the same dispatch + null-check + family-disabled
  short-circuit logic as `publishPremiumPurchase`. No new internal infrastructure —
  reuses `EventsPublisher` / `EventEnvelope` / `EventTypeRegistry` as-is.

- [todo] R7. `bohpts-core` MUST extend the existing
  `l2e.gameserver.l2nx.events.BohptsEventsModule` (the same module that owns
  the `events.premiumpurchase` wiring) to additionally:
  - `onConnect(ctx)` — bind the captured `ctx.events()` handle into a new
    `OnlineSnapshotBuilder` static facade alongside the existing
    `PremiumPublisher.bind(...)` call.
  - `start()` — schedule a periodic snapshot task on `ThreadPoolManager`
    via `scheduleAtFixedDelay` (NOT `scheduleAtFixedRate` — a stalled tick
    must NOT trigger catch-up bursts) at a 30-second period.
  - `stop()` — cancel the scheduled task.
  - `onDisconnect()` — also release the `OnlineSnapshotBuilder` handle.

  The snapshot-builder iterates `GameObjectsStorage.getPlayers()` once per tick,
  walks every player, applies the bohpts predicates (`isInOfflineMode`,
  `isFishing`, `isFakePlayer`, `getHWID`, `getIPAddress`) and computes the
  four canonical buckets:
  - `total` — every player in the iteration (including phantoms and
    offline-trade).
  - `unique` — `Set<(hwid, ip)>.size()` accumulated across players where
    `!isInOfflineMode() && !isFakePlayer()` and both HWID and IP are
    meaningful (HWID != `"N/A"`, IP != `"N/A"` and != `"Disconnected"`).
  - `offline_trade` — `isInOfflineMode()`.
  - `fishing` — `isFishing()` regardless of offline-trade / phantom state.

  Builds `ServerOnlineSnapshotEvent` with UUIDv7 `eventId`, calls
  `nxEvents.publishServerOnlineSnapshot(event)`. Any uncaught `Throwable` is
  logged at DEBUG and swallowed — game-loop safety identical to
  `PremiumPublisher`.

  No separate `AdapterModule` registration — `events.serveronline` rides the same
  `bohpts-events` module entry in `META-INF/services` as `events.premiumpurchase`.

**Should:**

- [todo] R8. Cadence is host-managed and currently hardcoded to 30 seconds.
  Promoting to a `Config` property is a follow-up if operators ask for it.

**Could:**

- [todo] R9. Per-character online-presence delta events
  (`OnlineLoginEvent` / `OnlineLogoutEvent` / `OnlineStateChangeEvent`) on the
  same `online` family — out of scope for this slice. Snapshot-only ground
  truth is sufficient for Phase 1 dashboarding.

**Non-goals:**

- **Adapter-side scheduler / pull-SPI.** Host owns cadence, identical to
  `events.premiumpurchase` shape. Adapter-core stays mechanism-only.
- **Per-family heartbeat counters.** `EventsStats` aggregates queue/published/
  dropped/failed across all families today; per-family breakdown is a separate
  enhancement that lands when there are 3+ families.
- **Strong-typed wellknown bucket fields.** Open `Map<String, Long>` is the
  deliberate choice — adding a new wellknown constant doesn't bump the api
  version, and host-specific custom buckets ride the same map.

### Edge cases

- **Empty player set on startup tick.** Builder produces a snapshot with
  every wellknown bucket = 0; consumer sees `TOTAL=0` and a flat dashboard
  curve. No special-case suppression — observability prefers explicit zeros.
- **Snapshot publish during `onDisconnect`.** Module's `stop()` cancels the
  scheduled task before `onDisconnect()` releases the handle. Race window:
  a tick already in `run()` may call `publishServerOnlineSnapshot` after handle release;
  `NoOpEvents` swallows it (per `ConnectContext` normalization).
- **Reconnect mid-tick.** The `NxEvents` façade handed to the module via
  `ConnectContext.events()` has stable identity across reconnect cycles —
  the adapter swaps an internal `AtomicReference` to the live publisher on
  every reconnect, so a snapshot built before reconnect and published after
  lands on the freshly-reconnected producer without the module re-acquiring
  the handle.
- **`publishServerOnlineSnapshot` thrown from inside the host (e.g. snapshot-builder bug).**
  Tick logs at DEBUG, skips the publish, schedules the next tick normally.
  No backoff — transient bug fixes itself on next tick.
- **Family disabled (platform did not configure `MessagingTopics.events.serveronline`).**
  Adapter logs DEBUG once per call, increments nothing, surfaces via the
  `events.disabled-families` heartbeat slot (existing mechanism).
- **Buckets overlap.** A fishing active player counts toward both `FISHING`
  and `UNIQUE` and `TOTAL`. `TOTAL` is published as a separate map entry —
  the consumer never tries to derive it from `sum(buckets)`.
- **Unidentifiable client.** Active non-phantom player whose `_client` is
  transiently null (login / logout boundary) reports HWID=`"N/A"` and
  IP=`"N/A"`. The bucket-builder skips that row from the unique-set entirely
  rather than collapsing it into a `("N/A","N/A")` sentinel — accuracy of
  `unique` is more important than reflecting transient login-window state.

## Open questions

- [resolved: snapshot-only, no per-character deltas in Phase 1 — user confirmed
  during design that per-character stats land later as a separate slice.]
- [resolved: host-pushed (no SPI / no engine in adapter-core) — symmetric with
  `events.premiumpurchase`. Adapter does not own the cadence.]
- [resolved: open `Map<String, Long>` + `WellKnownServerOnlineBuckets` constants
  (rejected typed-fields-per-bucket alternative). Allows adding new
  conventional buckets without api releases and accommodates host-specific
  custom buckets without a wire-schema change.]
- [resolved: canonical set narrowed to `total` / `unique` / `offline_trade` /
  `fishing` (api/v0.26.0). `online` / `real` / `phantoms` were removed —
  `unique` (distinct active humans by host-defined identity tuple) carries
  the operator-facing audience number; hosts that still want phantom counts
  emit them under custom keys.]
- [resolved: partition-key = `null` (round-robin). Snapshot cadence is low
  (~one per 30 sec × N servers); ordering preserved per-server via the UUIDv7
  `eventId` timestamp. Consumer groups by `Nx-Server-Id` header.]
- [assumed: 30-second default tick interval. Matches the typical pre-existing
  `OnlinePlayers` announce cadence and gives ~2 ppm baseline traffic per
  server. Promoting to a Config knob is R8.]

## Links

- Sibling feature (events runtime + per-family fanout):
  [`docs/specs/008-messaging.md`](008-messaging.md)
- Sibling reference (premium-family wire DTOs + publisher pattern):
  `nx-gs-adapter-api/src/main/java/app/l2nx/gs/adapter/api/kafka/events/premium/`
- Technical design: see the Technical design section below — wire layout,
  binding registration, and the bohpts snapshot-builder walkthrough

---

## Technical design

### Overview

`events.serveronline` is the second event family on the per-family Kafka rail
introduced by `events.premiumpurchase`. It carries periodic snapshots of game-server
population broken down by activity bucket. Wire shape: a single concrete DTO
`ServerOnlineSnapshotEvent` with a UUIDv7 `eventId` and an open `Map<String, Long>`
of bucket-key → count entries (lower_snake_case keys, consistent with
`WellKnownServices`); `WellKnownServerOnlineBuckets` enumerates four
canonical constants — `total` and `unique` are required on every snapshot,
`offline_trade` and `fishing` are optional canonical (host SHOULD publish
when concept applies). Hosts MAY additionally publish arbitrary
non-canonical keys (open map). Hosts publish via a single
`NxEvents.publishServerOnlineSnapshot(ServerOnlineSnapshotEvent)` SPI method.
Adapter-core registers the binding in `EventTypeRegistry`; the existing
`EventsPublisher` / `EventEnvelope` machinery handles fanout, headers, and
disabled-family short-circuiting unchanged. bohpts-core extends its existing
`BohptsEventsModule` (the module that already owns `events.premiumpurchase` wiring)
to schedule a 30-second snapshot tick (`scheduleAtFixedDelay`) on
`ThreadPoolManager`, walking `GameObjectsStorage.getPlayers()`, computing
the four canonical buckets, and publishing.

### Structure

- `nx-gs-adapter-api/src/main/java/app/l2nx/gs/adapter/api/kafka/events/online/`
  - `ServerOnlineSnapshotEvent.java` — abstract family base (empty body, type-bound)
  - `ServerOnlineSnapshotEvent.java` — Phase-1 concrete DTO + Builder
  - `WellKnownServerOnlineBuckets.java` — canonical bucket-key constants
- `nx-gs-adapter-api/src/main/java/app/l2nx/gs/adapter/api/spi/`
  - `NxEvents.java` — adds `publishServerOnlineSnapshot(ServerOnlineSnapshotEvent)`
  - `NoOpEvents.java` — adds the no-op variant
- `nx-gs-adapter-core/src/main/java/app/l2nx/gs/adapter/core/events/`
  - `EventTypeRegistry.java` — adds `online` family + `ServerOnlineSnapshotEvent`
    binding
  - `NxEventsImpl.java` — adds `publishServerOnlineSnapshot` dispatch
- `bohpts-core/core/src/main/java/l2e/gameserver/l2nx/events/`
  - `BohptsEventsModule.java` — extended to also schedule the online snapshot
    tick alongside its existing premium wiring
  - `OnlineSnapshotBuilder.java` — pure player-iteration → bucket-counts +
    publish facade (lives next to `PremiumPublisher.java`)

### Key components

- **ServerOnlineSnapshotEvent** (implements R1) — abstract base for the `online` family,
  empty body. Mirrors `PremiumPurchaseEvent` exactly so future subtypes (e.g.
  `OnlineLoginEvent`, `OnlineLogoutEvent`) plug into the same
  `publishServerOnlineSnapshot(ServerOnlineSnapshotEvent)` entry-point and dispatch via `Nx-Message-Type`.

- **ServerOnlineSnapshotEvent** (implements R2) — concrete Phase-1 subtype.
  Two fields: `UUID eventId` (UUIDv7), `Map<String, Long> buckets`. Map is
  defensively copied on construction and exposed via an unmodifiable view;
  null normalizes to `Collections.emptyMap()`. Has a hand-written `Builder`
  with `toBuilder()`. Java-8 source (no records).

- **WellKnownServerOnlineBuckets** (implements R3) — string constants split
  into required (`TOTAL`, `UNIQUE`) and optional canonical (`OFFLINE_TRADE`,
  `FISHING`). Each constant carries a Javadoc paragraph clarifying the bohpts
  reference definition; other forks may reuse the constant with their own
  bucket-builder logic so long as the operator-facing meaning is consistent
  (e.g. `UNIQUE` is "distinct active humans" — the identity tuple is
  host-defined).

- **NxEvents.publishServerOnlineSnapshot / NxEventsImpl.publishServerOnlineSnapshot** (implements R4, R6) —
  symmetric to `publishPremiumPurchase`. Null event → WARN + drop; missing registry
  binding → WARN + drop; family disabled → DEBUG + drop; otherwise enqueue
  on `EventsPublisher` via the existing `EventEnvelope` path. Drop policy on
  queue overflow defaults to `newest` (drop incoming; queue order preserved);
  `oldest` (evict head) remains opt-in but over-counts `dropped-total` under
  multi-producer contention.

- **`NxEvents` façade reconnect stability** — the façade returned by
  `ConnectContext.events()` survives reconnect cycles. An internal
  `AtomicReference` inside the façade is swapped to the live publisher on
  every reconnect, so `OnlineSnapshotBuilder` (and any other module-scoped
  caller) can cache the handle once at `onConnect` and never re-acquire.

- **EventTypeRegistry online binding** (implements R5) — `("serveronline",
"ServerOnlineSnapshotEvent", evt -> null)`. Null partition-key is intentional —
  consumer groups by `Nx-Server-Id` header (always present post-handshake)
  and orders within-server by the UUIDv7 timestamp.

- **BohptsEventsModule** (implements R7) — pre-existing Tier-1 module
  extended with one `ScheduledFuture<?>` slot for the online tick.
  `onConnect` calls both `PremiumPublisher.bind(...)` and
  `OnlineSnapshotBuilder.bind(...)`; `start()` schedules
  `OnlineSnapshotBuilder::tick` via
  `ThreadPoolManager.scheduleAtFixedDelay` (30 s — fixed-delay so a
  stalled tick never triggers a catch-up burst); `stop()` cancels;
  `onDisconnect()` unbinds both publishers.

- **OnlineSnapshotBuilder** — public static facade in the same
  `l2nx.events` package as `PremiumPublisher`. Holds the current
  `NxEvents` handle (volatile). `tick()` iterates the player set on
  whatever thread `ThreadPoolManager` provides, computes the four
  canonical counts in a single pass, builds the event, and publishes.
  Wraps the whole tick in `try { ... } catch (Throwable t) { log.debug }`
  for game-loop-safety symmetry with `PremiumPublisher`.

### Data flows

Snapshot tick (host → platform):

```
ThreadPoolManager.scheduleAtFixedDelay
  → OnlineSnapshotBuilder.tick()
    → GameObjectsStorage.getPlayers() iteration (single pass)
      → counters: total++ always
                  offlineTrade++ if isInOfflineMode()
                  uniqueSet.add(hwid + "|" + ip) if !offline && !fake
                                                && hwid != "N/A"
                                                && ip ∉ {"N/A","Disconnected"}
                  fishing++ if isFishing()
      → unique = uniqueSet.size()
    → ServerOnlineSnapshotEvent (eventId = UUIDv7.generate(), buckets = Map.of(...))
    → NxEvents.publishServerOnlineSnapshot(event)
      → NxEventsImpl: registry.lookup → publisher.isFamilyEnabled →
        publisher.enqueue(EventEnvelope)
        → EventsPublisher daemon: KafkaProducer.send(record + Nx-Message-Type
          header)
```

Disabled family (no `online` topic in `MessagingTopics.events`):

```
NxEventsImpl.publishServerOnlineSnapshot → registry.lookup → publisher.isFamilyEnabled = false
  → log.debug("events.serveronline disabled — no topic configured; skipping publish")
  → return (no enqueue, no queue-depth growth, no dropped-total increment)
```

### Decisions

- **Snapshot, not deltas, for Phase 1.** Snapshot-cadence is naturally
  appropriate for population stats; per-fact login/logout deltas would
  require platform-side state reconstruction and add traffic proportional
  to login churn. Delta-based per-character events are deferred.

- **Host-pushed (no SPI on the host side).** Symmetric with `events.premiumpurchase`.
  Adapter has no `OnlineStatsProvider` SPI and no scheduled engine; the host
  owns the cadence and the bucket-builder. Trade-off: cadence is per-host
  rather than centrally tunable from the platform — acceptable since hosts
  already own cadence in many other places (e.g. existing `OnlinePlayers`
  announce schedule).

- **Open Map + WellKnown constants (vs. typed bucket fields).** Adding a
  new wellknown constant is a non-breaking minor-version change in
  `nx-gs-adapter-api`; adding a typed field would force every consumer to
  recompile against the new class shape. The map also accommodates host-
  specific custom buckets without any api change.

- **Partition-key = null (vs. server-id-as-key).** Snapshots are infrequent;
  the cost of round-robin partitioning (no within-server ordering at the
  partition level) is offset by the UUIDv7 `eventId` carrying the
  occurrence timestamp. The consumer groups by `Nx-Server-Id` (already a
  static header) and orders by `eventId`. Plumbing server-id into the
  binding's `partitionKeyExtractor` would require expanding the binding
  shape — overkill for one family.

### Extension points

- **Add a new wellknown bucket.** Append a `public static final String`
  constant to `WellKnownServerOnlineBuckets`. Update each host's bucket-builder
  to populate the new entry where applicable. Old consumers ignore the
  new key; new consumers see `null`/missing for old hosts.

- **Add a new concrete subtype to `events.serveronline`.** Define a new class
  extending `ServerOnlineSnapshotEvent`, append a binding entry to `EventTypeRegistry`
  (same family `"serveronline"`, distinct message-type string). The platform
  consumer must learn to dispatch the new `Nx-Message-Type`.

- **Add a new event family.** Define `<family>Event` abstract base under
  `app.l2nx.gs.adapter.api.kafka.events.<family>`, ship one or more concrete
  subtypes, add a `publishX(<family>Event)` method on `NxEvents`, append
  bindings to `EventTypeRegistry`. Platform must populate
  `MessagingTopics.events.<family>` with a topic name. Mirrors what this
  feature does for `online`.
