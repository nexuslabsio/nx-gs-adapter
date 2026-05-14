# Events — Online snapshot — tech

> Covers: spec.md

## Overview

`events.serveronline` is the second event family on the per-family Kafka rail
introduced by `events.premiumpurchase`. It carries periodic snapshots of game-server
population broken down by activity bucket. Wire shape: a single concrete DTO
`ServerOnlineSnapshotEvent` with a UUIDv7 `eventId` and an open `Map<String, Long>`
of bucket-key → count entries (lower_snake_case keys, consistent with
`WellKnownServices`); `WellKnownServerOnlineBuckets` enumerates the canonical
constants (`total`, `online`, `real`, `offline_trade`, `fishing`,
`phantoms`). Hosts publish via a single new
`NxEvents.publishServerOnlineSnapshot(ServerOnlineSnapshotEvent)` SPI method. Adapter-core registers the
binding in `EventTypeRegistry`; the existing `EventsPublisher` /
`EventEnvelope` machinery handles fanout, headers, and disabled-family
short-circuiting unchanged. bohpts-core extends its existing
`BohptsEventsModule` (the module that already owns `events.premiumpurchase` wiring)
to schedule a 30-second snapshot tick (`scheduleAtFixedDelay`) on
`ThreadPoolManager`, walking `GameObjectsStorage.getPlayers()`, computing
the six wellknown buckets, and publishing.

## Structure

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

## Key components

- **ServerOnlineSnapshotEvent** (implements R1) — abstract base for the `online` family,
  empty body. Mirrors `PremiumPurchaseEvent` exactly so future subtypes (e.g.
  `OnlineLoginEvent`, `OnlineLogoutEvent`) plug into the same
  `publishServerOnlineSnapshot(ServerOnlineSnapshotEvent)` entry-point and dispatch via `Nx-Message-Type`.

- **ServerOnlineSnapshotEvent** (implements R2) — concrete Phase-1 subtype.
  Two fields: `UUID eventId` (UUIDv7), `Map<String, Long> buckets`. Map is
  defensively copied on construction and exposed via an unmodifiable view;
  null normalizes to `Collections.emptyMap()`. Has a hand-written `Builder`
  with `toBuilder()`. Java-8 source (no records).

- **WellKnownServerOnlineBuckets** (implements R3) — string constants. Doc-only
  semantics: each constant carries a Javadoc paragraph clarifying the bohpts
  reference definition; other forks may reuse the constant with their own
  bucket-builder logic so long as the operator-facing meaning is consistent.

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
  whatever thread `ThreadPoolManager` provides, computes the six
  wellknown counts in a single pass, builds the event, and publishes.
  Wraps the whole tick in `try { ... } catch (Throwable t) { log.debug }`
  for game-loop-safety symmetry with `PremiumPublisher`.

## Data flows

Snapshot tick (host → platform):

```
ThreadPoolManager.scheduleAtFixedDelay
  → OnlineSnapshotBuilder.tick()
    → GameObjectsStorage.getPlayers() iteration (single pass)
      → counters: total++, online++ if !offline, real++ if !fake,
                  offlineTrade++ if offline, fishing++ if fishing,
                  phantoms++ if fake
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

## Decisions

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

## Extension points

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
