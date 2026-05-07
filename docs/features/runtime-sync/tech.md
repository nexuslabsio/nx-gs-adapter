# Runtime Sync — tech

> Covers: spec.md

## Overview

Runtime sync is a sibling adapter module to `db-sync`, shipped as the
`nx-gs-runtime-sync-core` artifact. It runs one daemon thread per declared
`RuntimeEntityMapping`, ticks at a configurable interval (default 10s), pulls a
`Iterable<RuntimeRow>` snapshot from a Tier-2 `RuntimeStateProvider` SPI implemented in
the host JAR, computes an FNV-1a 64-bit hash per row, diffs against the previous tick's
hash map, and publishes NEW + CHANGED rows as `SyncEvent<T>` to the per-entity Kafka
topic resolved from `ConnectResponse.syncTopics.runtime[entityName]`. GONE rows are
silently dropped (no tombstone) — `db-sync` owns "permanently gone" semantics. MVP
entity is `character` (vitals + position) against bohpts.

## Structure

- `nx-gs-runtime-sync-core/` [planned] — module artifact root
- `nx-gs-runtime-sync-core/src/main/java/app/l2nx/gs/runtime/sync/` [planned] —
  package root, parallel to `app.l2nx.gs.db.sync`
- `nx-gs-runtime-sync-core/src/main/java/app/l2nx/gs/runtime/sync/RuntimeSyncModule.java`
  [planned] — Tier-1 `AdapterModule` impl, `name() = "runtime-sync"`
- `nx-gs-runtime-sync-core/src/main/java/app/l2nx/gs/runtime/sync/engine/` [planned] —
  per-entity tick loop, snapshot+diff, publish
- (hash helper) — uses published `app.l2nx.gs.commons.hash.Fnv1a64` from
  `:nx-gs-commons`; no engine-internal copy
- `nx-gs-runtime-sync-core/src/main/resources/META-INF/services/app.l2nx.gs.adapter.api.spi.AdapterModule`
  [planned] — ServiceLoader descriptor
- `nx-gs-adapter-api/src/main/java/app/l2nx/gs/adapter/api/spi/RuntimeStateProvider.java`
  [planned] — Tier-2 SPI
- `nx-gs-adapter-api/src/main/java/app/l2nx/gs/adapter/api/spi/RuntimeEntityMapping.java`
  [planned] — per-entity mapping contract
- `nx-gs-adapter-api/src/main/java/app/l2nx/gs/adapter/api/spi/RuntimeRow.java`
  [planned] — `{ long pk, T dto }` row record
- `nx-gs-adapter-api/src/main/java/app/l2nx/gs/adapter/api/kafka/sync/runtime/character/CharacterRuntimeDto.java`
  [planned] — wire DTO

Bohpts-side (in `bohpts-core` repo, not this monorepo):

- `bohpts-core/core/src/main/java/l2e/gameserver/l2nx/BohptsRuntimeStateProvider.java`
  [planned] — Tier-2 impl, sibling of `BohptsDbSchemaProvider`
- `bohpts-core/core/src/main/java/l2e/gameserver/l2nx/CharacterRuntimeMapping.java`
  [planned] — `RuntimeEntityMapping<CharacterRuntimeDto>` for the `character` entity
- `bohpts-core/core/src/main/resources/META-INF/services/app.l2nx.gs.adapter.api.spi.RuntimeStateProvider`
  [planned] — ServiceLoader descriptor

## Key components

- **RuntimeSyncModule** [planned] (implements R1) — `AdapterModule` entry point.
  Resolves `RuntimeStateProvider` via ServiceLoader at `start()`, validates the topic map
  on `onConnect` (per R7), spins up one `EntityTickLoop` per declared
  `RuntimeEntityMapping`, surfaces module health on heartbeat (R12).
- **EntityTickLoop** [planned] (implements R5, R8) — daemon-thread per entity. Each tick:
  call `mapping.snapshot()`, hash each row via `mapping.hash(dto)` into a fresh
  `Long2LongMap`, diff vs previous, publish via `SyncEventPublisher`, swap in only the
  acked subset of the new snapshot. All exception handling at this boundary.
- **SyncEventPublisher** [planned] — wraps `NxKafka` producer. Reuses the same
  `inFlight`-future + flush-at-tick-end mechanism `db-sync` has on a per-window basis;
  runtime-sync has no windows, so the equivalent flush boundary is the tick itself
  (parallel implementation, not literal reuse — module isolation).
- **Fnv1a64** (implements R6) — `app.l2nx.gs.commons.hash.Fnv1a64` from published
  `:nx-gs-commons` artifact. Public API: `long start()`, `long mix(long state, long
  value)` / `mix(state, int)` / `mix(state, boolean)` / `mix(state, CharSequence)`.
  Provider's `mapping.hash(dto)` chains these over fields it cares about. Engine
  never inspects the hash, only compares longs — algorithm is the provider's choice;
  FNV-1a is the recommended default.
- **RuntimeStateProvider SPI** [planned] (implements R2, R3, R4) — Tier-2 SPI on
  `nx-gs-adapter-api`. Mirror shape to `DbSchemaProvider`: `schemaName()` +
  `mappings()`. ServiceLoader descriptor expected at
  `META-INF/services/app.l2nx.gs.adapter.api.spi.RuntimeStateProvider`.
- **RuntimeEntityMapping<T> SPI** [planned] (implements R4) — per-entity contract:
  `entityName()`, `dtoType()`, `snapshot()`, `hash(T)`. Notice the absence of any
  `mapRow(ResultSet)` or DB-schema concepts — runtime SPI is pure-Java, host-internal.
- **CharacterRuntimeDto** [planned] (implements R10) — Java 8 POJO in
  `kafka.sync.runtime.character` package. `id` non-null, all other fields `@Nullable
  Integer`. Hand-written builder, equals/hashCode/toString.
- **BohptsRuntimeStateProvider** [planned] (implements R9) — bohpts-side concrete
  provider. Lives in `bohpts-core/l2e.gameserver.l2nx`, sibling of
  `BohptsDbSchemaProvider`. Single mapping for the `character` entity.

## Data flows

End-to-end per tick (one entity, e.g. `character`):

1. `EntityTickLoop` wakes (every 10s by default)
2. Calls `mapping.snapshot()` → `Iterable<RuntimeRow<CharacterRuntimeDto>>`
   (bohpts impl: `new ArrayList<>(GameObjectsStorage.getPlayers())`, filtered by
   `player.isOnline()`)
3. For each `RuntimeRow{pk, dto}`:
    - Compute `hash = mapping.hash(dto)` (FNV-1a 64-bit over vitals + coords)
    - Insert `{pk → hash}` into `currentSnapshot: Long2LongMap`
4. Diff `currentSnapshot` vs `prevSnapshot`:
    - PK in current, not in prev → emit `SyncEvent.create(pk, dto)`
    - PK in both, hashes differ → emit `SyncEvent.update(pk, dto)`
    - PK in prev, not in current → no-op (drop silently from tracking)
5. For each emit: `SyncEventPublisher.publish(topic, pk, event)` → returns
   `CompletableFuture<Void>`; future enters `inFlight: Map<pk, Future>`
6. Wait up to `publish-flush-seconds` for futures
7. For each acked future → write `{pk → hash}` from `currentSnapshot` into the
   "carrier-forward" snapshot for next cycle. Failed futures: leave previous
   `prevSnapshot[pk]` (if any) untouched → forces replay next tick
8. Replace `prevSnapshot = carrierForward`
9. Update heartbeat stats: `lastTickEpochMs`, `lastTickDurationMs`,
   `lastTickChanges = NEW + CHANGED`, `rowCount = currentSnapshot.size()`

## Data model

In-memory only; no DB tables, no persistence.

- **prevSnapshot** [planned] — `Long2LongMap` per entity. Cleared on `onDisconnect`,
  repopulated on next reconnect's first tick (every entity emits as NEW after
  reconnect — re-syncs the platform's live view).

Wire DTO (Kafka payload):

- **CharacterRuntimeDto** [planned] — `kafka.sync.runtime.character.CharacterRuntimeDto`
  in `nx-gs-adapter-api`. Field set per R10. Wrapped in `SyncEvent<CharacterRuntimeDto>`
  envelope (the same envelope `db-sync` uses; lives at `kafka.sync.db` and is reused).

## Integration points

- **Tier-1 SPI: `AdapterModule`** [planned] — ServiceLoader registration in
  `META-INF/services`. `nx-gs-adapter-core` discovers `RuntimeSyncModule` alongside
  `DbSyncModule`; both modules coexist.
- **Tier-2 SPI: `RuntimeStateProvider`** [planned] — defined in `nx-gs-adapter-api`,
  consumed by `nx-gs-runtime-sync-core` engine. Bohpts-core ships
  `BohptsRuntimeStateProvider` impl + ServiceLoader descriptor.
- **`ConnectContext.syncTopics().runtime()`** [planned] — engine reads its per-entity
  Kafka topic from this map. Map shape and adapter-side parsing are owned by
  [`adapter-bootstrap` R17](../adapter-bootstrap/spec.md).
- **`HeartbeatEvent.enabledModules`** [planned] — surface
  `{name: "runtime-sync", state, stats: {entities: [...]}}` per R12. Same envelope
  shape as `db-sync` — operators read both modules side by side.
- **bohpts in-memory store** [planned] —
  `l2e.gameserver.model.GameObjectsStorage.getPlayers()` is the read source for the
  `character` entity. Returns a `Collection<Player>` view over a napile
  `CHashIntObjectMap` (concurrent-safe for read-only iteration). Provider wraps in
  `new ArrayList<>(...)` for stable iteration and filters `player.isOnline()` to
  exclude just-logged-out instances still present in the map. Read-only — never
  mutates game state.
- **`NxKafka` producer** — reused via `SyncEventPublisher`. No new Kafka init code; the
  producer instance built at adapter bootstrap (per `adapter-bootstrap` R6) is shared
  across all sync modules.

## Decisions

- **Decision:** Pull-based snapshot+diff per tick, not event-driven push.
  **Why:** Pull keeps the SPI surface small (one `snapshot()` method) and non-invasive
  on the host code-path. Event hooks (`onHpChange`, `onLogin`, `onAutofarmToggle`) would
  require touching dozens of game-server call sites and risk listener-list contention on
  hot threads (regen ticks fire hundreds of times per second across thousands of
  players). The trade-off — sub-tick freshness for high-frequency events — is acceptable
  per the project's existing live-data SLA (10s freshness for vitals).

- **Decision:** Recommended hash algorithm = FNV-1a 64-bit, lifted to published
  `:nx-gs-commons` (`app.l2nx.gs.commons.hash.Fnv1a64`).
  **Why:** `db-sync` uses MySQL-native CRC32 because the hash runs on the DB side; the
  algorithm is forced by the platform. Runtime is pure Java, free choice. Considered:
  CRC32 (`java.util.zip.CRC32`, JDK), CRC32C (Java 9+, ruled out by Java 8 baseline),
  xxHash64 / MurmurHash3 (added dep, overkill). FNV-1a 64-bit is the sweet spot:
  ~15 LOC, zero deps, mul+xor (faster than CRC32 table lookup), 64-bit collision safety
  (~0% on 10k entries vs ~1% birthday collision for 32-bit on the same set), structural
  hashing of typed fields skips intermediate string serialization. Engine treats the
  returned long as opaque — provider can swap the algorithm without engine changes.

- **Decision:** No tombstone on logout / disconnect.
  **Why:** `db-sync` owns "permanently gone" (clan disbanded, character deleted).
  Runtime "absence" is transient — the same character will be back on next login. Platform
  consumers treat the last-published runtime state as "stale snapshot" by their own
  freshness logic (e.g. cross-reference with heartbeat liveness, or apply a TTL on
  read). Emitting tombstones every logout would double the runtime topic's event rate
  and require subscribers to distinguish "char logged out" from "char actually deleted"
  — they get the latter from `db-sync` already.

- **Decision:** `entityName` collision between modules is allowed; namespace separation
  is in `syncTopics`.
  **Why:** Operator-friendly mental model: `character` is a domain concept, present in
  both DB and runtime data sources. Forcing `character` (db) vs `character_runtime` (runtime)
  in the entity name space leaks implementation source into domain vocabulary. The
  ConnectResponse refactor (`syncTopics: { db, runtime, dp }`) provides clean
  resolution at topic-lookup time without polluting entity names.

- **Decision:** Per-module `publish-flush-seconds` —
  `l2nx.runtime-sync.publish-flush-seconds` (default 5s) is independent of
  `l2nx.cdc-engine.publish-flush-seconds`.
  **Why:** Tick cadences differ (10s for runtime vs 60s for cdc-engine) and the flush
  window should be sized against the corresponding tick budget. Sharing one knob would
  force operators to compromise — set it long enough for cdc and runtime tail latency
  bites; set it short and cdc starts replaying frequently. Separate keys cost nothing
  in code (each engine reads its own property) and let each module tune independently.

- **Decision:** Snapshot state is in-memory only, wiped on `onDisconnect`, rebuilt on
  reconnect.
  **Why:** Same persistence model as `db-sync`. Persisting snapshot to disk would
  require a write path on disconnect (operator-hostile during shutdown) and a recovery
  path on next start. Reconnect-and-re-sync is acceptable: first tick after reconnect
  publishes everything as NEW, ~10k events on the wire, finishes in seconds.

## Extension points

- **Add a new runtime entity** — declare an additional `RuntimeEntityMapping<NewDto>`
  in the host's `RuntimeStateProvider.mappings()` list. Engine spins up an additional
  daemon thread automatically. Platform side must add the corresponding entry to
  `ConnectResponse.syncTopics.runtime[newEntityName]`.

- **Add a new tenant** — implement `RuntimeStateProvider` in the tenant's host JAR with
  a `META-INF/services/app.l2nx.gs.adapter.api.spi.RuntimeStateProvider` descriptor.
  Same plug-in shape as `DbSchemaProvider`. No artifact published to Maven Central; the
  provider lives in the tenant's private repo.

- **Per-entity tick rate override (Could R13)** — engine reads
  `l2nx.runtime-sync.entities.<name>.tick-interval-seconds` first, falls back to global
  `l2nx.runtime-sync.tick-interval-seconds`, then to default (10s).

- **Explicit backpressure (deferred)** — when a high-cardinality runtime entity ships,
  add per-entity rate limiter at the publisher boundary (records/sec cap with sleep
  back-off) and/or convert snapshot iteration to a streaming pattern (don't buffer the
  full 1M-row snapshot; iterate-and-publish with periodic flush). Engine's current
  contract (single-shot `snapshot()` call) accommodates streaming via a lazy
  `Iterable` impl on the provider side without SPI breakage.
