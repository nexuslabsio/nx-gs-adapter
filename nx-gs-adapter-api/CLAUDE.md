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
  `ConnectResponse`, `KafkaConfig`, `SyncTopics`, `MessagingTopics`)
- `app.l2nx.gs.adapter.api.kafka` — Kafka message payloads + header contract
  (`HeartbeatEvent`, `NxHeaders`)
- `app.l2nx.gs.adapter.api.kafka.events.<family>` — outbound discrete-fact event
  DTOs grouped by family. Single-event families take the concrete type on the
  publish method directly; multi-event families bind on an abstract base and
  dispatch on the platform via the `Nx-Message-Type` header. Shipped families:
    - `events.premiumpurchase` — `PremiumPurchaseEvent` (final) +
      `PurchaseItem` / `PurchaseService` / `Payment` + `WellKnownServices`
      constants. Single-event family; per-fact, host-pushed via
      `NxEvents.publishPremiumPurchase(PremiumPurchaseEvent)`.
    - `events.serveronline` — `ServerOnlineSnapshotEvent` (final, UUIDv7
      `eventId` + open `Map<String, Long> buckets`) +
      `WellKnownServerOnlineBuckets` lower_snake_case constants (`total` /
      `online` / `real` / `offline_trade` / `fishing` / `phantoms`).
      Single-event family; periodic snapshots, host-pushed via
      `NxEvents.publishServerOnlineSnapshot(ServerOnlineSnapshotEvent)` on
      a host-managed cadence.
    - `events.privatestore` — `PrivateStorePurchaseEvent` (closed-deal facts)
        + `PrivateStoreSnapshotEvent` (per-`(itemId, side)` order book) +
          `TradeLine` / `Offer` line types + `PrivateStoreSide` enum +
          `WellKnownElements` constants. Multi-event family (no abstract base);
          both subtypes ride one topic, host-pushed via per-subtype methods
          `NxEvents.publishPrivateStorePurchase(PrivateStorePurchaseEvent)` /
          `NxEvents.publishPrivateStoreSnapshot(PrivateStoreSnapshotEvent)`.
- `app.l2nx.gs.adapter.api.kafka.commands` — inbound command marker `NxCommand`,
  reply envelope `CommandResult<R>`, structured `ErrorCode` enum. Future concrete
  command DTOs ship under `kafka.commands.<group>.*` (group = code-org bucket:
  `character` / `item` / `mail` / `account`); the topic remains single, the
  package split is for Javadoc / IDE discovery only.
- `app.l2nx.gs.adapter.api.kafka.ops` — operational telemetry payloads
  (`HeartbeatEvent`, `ModuleStatus`, `EntityStats`, `PoolStats`, `EventsStats`,
  `CommandsStats`)
- `app.l2nx.gs.adapter.api.spi` — SPIs: Tier-1 `AdapterModule`, Tier-2
  `DbSchemaProvider` / `RuntimeStateProvider`, Tier-3 `JdbcConnectionSource`,
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
- **`SyncEvent` DELETED payload.** `payload=null` on `DELETED` ops no longer
  claims Kafka-tombstone semantics: topics use bounded retention, not log
  compaction, so consumers MUST explicitly handle the `DELETED` op (do not
  rely on the null-value tombstone protocol).
- **Identifier validation.** Any SQL identifier passed via `EntityMapping`,
  `PrimarySource`, or `ChildSource` (tableName / pkColumn / fkColumn /
  hashedColumns) MUST match `^[A-Za-z_][A-Za-z0-9_]{0,63}$`. Schema-qualified
  names (`schema.table`), quoted identifiers, and anything outside that pattern
  are rejected at engine start — no runtime quoting / escaping is performed.

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
