# nx-gs-kafka

Subproject of the `nx-gs-adapter` monorepo. See [`../CLAUDE.md`](../CLAUDE.md) for repo-wide
conventions (per-module slash-namespaced versioning, Maven Central publishing flow, license,
shared `:nx-gs-log`).

## Purpose

Lightweight Kafka client facade for Java 8+ host JVMs (game-server cores embedding the
adapter). Published as `app.l2nx:nx-gs-kafka`. Scoped to the game-server adapter use case —
package root `app.l2nx.gs.kafka` makes that explicit.

## Package layout

- `app.l2nx.gs.kafka` — `NxKafka`, `KafkaConfig`, `KafkaState`, `KafkaException`
- `app.l2nx.gs.kafka.producer` — `NxProducer`, `DefaultNxProducer`
- `app.l2nx.gs.kafka.consumer` — `NxConsumer`, `ConsumerGroup` (internal, renamed
  from `NxConsumerGroup` per the "`Nx*` prefix only on client-facing singletons"
  convention), `ReplyContext`
- `app.l2nx.gs.kafka.serde` — `GsonSerializer`, `GsonDeserializer` (Kafka-side
  `org.apache.kafka.common.serialization.*` impls)

Logging uses `app.l2nx.gs.log.NxLog` from sibling `:nx-gs-log` subproject — shadow-included into
the published jar so consumers don't see a separate `nx-gs-log` Maven dep.

## Producer durability defaults

`DefaultNxProducer` pins durability-relevant defaults at build time; user
`KafkaConfig.Builder.property(...)` calls override them on a per-key basis:

| Property                                | Default             |
|-----------------------------------------|---------------------|
| `acks`                                  | `all`               |
| `enable.idempotence`                    | `true`              |
| `max.in.flight.requests.per.connection` | `5`                 |
| `linger.ms`                             | `10`                |
| `compression.type`                      | `gzip`              |
| `retries`                               | `Integer.MAX_VALUE` |
| `delivery.timeout.ms`                   | `120000`            |

`lz4` / `snappy` / `zstd` native libs are excluded in `build.gradle.kts` — only
`gzip` (JDK-built-in) works without classpath additions.

Consumer defaults: `enable.auto.commit=false`. `ConsumerGroup` commits sync after
handler success; on handler exception, the offset is NOT committed and the record
is redelivered on the next poll / on restart.

## Internals worth knowing

- **Single producer.** `DefaultNxProducer` uses ONE `KafkaProducer<byte[], Object>`.
  String keys are encoded to UTF-8 bytes at the boundary. This halves
  `buffer.memory` (saves ~32 MiB resident) and halves broker TCP connections vs
  the prior per-key-type producer pair.
- **Configurable close timeout.** `KafkaConfig.Builder.producerCloseTimeout(Duration)`
  bounds how long `KafkaProducer.close(...)` waits to drain in-flight sends
  during shutdown. Default 10s.
- **Idempotent shutdown.** A `CountDownLatch` guards `NxKafka.shutdown()` so that
  concurrent callers (e.g. application code + the JVM shutdown hook) all await
  the first caller's completion instead of racing each other.
- **Send-vs-close.** Operations are guarded by a `ReentrantReadWriteLock` —
  every `send(...)` takes the read lock, `close(...)` takes the write lock.
  No send can race a close.
- **Daemon threads carry an `UncaughtExceptionHandler`** that logs to `NxLog`
  at ERROR — silent thread death is impossible.
- **Persistent AdminClient.** The health-check `AdminClient` is created once at
  startup and reused for every `describeCluster` tick — not re-created on every
  health check.
- **Explicit consumer groupId.** `NxKafka.subscribe(topic, groupId, ...)`
  REQUIRES a groupId — the previous overload that reused `clientId` is gone.
- **`KafkaConfig` bounds.** `connectTimeoutMs <= 60_000`, `reconnectIntervalMs
  <= 5 * 60_000` — validated at `build()` time.

## Dependencies

Declared via `gradle/libs.versions.toml` at the monorepo root:

- `kafka-clients` — Apache Kafka client (compression libs `snappy-java`, `lz4-java`,
  `zstd-jni` excluded from transitives — they're native code that breaks classloader
  isolation in host JVMs)
- `gson` — JSON serialization
- `slf4j-api` — `compileOnly`, optional at runtime

## Constraints

- **Java 8 source + target** — host JVMs span Java 8 to 25+. Tests run on Java 11+.
- **Never propagate exceptions to host threads** — every public entry point catches and
  logs internally.
- **SLF4J is optional** (`compileOnly`). Library code MUST use `app.l2nx.gs.log.NxLog` —
  never `import org.slf4j.*` directly.
- **No compression libs in transitives** — `snappy-java`, `lz4-java`, `zstd-jni` excluded
  in `build.gradle.kts`.
- **Public API → Javadoc mandatory** — every type a consumer touches (`NxKafka`,
  `KafkaConfig`, `NxProducer`, `NxConsumer`, `ReplyContext`) carries Javadoc.

## Versioning

Slash-namespaced tag `kafka/vX.Y.Z` releases this module independently. Fallback when no
`-Pnx-gs-kafka.version=...` is passed: the literal in this module's `build.gradle.kts`.
Release flow lives in the monorepo root — see [`../CLAUDE.md`](../CLAUDE.md).

## Testing

- **Naming**: `methodName_shouldExpectedBehavior` or `methodName_shouldExpectedBehavior_whenCondition`
- Unit tests — JUnit 5
- Integration tests — `@Tag("integration")` + `@Testcontainers(disabledWithoutDocker = true)`.
  Excluded by `-PexcludeTags=integration` in the publish CI; run locally with Docker.
