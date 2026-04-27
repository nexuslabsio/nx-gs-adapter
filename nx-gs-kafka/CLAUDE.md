# nx-gs-kafka

Subproject of the `nx-gs-adapter` monorepo. See [`../CLAUDE.md`](../CLAUDE.md) for repo-wide
conventions (per-module slash-namespaced versioning, Maven Central publishing flow, license,
shared `:nx-log`).

## Purpose

Lightweight Kafka client facade for Java 8+ host JVMs (game-server cores embedding the
adapter). Published as `app.l2nx:nx-gs-kafka`. Scoped to the game-server adapter use case —
package root `app.l2nx.gs.kafka` makes that explicit.

## Package layout

- `app.l2nx.gs.kafka` — `NxKafka`, `KafkaConfig`, `KafkaState`, `KafkaException`
- `app.l2nx.gs.kafka.producer` — `NxProducer`, `DefaultNxProducer`
- `app.l2nx.gs.kafka.consumer` — `NxConsumer`, `NxConsumerGroup`, `ReplyContext`
- `app.l2nx.gs.kafka.serde` — `GsonSerializer`, `GsonDeserializer` (Kafka-side
  `org.apache.kafka.common.serialization.*` impls)

Logging uses `app.l2nx.log.NxLog` from sibling `:nx-log` subproject — shadow-included into
the published jar so consumers don't see a separate `nx-log` Maven dep.

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
- **SLF4J is optional** (`compileOnly`). Library code MUST use `app.l2nx.log.NxLog` —
  never `import org.slf4j.*` directly.
- **No compression libs in transitives** — `snappy-java`, `lz4-java`, `zstd-jni` excluded
  in `build.gradle.kts`.
- **Public API → Javadoc mandatory** — every type a consumer touches (`NxKafka`,
  `KafkaConfig`, `NxProducer`, `NxConsumer`, `ReplyContext`) carries Javadoc.

## Versioning

Slash-namespaced tag `gs-kafka/vX.Y.Z` releases this module independently. Fallback when no
`-Pnx-gs-kafka.version=...` is passed: the literal in this module's `build.gradle.kts`.
Release flow lives in the monorepo root — see [`../CLAUDE.md`](../CLAUDE.md).

## Testing

- **Naming**: `methodName_shouldExpectedBehavior` or `methodName_shouldExpectedBehavior_whenCondition`
- Unit tests — JUnit 5
- Integration tests — `@Tag("integration")` + `@Testcontainers(disabledWithoutDocker = true)`.
  Excluded by `-PexcludeTags=integration` in the publish CI; run locally with Docker.
