# nx-gs-kafka

Lightweight Kafka client library for Java 8+ game server cores. Designed as a thin, convenient facade over
Apache Kafka with minimal dependencies and zero impact on game server stability.

## Features

- **Java 8 compatible** — works with any L2 game server core
- **Minimal dependencies** — only `kafka-clients` + `gson`, no Spring, no Jackson
- **Non-blocking** — Kafka unavailability never affects game server operation
- **Auto-reconnection** — background health checks, automatic state recovery
- **Smart logging** — auto-detects SLF4J; falls back to console if no logging framework present
- **Drop-in ready** — use via Maven/Gradle or just add the JAR to classpath

## Installation

### Gradle

<!-- @formatter:off -->
```groovy
dependencies {
    implementation 'app.l2nx:nx-gs-kafka:0.0.1'
}
```
<!-- @formatter:on -->

### Maven

<!-- @formatter:off -->
```xml
<dependency>
    <groupId>app.l2nx</groupId>
    <artifactId>nx-gs-kafka</artifactId>
    <version>0.0.1</version>
</dependency>
```
<!-- @formatter:on -->

### Drop-in JAR

Download `nx-gs-kafka-0.0.1-all.jar` and add it to your game server's classpath. This fat JAR includes
`kafka-clients` and `gson` — no other dependencies required.

## Quick Start

### Connect to Kafka

<!-- @formatter:off -->
```java
NxKafka kafka = NxKafka.configure()
    .brokers("kafka1:9092,kafka2:9092")
    .clientId("my-server")
    .build();
```
<!-- @formatter:on -->

That's it. The library connects to the broker, verifies the connection, and starts a background health check.
If the broker is unavailable, the game server starts normally — the library logs a warning and retries
automatically.

### Check Connection State

<!-- @formatter:off -->
```java
NxKafka.instance().isConnected();  // true / false
NxKafka.instance().state();        // KafkaState.CONNECTED
```
<!-- @formatter:on -->

### Produce Messages

<!-- @formatter:off -->
```java
// Fire-and-forget
NxKafka.instance().send("my.topic", new MyEvent(data));

// With partition key — messages with the same key go to the same partition (ordered)
NxKafka.instance().send("my.topic", playerId, new MyEvent(data));

// With callback
NxKafka.instance().send("my.topic", event, (metadata, exception) -> {
    if (exception != null) log.warn("Send failed", exception);
});

// With key and callback
NxKafka.instance().send("my.topic", playerId, event, (metadata, exception) -> {
    if (exception != null) log.warn("Send failed", exception);
});
```
<!-- @formatter:on -->

### Consume Messages

<!-- @formatter:off -->
```java
// Subscribe — creates a dedicated daemon thread per topic.
// groupId is REQUIRED — pick a stable identifier for the consumer group.
NxKafka.instance().subscribe("my.topic", "my-server.events", MyEvent.class, event -> {
    // Runs on NxKafka thread — dispatch to game thread if needed
    gameServer.enqueue(() -> handleEvent(event));
});

// Unsubscribe — stops poll thread, closes consumer
NxKafka.instance().unsubscribe("my.topic");
```
<!-- @formatter:on -->

Manual offset commit — the consumer commits the offset synchronously after the
handler returns normally. If the handler throws, the offset is NOT committed and
the record is redelivered on the next poll / on JVM restart. Make handlers
idempotent.

### Request-Reply (Spring Kafka compatible)

The library can act as a responder to requests from Spring Boot services using
`ReplyingKafkaTemplate`. The incoming `kafka_replyTopic` and `kafka_correlationId` headers are
automatically extracted and forwarded in the reply.

<!-- @formatter:off -->
```
Spring Boot (ReplyingKafkaTemplate)           GS (nx-gs-kafka)
   |                                              |
   |-- send to "gs.char.info.request" ----------->|
   |   headers:                                   |
   |     kafka_replyTopic = "platform.replies"    |
   |     kafka_correlationId = UUID               |
   |                                              |
   |   waits on Future<Reply>                     |-- handler receives request
   |                                              |   + ReplyContext
   |                                              |
   |<-- send to "platform.replies" ---------------|  replyTo.reply(charInfo)
   |    headers:                                  |
   |      kafka_correlationId = UUID (same)       |
   |                                              |
   |-- Future completes with reply                |
```

```java
NxKafka.instance().subscribe("gs.char.info.request", CharInfoRequest.class, (request, replyTo) -> {
    CharInfo info = gameServer.getCharInfo(request.getCharId());
    replyTo.reply(info);  // sends to kafka_replyTopic with same correlationId
});
```
<!-- @formatter:on -->

nx-gs-kafka is the **responder only** — reply topic management, correlation tracking, and timeouts
are handled by the Spring Boot requester side (`ReplyingKafkaTemplate`).

### Shutdown

<!-- @formatter:off -->
```java
NxKafka.instance().shutdown();
```
<!-- @formatter:on -->

A JVM shutdown hook is also registered automatically.

## Configuration

<!-- @formatter:off -->
```java
NxKafka kafka = NxKafka.configure()
    .brokers("kafka1:9092,kafka2:9092")       // required
    .clientId("bohpts-x20")                   // optional, default: nx-gs-kafka
    .connectTimeout(5, TimeUnit.SECONDS)      // optional, default: 5s, max: 60s
    .reconnect(true)                          // optional, default: true
    .reconnectInterval(30, TimeUnit.SECONDS)  // optional, default: 30s, max: 5min
    .producerCloseTimeout(Duration.ofSeconds(10))  // optional, default: 10s
    .gson(customGson)                         // optional, default: new Gson()
    .onStateChange(state -> log.info("Kafka state: {}", state))  // optional
    .property("security.protocol", "SASL_SSL") // any kafka-clients property
    .build();
```
<!-- @formatter:on -->

All standard `kafka-clients` properties can be passed via `.property(key, value)`.

### Producer durability defaults

The library pins durability-relevant producer defaults; per-property overrides via
`.property(...)` still win on a per-key basis:

| Property                                  | Default             |
|-------------------------------------------|---------------------|
| `acks`                                    | `all`               |
| `enable.idempotence`                      | `true`              |
| `max.in.flight.requests.per.connection`   | `5`                 |
| `linger.ms`                               | `10`                |
| `compression.type`                        | `gzip`              |
| `retries`                                 | `Integer.MAX_VALUE` |
| `delivery.timeout.ms`                     | `120000`            |

Consumer side: `enable.auto.commit=false` is pinned — commits happen synchronously
after handler success.

The custom `Gson` instance is used for both producer serialization and consumer deserialization.
The `onStateChange` callback is invoked on state transitions (CONNECTED ↔ DISCONNECTED, → CLOSED) —
dispatch to game thread if needed.

## Dependencies

| Dependency | Size | Required |
|---|---|---|
| `kafka-clients:3.6.2` | ~4.8MB | Yes |
| `gson:2.13.2` | ~280KB | Yes |
| `slf4j-api:2.0.17` | ~40KB | No (auto-detected) |

### Why kafka-clients 3.6.2?

The library targets Java 8. Apache Kafka dropped Java 8 support in `kafka-clients:3.7+`. Version 3.6.2 is the
latest Java 8 compatible client and is fully compatible with Kafka 3.7+ brokers (including Confluent 7.7.0)
via Kafka's backward-compatible protocol.

### Why Gson instead of Jackson?

Gson is a single ~280KB JAR with no transitive dependencies. Jackson requires multiple JARs (~2MB+) and
frequently causes classpath conflicts in game server environments. Gson is the safer choice for universal
compatibility.

### Compression

Transitive compression libraries (snappy, lz4, zstd) are excluded by default — they contain native binaries
(~9MB total) that can conflict across OS/arch combinations. Default `compression.type` is `none`.

If you need compression:

- `gzip` works out of the box (built into JDK)
- For `lz4`/`snappy`/`zstd`, add the specific library to your classpath and configure:

<!-- @formatter:off -->
```java
NxKafka.configure()
    .brokers("kafka:9092")
    .property("compression.type", "lz4")
    .build();
```
<!-- @formatter:on -->

## Logging

The library uses its own `NxLog` facade that auto-detects the logging setup:

| Classpath | Behavior |
|---|---|
| SLF4J + binding (logback, log4j2, etc.) | Logs via SLF4J |
| SLF4J without binding | Console fallback |
| No SLF4J | Console fallback |

Console fallback format:

```
[NxKafka] [INFO] 2026-04-06 12:00:00 NxKafka - Connected to cluster abc123, brokers: 3
```

## Reconnection

The library handles broker unavailability gracefully:

1. **On startup** — if the broker is unreachable, state is set to `DISCONNECTED` and the game server starts
   normally
2. **Background health check** — a single daemon thread periodically checks broker availability
3. **Auto-recovery** — when the broker comes back, state transitions to `CONNECTED`
4. **Built-in Kafka retries** — `KafkaProducer` and `KafkaConsumer` have their own reconnection logic for
   active produce/consume operations

## Error Handling

The library is designed to never interfere with game server operation:

- Kafka unavailable → logs warning, continues retrying in background
- Serialization error → logs error, skips the message
- Any internal exception → caught and logged, never propagates to game threads

## License

Apache License 2.0
