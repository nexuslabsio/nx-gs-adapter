# Adapter Bootstrap

## Problem

Игровые серверы (L2J / Lucera / Essence-форки) не могут получить свои Kafka-creds и
установить соединение с L2NX-платформой без чёткого bootstrap-флоу: адаптер должен
найти server-key в окружении host JVM, обменять его через `POST /api/tenants/servers/connect`
на платформенные creds, поднять Kafka producer и подтверждать живость через
heartbeat. Без этого slice'а ни один nx-* модуль синков (db-l2j, dp-l2j) не может
стартануть — это foundation feature всего game-server adapter'а. Аудитория:
операторы (запускают адаптер с server-key); следующие slice'ы (модули синков,
ConfigWatcher, MetricsPusher) полагаются на готовый bootstrap.

## Requirements

**Must:**

- [todo] R1. Adapter MUST resolve `serverKey` on startup from one of three sources, in priority
  order:
    1. JVM system property `-Dl2nx.gs-key=<value>`
    2. Environment variable `L2NX_GS_KEY=<value>`
    3. Classpath file `l2nx.properties` with key `l2nx.gs-key=<value>`
       If none yields a value — fail with `IllegalStateException` whose message lists all three options.

    - SC1. Resolution MUST be zero-dep — only JDK `System.getProperty`/`getenv`,
      `ClassLoader.getResourceAsStream`, and `java.util.Properties`. No SnakeYAML, no Spring,
      no third-party config library.
- [todo] R2. Adapter MUST validate the resolved server-key format before any platform call: prefix
  `nx_sk_` + total length 38 chars (matches `Base62GameServerKeyGenerator` in nx-tenants).
  Invalid format → `IllegalStateException` thrown from `start()`.
- [todo] R3. Adapter MUST resolve `platformUrl` from the same three-source chain (key
  `l2nx.platform-url`), falling back to a hardcoded default when none is provided. `platformUrl`
  is the bare host (scheme + host + optional port), NOT a full base URL with context-path.
    - SC2. Hardcoded default `platformUrl` MUST be the production nx-tenants public host
      (e.g. `https://api.l2nx.app`); exact public DNS host is decided at first prod deploy
      (open question).
- [todo] R4. Adapter MUST POST to `{platformUrl}/api/tenants/servers/connect` with header
  `Authorization: Bearer <serverKey>` and JSON body `{"adapterVersion": "<version>"}`, using JDK
  `HttpURLConnection`. JSON serialization via Gson. The `/api/tenants` servlet context-path of
  nx-tenants is owned by the adapter (hardcoded into the request path), not by `platformUrl`.
- [todo] R5. Adapter MUST handle `/connect` HTTP responses per the platform contract:
    - 200 → state `ACTIVE`, parse `ConnectResponse`, proceed to Kafka init
    - 401 → state `FAILED`, no retry (key invalid / unknown)
    - 403 (code `GAME_SERVER_DEACTIVATED`) → state `REJECTED`, no retry
    - 409 (code `KAFKA_CREDENTIALS_MISSING`) → state `DEGRADED`, retry with backoff
    - 5xx / network / timeout → state `DEGRADED`, retry with backoff
    - SC3. Backoff schedule for retryable failures: 30s → 1m → 2m → 5m capped (matches
      architecture v2 §6.4).
- [todo] R6. Adapter MUST initialize Kafka producer via `nx-gs-kafka` after a successful `/connect`,
  composing builder properties from `ConnectResponse.kafka`:
    - `bootstrap.servers` ← `kafka.bootstrap`
    - `security.protocol` ← `kafka.securityProtocol`
    - `sasl.mechanism` ← `kafka.saslMechanism`
    - `sasl.jaas.config` ← `org.apache.kafka.common.security.scram.ScramLoginModule required
      username="<saslUsername>" password="<saslPassword>";`
      Kafka init MUST NOT block on broker reachability — `nx-gs-kafka` is graceful when the broker
      is unreachable.
- [todo] R7. Adapter MUST publish a heartbeat message to `kafka.topics.heartbeat` every 60
  seconds (Kafka message key = `serverId`). Payload type is `HeartbeatMessage` defined in
  `nx-gs-adapter-api` under `app.l2nx.gs.adapter.api.kafka` (single source of truth — the
  platform-side consumer in `server-registration` R9–R11 will compile against the same
  type). Fields: `serverId`, `adapterVersion`, `uptime` (seconds since the most recent
  successful `/connect` — session uptime, resets on reconnect), `enabledModules` (empty
  list in MVP). Gson serializes the POJO to JSON.
    - SC4. Heartbeat interval = 60s (matches `server-registration` spec R9 / SC4).
- [todo] R8. Adapter MUST expose a public API `NxAdapter`:
    - `static NxAdapter start()` — fire-and-forget bootstrap (returns immediately, all work
      runs on daemon threads)
    - `AdapterState state()` — current FSM state; returns `CLOSED` after `shutdown()` completes
    - `void shutdown()` — graceful stop: cancel heartbeat scheduler, close Kafka producer,
      transition to `CLOSED`, emit a final `onStateChange(CLOSED)` callback. Idempotent.
- [todo] R9. Adapter MUST register a JVM shutdown hook on `start()` that invokes `shutdown()`
  idempotently for graceful resource cleanup.
- [todo] R10. Adapter MUST never propagate exceptions to host-JVM threads. Every daemon-thread
  entry point (connect retry loop, heartbeat scheduler tick, Kafka producer callback) MUST
  catch `Throwable`, log via the adapter's logging facade, and never re-throw.
    - SC5. Each daemon-thread entry point has a unit test that asserts the runnable does NOT
      throw when the wrapped logic throws.

**Should:**

- [todo] R11. Adapter SHOULD expose an `onStateChange(Consumer<AdapterState>)` callback that the
  host (game core) can register before `start()` to surface lifecycle transitions in operator
  UI / logs. Callback dispatch is fire-and-forget on the state-change thread; host is
  responsible for handing off to its own thread if needed.

**Could:**

- [todo] R12. Adapter COULD log the resolved config source at INFO on startup (e.g.
  `server-key from system property` / `from env L2NX_GS_KEY` / `from classpath l2nx.properties`),
  always redacting the key value (e.g. `nx_sk_xxxx...xxxx`).
- [todo] R13. Adapter COULD support an `l2nx.adapter-version` config override for testing;
  default — read from the JAR manifest `Implementation-Version` attribute.

**Non-goals:**

- ServiceLoader-based discovery of `AdapterModule` — deferred to the next slice
  (`adapter-modules`). MVP heartbeat carries an empty `enabledModules` list.
- `ConfigWatcher` — Kafka consumer for `nexus.adapter.sync-config` (R17) and per-tenant
  credentials topic (R16, R18) — deferred.
- `MetricsPusher` (Pushgateway) — deferred.
- Adapter-side handling of `RECONNECT_REQUIRED` commands (R18 in `server-registration`) —
  deferred.
- DB and datapack sync modules (`nx-gs-adapter-db-*`, `nx-gs-adapter-dp-*`) — separate
  features per architecture v2 §3.

## Open questions

- [resolved: `ConnectRequest` / `ConnectResponse` / `KafkaConfig` / `Topics` are migrated
  from `nx-tenants/api/rest/dto/` (Lombok-`@Builder` records) to
  `nx-gs-adapter-api/src/main/java/app/l2nx/gs/adapter/api/rest/` as Java 8 POJOs in this slice
  (REST package — the wire shape of `POST /servers/connect`). nx-tenants becomes a consumer
  of `app.l2nx:nx-gs-adapter-api` via Gradle composite include. Validation annotations
  (`jakarta.validation`) stay out of the api lib — manual validation lives in the nx-tenants
  `AdapterController`. The first published version of the api artifact is **0.1.0**.]
- [resolved: `HeartbeatMessage` ships in the 0.1.0 release of `nx-gs-adapter-api` under
  `app.l2nx.gs.adapter.api.kafka` — single source of truth for the wire shape, same as
  REST DTOs. Adapter is the producer in this slice; the platform-side consumer
  (`server-registration` R9–R11) will compile against the same type when implemented.
  Java 8 POJO with hand-written builder, no validation annotations.]
- [resolved: `state()` and `onStateChange` emit `CLOSED` after `shutdown()` — mirrors
  `nx-gs-kafka`'s `NxKafkaState.CLOSED`. Predictable lifecycle for operators reading the state
  machine; architecture v2 §6.4 is silent on `CLOSED` but doesn't forbid it.]
- [resolved: heartbeat-payload `uptime` is seconds since the most recent successful
  `/connect` (session uptime, resets on reconnect). Platform-side dashboards interpret this
  as "current adapter session lifetime"; on reconnect the counter starts fresh, matching
  the 1:1 heartbeat-lock semantics in `server-registration`.]
- [assumed: Default `platformUrl` is `https://api.l2nx.app` (bare host). The adapter appends
  the nx-tenants servlet context-path `/api/tenants` itself when constructing the
  `/connect` URL. Exact public DNS host is decided at first prod deploy.]
- [assumed: Adapter version is read from the JAR manifest's `Implementation-Version`
  attribute via `getClass().getPackage().getImplementationVersion()`, with fallback to
  `"0.0.0-unknown"` for IDE / test runs.]
- [assumed: `IllegalStateException` thrown from `start()` during config resolution propagates
  to the caller (game-core init code). Catching it is the operator's responsibility — adapter
  logs and re-throws. After `start()` returns successfully, every other failure is
  internalized.]
- [NEEDS CLARIFICATION: R7 + the resolved Open Question above commit `HeartbeatMessage` to
  ship in `nx-gs-adapter-api` 0.1.0 under `app.l2nx.gs.adapter.api.kafka`, but the file is
  currently missing — the api subproject contains only the REST DTOs (`ConnectRequest`,
  `ConnectResponse`, `KafkaConfig`, `Topics`). Decide before tagging `api/v0.1.0`:
  (a) add the `HeartbeatMessage` POJO to api now, or (b) defer the type to a later api
  version and revise the resolution wording to match.]

## Links

- Architecture: `nx-gs-adapter-architecture-v2.md` (working copy at repo root —
  will move to `docs/architecture.md`)
- Server-side `/connect`:
  [
  `nx-tenants/docs/features/server-registration/spec.md`](../../../../nx-tenants/docs/features/server-registration/spec.md)
- Server-side Kafka creds delivery:
  [
  `nx-tenants/docs/features/tenant-registration/spec.md`](../../../../nx-tenants/docs/features/tenant-registration/spec.md)
