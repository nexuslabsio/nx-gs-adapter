# Adapter Bootstrap — tech

> Covers: spec.md

## Overview

Точка входа в адаптер — `NxAdapter.start()`, которую вызывает bootstrap-код игрового ядра.
Запуск non-blocking: сразу возвращается, всё дальнейшее выполняется на daemon-потоках.
Внутри — резолв конфига (`AdapterConfig` через sysprop / env / classpath chain), POST
`/connect` через JDK `HttpURLConnection` + Gson, инициализация Kafka producer'а через
`nx-gs-kafka`, и heartbeat-петля на `ScheduledExecutorService`. Lifecycle FSM (`AdapterState`)
— единственный шарящийся state, доступный наружу через `state()` и опциональный
`onStateChange` callback.

## Structure

- `nx-gs-adapter-core/src/main/java/app/l2nx/gs/adapter/core/` [planned] — runtime root package
    - `NxAdapter.java` [planned] — public entry point (`start`, `state`, `shutdown`,
      `onStateChange`)
    - `AdapterState.java` [planned] — lifecycle enum (`INIT`, `REGISTERING`, `ACTIVE`,
      `DEGRADED`, `FAILED`, `REJECTED`, `CLOSED`)
    - `config/AdapterConfig.java` [planned] — immutable holder (`serverKey`, `platformUrl`,
      `adapterVersion`)
    - `config/ConfigResolver.java` [planned] — sysprop → env → classpath properties chain
    - `connect/ConnectFlow.java` [planned] — POST `/connect` lifecycle, status-code dispatch,
      retry-with-backoff
    - `connect/ConnectClient.java` [planned] — thin `HttpURLConnection` wrapper with Gson
      body marshalling
    - `connect/BackoffSchedule.java` [planned] — 30s → 1m → 2m → 5m capped delay generator
    - `kafka/KafkaInitializer.java` [planned] — composes `NxKafka` builder from
      `ConnectResponse.kafka`
    - `heartbeat/HeartbeatService.java` [planned] — `ScheduledExecutorService`-driven 60s
      loop
    - `lifecycle/ShutdownHook.java` [planned] — registers JVM shutdown hook
    - logging via `app.l2nx.log.NxLog` from sibling `:nx-log` subproject (shadow-included
      into the published jar — see Integration points)
- `nx-gs-adapter-core/src/test/java/app/l2nx/gs/adapter/core/` [planned] — unit tests for
  `ConfigResolver`, `BackoffSchedule`, `ConnectFlow` (with WireMock), `HeartbeatService`
  (with mocked `NxKafka`).

## Key components

- **`NxAdapter`** [planned] (R8) — singleton-style facade. `start()` returns the instance,
  registers the JVM shutdown hook, kicks off the connect flow on a daemon `ExecutorService`.
  State transitions are atomic via `AtomicReference<AdapterState>`.
- **`ConfigResolver`** [planned] (R1, R2, R3) — pure JDK; reads from `System.getProperty` →
  `System.getenv` → `ClassLoader.getSystemClassLoader().getResourceAsStream("l2nx.properties")`
  parsed as `java.util.Properties`. Validates the server-key format (prefix `nx_sk_` + total
  length 38) up front.
- **`ConnectFlow`** [planned] (R4, R5) — drives the connect lifecycle. On 200 → state `ACTIVE`,
  triggers `KafkaInitializer` + `HeartbeatService.start`. On 401/403 → terminal failure
  (`FAILED` / `REJECTED`). On 409/5xx/network → schedules a retry via `BackoffSchedule`.
- **`ConnectClient`** [planned] (R4) — opens `HttpURLConnection`, writes JSON body via Gson,
  reads response, surfaces status code + parsed body. Sets `Connection: close` (avoid
  host-JVM connection-pool leaks). Reasonable timeouts: connect 5s, read 10s.
- **`BackoffSchedule`** [planned] (R5/SC3) — stateless `next(attempt)` returning a `Duration`
  from the canonical schedule, capped at 5 minutes.
- **`KafkaInitializer`** [planned] (R6) — translates `ConnectResponse.kafka` into a
  `NxKafka.configure().property(...).build()` call. Does NOT block on broker reachability —
  relies on nx-gs-kafka's graceful start.
- **`HeartbeatService`** [planned] (R7) — single-threaded `ScheduledExecutorService` (daemon,
  named `nx-adapter-heartbeat`). Each tick builds an `app.l2nx.gs.adapter.api.kafka.HeartbeatMessage`
  (defined in `nx-gs-adapter-api`) carrying `serverId`, `adapterVersion`,
  `uptime` = seconds since the most recent successful `/connect`, and an empty
  `enabledModules` list, then calls `NxKafka.send(topic, key, payload)`. The `uptime`
  clock is captured fresh on every successful `(re)connect`, so platform-side dashboards
  show session-uptime rather than adapter-uptime. Errors are logged, never propagated.
- **`ShutdownHook`** [planned] (R9) — `Runtime.getRuntime().addShutdownHook(...)` registered
  once on `start()`. Calls `shutdown()` idempotently.
- **Logging** — uses `app.l2nx.log.NxLog` from sibling `:nx-log` subproject (auto-detects
  `org.slf4j.LoggerFactory` via reflection; SLF4J on classpath → uses it; otherwise →
  formatted `System.out.println` fallback). Shadow-included into the published
  `nx-gs-adapter-core` jar so consumers don't see a separate `nx-log` Maven dep. Library
  code never imports SLF4J directly.

## Data flows

1. **Bootstrap** — game core calls `NxAdapter.start()` → `ConfigResolver.resolve()` builds
   `AdapterConfig` → `ConnectFlow` schedules POST on the daemon executor → state
   `INIT` → `REGISTERING`.
2. **Connect success** — `ConnectClient` parses `ConnectResponse` → `KafkaInitializer`
   builds the `NxKafka` instance → `HeartbeatService.start(serverId, kafka.topics.heartbeat)`
   → state `ACTIVE`.
3. **Connect retry** — non-terminal failure (409/5xx/network) → `BackoffSchedule.next(attempt)`
   → `ScheduledExecutorService.schedule(...)` re-runs the connect attempt → state stays
   `REGISTERING` on first-time bootstrap or transitions to `DEGRADED` if previously `ACTIVE`.
4. **Heartbeat tick** — every 60s `HeartbeatService` builds a `HeartbeatMessage` →
   `NxKafka.send(heartbeatTopic, serverId, message)`. nx-gs-kafka handles producer-side retries
   and reconnection; failed sends do not change adapter state.
5. **Shutdown** — JVM shutdown hook OR explicit `shutdown()` → cancel heartbeat scheduler →
   `NxKafka.shutdown()` → state `CLOSED`.

## Integration points

- **`:nx-gs-adapter-api`** (R4, R7) — sibling subproject in this monorepo. Provides
  `ConnectRequest`, `ConnectResponse`, `KafkaConfig`, `Topics` (migrated from
  `nx-tenants/api/rest/dto/` as Java 8 POJOs in this slice — they were Lombok-`@Builder`
  records there) under `app.l2nx.gs.adapter.api.rest`, plus `HeartbeatMessage` under
  `app.l2nx.gs.adapter.api.kafka`. `nx-tenants` consumes the same published artifact
  (`app.l2nx:nx-gs-adapter-api`) via Gradle composite include of the whole `nx-gs-adapter`
  repo with a `dependencySubstitution` mapping — single source of truth for the wire
  shape. Validation annotations (`jakarta.validation`) stay on the controller side; the
  api lib has zero runtime deps. First published version is **0.1.0**.
- **`:nx-gs-kafka`** (R6, R7) — sibling subproject. `NxKafka.configure().build()` for the
  producer; `NxKafka.send(topic, key, value)` for heartbeat. `:nx-gs-adapter-core` depends
  on it via `project(":nx-gs-kafka")`.
- **`:nx-log`** — sibling subproject (internal logging facade, package `app.l2nx.log`).
  Shadow-included into `:nx-gs-adapter-core` and `:nx-gs-kafka` jars at build time, NOT
  published as a separate Maven artifact. Auto-detects SLF4J via reflection, falls back
  to console output. Library code never imports SLF4J directly.
- **`nx-tenants` `POST /api/tenants/servers/connect`** (R4, R5) — wire contract; status codes
  per `server-registration` spec R7/R8 + `tenant-registration` spec R4.
- **JDK `HttpURLConnection`** (R4) — no third-party HTTP. Configured with explicit timeouts
  and `Connection: close`.
- **JDK `java.util.Properties`** (R1) — classpath `l2nx.properties` parsing.
- **Game-core JVM lifecycle** (R9) — `Runtime.getRuntime().addShutdownHook(...)`.

## Decisions

- **Three-source config chain (sysprop → env → classpath properties).** Sysprop wins for easy
  override during testing/CI; env vars are 12-factor / k8s-native; classpath file is the
  drop-in JAR scenario. `.properties` over YAML because SnakeYAML is a heavy dep for one
  config file (zero-deps principle).
- **Server-key validation up-front.** Fail fast with `IllegalStateException` before any
  network call when the format is wrong — avoids attributing format errors to "platform
  unreachable" symptoms.
- **`HttpURLConnection` over OkHttp / Apache HttpClient.** JDK-only stays consistent with the
  "minimum dependencies" principle. Adapter makes one POST + retries — the feature surface is
  trivial; OkHttp's connection pool is wasted on a single endpoint and would risk classpath
  conflicts in host JVMs.
- **Gson over Jackson.** Already in tree (nx-gs-kafka uses it); single ~280KB JAR vs Jackson's
  multi-JAR ~2MB+ footprint that frequently clashes with host-JVM classpath. Same rationale
  as nx-gs-kafka.
- **`ScheduledExecutorService` over a hand-rolled thread + `Thread.sleep`.** Allows clean
  cancellation on shutdown, named threads, graceful daemon termination.
- **`NxAdapter.start()` is fire-and-forget, non-blocking.** Game-core init code shouldn't
  wait on platform availability — host JVM continues booting normally even if `/connect`
  fails. Status surfaced via `state()` / `onStateChange`.
- **All daemon threads catch `Throwable`.** Game-server stability dominates — uncaught
  exceptions in adapter threads must NEVER bring down the host JVM. Same philosophy as
  `nx-gs-kafka`.
- **Backoff cap = 5 minutes.** Matches architecture v2 §6.4. After 5m the adapter keeps
  retrying at the cap (no give-up); operator intervention (revoke / fix tenant) is signalled
  via terminal `FAILED` / `REJECTED` states instead.
- **Heartbeat key = `serverId`.** Matches `server-registration` spec R9 (per-server message
  ordering on the consumer side).
- **No reflection-heavy DI / Spring.** Wiring inside `NxAdapter.start()` is plain `new`.
  Constructor injection only — keeps bytecode predictable across host classloaders.
- **`CLOSED` state shipped in the FSM.** Mirrors `nx-gs-kafka`'s `NxKafkaState.CLOSED` and gives
  operators a deterministic terminal state to query / observe via `onStateChange`. Adds one
  enum constant — no behavior cost.
- **`uptime` = seconds since `/connect`.** Session-scoped, resets on reconnect. Reflects "this
  adapter session has been online for X" rather than "this JVM has been booted for X" —
  matches the 1:1 heartbeat-lock semantics in `server-registration` (a new connect = a new
  session = a new lock).
- **DTO migration `nx-tenants` → `nx-gs-adapter-api`.** `ConnectRequest` / `ConnectResponse`
  / `KafkaConfig` / `Topics` were defined as Lombok-`@Builder` Java records in
  `nx-tenants/api/rest/dto/`; this slice converts them to Java 8 POJOs (hand-written
  builders, all-args public constructor for Jackson parameter-name binding) and lands them
  in `nx-gs-adapter-api/src/main/java/app/l2nx/gs/adapter/api/rest/`. nx-tenants becomes a
  consumer via composite include. Wire shape is unchanged. Trade-off: Spring side loses
  record syntactic sugar; gain is single source of truth for the contract and the adapter
  side gets the same types it sees on the wire — no parallel definitions to drift.
- **Package split `api.rest` vs `api.kafka`.** REST request/response DTOs live in
  `app.l2nx.gs.adapter.api.rest` (0.1.0: `ConnectRequest`, `ConnectResponse`, `KafkaConfig`,
  `Topics`). Kafka message payloads live in `app.l2nx.gs.adapter.api.kafka` (0.1.0:
  `HeartbeatMessage`). The split keeps wire-protocol concerns visually separate, mirrors
  how nx-tenants is structured internally (`api/rest/` vs `infra/kafka/`), and makes it
  obvious to a consumer where to look for the contract of a given transport.
- **`HeartbeatMessage` ships in 0.1.0 of the api lib.** Despite the platform-side
  consumer not existing yet (server-registration R9–R11), the type lives in
  `nx-gs-adapter-api` from day one — single source of truth for the wire shape, identical
  reasoning to the REST DTOs. Avoids a future "graduation" step where an inline POJO
  would need to be promoted, refactored at call-sites, and re-released across two
  artifacts.

## Extension points

- **New config sources** — extend `ConfigResolver` with an additional resolver function. The
  chain itself is a `List<Function<String, Optional<String>>>`; new sources slot in by
  appending to the list.
- **Custom HTTP transport** — `ConnectClient` is an interface; later slices can swap in
  OkHttp or another transport if a non-trivial HTTP feature (proxy, h2, mTLS) is needed.
  MVP ships only the `HttpURLConnection`-backed implementation.
- **Heartbeat enrichment** — `HeartbeatMessage.enabledModules` currently lands as `[]`. The
  next slice (`adapter-modules`) wires `ServiceLoader` discovery and populates this field
  with discovered module names.
- **Pluggable backoff strategies** — `BackoffSchedule` is an interface; later slices could
  add jittered exponential or fibonacci variants without touching `ConnectFlow`.
