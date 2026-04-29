# Adapter Bootstrap — tech

> Covers: spec.md

## Overview

Точка входа в адаптер — `NxAdapter.start()`, которую вызывает bootstrap-код игрового ядра.
Запуск non-blocking: сразу возвращается, всё дальнейшее выполняется на daemon-потоках.
Внутри — резолв конфига (`AdapterConfig` через file-first chain: properties-file → sysprop;
env-vars в 0.1.0 нет), POST `/connect` через JDK `HttpURLConnection` + Gson, инициализация
Kafka producer'а через `nx-gs-kafka`, и heartbeat-петля на `ScheduledExecutorService`.
Lifecycle FSM (`AdapterState`) — единственный шарящийся state, доступный наружу через
`state()` и опциональный `onStateChange` callback.

## Structure

- `nx-gs-adapter-core/src/main/java/app/l2nx/gs/adapter/core/` — runtime root package
    - `NxAdapter.java` [done] — public entry point (`start`, `state`, `shutdown`,
      `onStateChange`); singleton-style facade with idempotent `start()` (AtomicBoolean
      guard) and `synchronized` transition for atomic set + callback dispatch
    - `AdapterState.java` [done] — lifecycle enum (`INIT`, `REGISTERING`, `ACTIVE`,
      `DEGRADED`, `FAILED`, `REJECTED`, `DISABLED`, `CLOSED`)
    - `config/AdapterConfig.java` [done] — immutable holder (`serverKey`, `platformUrl`,
      `adapterVersion`, `enabled`)
    - `config/ConfigResolver.java` [done] — file-first chain (properties-file → sysprop):
      file path from `-Dl2nx.config-file` or classpath `l2nx.properties` fallback (with
      multi-match guard); string + boolean resolution variants; UTF-8 reads;
      `resolvePlatformUrl()` enforces `https://`, rejects query/fragment/missing-host
    - `lifecycle/StartupBanner.java` [done] — emits the multi-line L2NX ASCII banner +
      adapterVersion via the logging facade
    - `lifecycle/AdapterVersion.java` [done] — static helper resolving the JAR
      manifest's `Implementation-Version` (with `unknown` fallback); used
      by the banner before the config resolver runs
    - `connect/ConnectFlow.java` [done] — POST `/connect` lifecycle, status-code dispatch,
      retry-with-backoff via `AtomicInteger` attempt counter; `sanitize()` redacts
      `Bearer <token>` patterns from log messages
    - `connect/ConnectClient.java` [done] — interface; implementations encode transport /
      parse failures as `ConnectResult` rather than throwing
    - `connect/HttpURLConnectionConnectClient.java` [done] — JDK-only impl: Bearer auth,
      `Connection: close`, 5s/10s timeouts, `BufferedWriter`-wrapped output, 1 MiB hard
      cap on response body (host-JVM OOM defense), UTF-8 char-array read
    - `connect/ConnectResult.java` [done] — typed result envelope (success / httpError /
      ioFailure)
    - `connect/ErrorEnvelope.java` [done] — wire-shape `{code, message}` Gson-deserialized
      from 4xx/5xx response bodies
    - `connect/BackoffSchedule.java` [done] — interface
    - `connect/DefaultBackoffSchedule.java` [done] — canonical 30s → 1m → 2m → 5m capped
      delay generator
    - `kafka/KafkaInitializer.java` [done] — composes `NxKafka` builder properties
      (`security.protocol`, `sasl.mechanism`, `sasl.jaas.config` templated against
      `ScramLoginModule`) from `ConnectResponse.kafka` and delegates to a `KafkaFactory`;
      escapes `\` / `"` in credentials before inlining into the JAAS string
    - `kafka/KafkaFactory.java` [done] — interface (test seam over the
      `NxKafka.configure().build()` singleton); contract returns post-build `KafkaState`
      and forwards a state-change listener
    - `kafka/DefaultKafkaFactory.java` [done] — default impl bridging to
      `NxKafka.configure().build()`; shuts down any live `NxKafka` singleton before
      re-init so a `DEGRADED → ACTIVE` reconnect cycle that re-fetches creds is
      idempotent
    - `heartbeat/HeartbeatService.java` [done] — `ScheduledExecutorService`-driven 60s
      loop; tick wrapped in `SafeRunnable` so a runaway throwable can't cancel the
      schedule. `KafkaPublisher` interface is the test seam over
      `NxKafka.instance().send(topic, key, payload)`. `start(serverId, topic)`
      captures `connectInstant` fresh each call, so uptime is session-scoped
      (resets on reconnect)
    - `concurrent/SafeRunnable.java` [done] — static `wrap(Runnable, NxLog)`
      helper that swallows `Throwable` from the delegate. Applied at every
      adapter daemon-thread entry point (connect submit, heartbeat tick,
      shutdown hook)
    - heartbeat wire type lives in `nx-gs-adapter-api`
      (`app.l2nx.gs.adapter.api.kafka.ops.HeartbeatEvent`) — adapter-bootstrap
      `0.1.0` shipped fields `serverId`, `adapterVersion`, `uptime` only;
      `tenantId` / `tenantSlug` / `serverSlug` / `serverName` / `enabledModules`
      added by `adapter-modules` slice (api `0.5.0`); `uptime` renamed to
      `uptimeMs` (millisecond unit) in api/0.6.0 for consistency with
      `EntityStats.lastSyncEpochMs` and `SyncEvent.timestampEpochMs`
    - shutdown hook is registered inline inside `NxAdapter.start()` (no separate
      `ShutdownHook.java`); a `Thread` named `nx-adapter-shutdown` wrapping
      `SafeRunnable.wrap(INSTANCE::shutdown)`
    - logging via `app.l2nx.log.NxLog` from sibling `:nx-log` subproject (shadow-included
      into the published jar — see Integration points)
- `nx-gs-adapter-core/src/test/java/app/l2nx/gs/adapter/core/` — unit tests for
  `ConfigResolver`, `NxAdapter`, `ConnectFlow` (WireMock-backed, status dispatch via
  `@ParameterizedTest`), `DefaultBackoffSchedule`. `CapturingScheduler` is a hand-rolled
  `ScheduledExecutorService` test double (Mockito 5.x / Byte Buddy doesn't support Java
  25+, so we don't mock the JDK interface). Future: `HeartbeatService` (with mocked
  `NxKafka`) once heartbeat lands.

## Key components

- **`NxAdapter`** [done] (R8, R10, R11, R14) — singleton-style facade. `start()` is
  idempotent (an `AtomicBoolean started` guard logs a WARN and returns on duplicate
  invocation), wraps `ConfigResolver.resolve()` in a central `try { ... } catch (Throwable)`
  so config-resolution failures log via `NxLog` and transition to `FAILED` instead of
  bubbling out into the host JVM. If `config.enabled == false` it logs an INFO message,
  transitions `INIT → DISABLED`, fires the registered `onStateChange` callback, and
  returns inert (no daemon threads, no shutdown hook). Otherwise it kicks off the
  connect flow on a daemon `nx-adapter-connect` `ScheduledExecutorService`, arms the
  heartbeat scheduler, and registers a JVM shutdown hook (`nx-adapter-shutdown`) that
  delegates to `shutdown()`. State transitions go through a `synchronized` block that
  serializes set + callback dispatch so observers see consistent state when reading
  from inside the callback.
- **`ConfigResolver`** [done] (R1, R2, R3, R14) — pure JDK; two-source chain per key,
  **file-first**: properties-file lookup → `System.getProperty(key)` (sysprop is only
  consulted when the file does not provide the key, so the file is authoritative). The
  properties file is loaded once at resolver construction: if
  `System.getProperty("l2nx.config-file")` is set, that absolute path is read via
  `java.nio.file.Files` (operator-preferred — file lives anywhere on the filesystem);
  otherwise `ClassLoader.getResourceAsStream("l2nx.properties")` is used as the classpath
  fallback. Validates the server-key format (prefix `nx_sk_` + total length 38) up front.
  Provides `resolveString(key)` and `resolveBoolean(key, default)` variants — the latter
  parses `true`/`false` case-insensitively for the `l2nx.enabled` flag and returns
  `false` when no source supplies a value. Environment-variable resolution is intentionally
  absent in 0.1.0 (see Decisions).
- **`StartupBanner`** [done] (R15) — emits the L2NX ASCII wordmark and the resolved
  adapter version on `start()`, blank-line-padded so it's visually distinct from
  surrounding host-JVM logs. Plain text via the logging facade — no ANSI escape codes.
- **`ConnectFlow`** [done] (R4, R5) — `Runnable` driving the connect lifecycle on the
  daemon scheduler. Emits a coarse-grained `Outcome` per logical event (`STARTING` /
  `ACTIVE` / `TRANSIENT` / `FAILED` / `REJECTED`) — the orchestrator (`NxAdapter`)
  translates outcomes into state transitions, keeping the flow itself orchestrator-agnostic.
  Dispatch: 200 → invoke `onActiveResponse(ConnectResponse)` callback (orchestrator owns
  post-200 state — see `NxAdapter.initKafka`); if no callback wired, falls back to bare
  `Outcome.ACTIVE`; 401 → `FAILED` (terminal); 403 + `code=GAME_SERVER_DEACTIVATED` →
  `REJECTED` (terminal); 409 + `code=KAFKA_CREDENTIALS_MISSING` / 5xx / IOException →
  `TRANSIENT` + reschedule via `BackoffSchedule`; any other status → `FAILED`. Retry
  attempt counter is an `AtomicInteger` (visibility across consecutive scheduler tasks).
  `sanitize()` strips `Bearer\s+\S+` patterns from anything routed to `NxLog` (defense
  against server-key leakage via JDK exception messages).
- **`ConnectClient`** [done] (R4) — interface contract: never throws; transport / parsing
  failures are encoded in the returned `ConnectResult` (sum-type: `success(ConnectResponse)`
  / `httpError(int, ErrorEnvelope)` / `ioFailure(IOException)`). Default impl
  `HttpURLConnectionConnectClient` opens `HttpURLConnection`, writes JSON via Gson through
  a `BufferedWriter`, reads response with a 1 MiB hard cap (host-JVM OOM defense) and
  UTF-8 char-array reads (preserves payload bytes verbatim). Sets `Connection: close`,
  5s connect / 10s read timeouts.
- **`BackoffSchedule`** [done] (R5/SC3) — interface; default impl `DefaultBackoffSchedule`
  is stateless and returns `Duration.ofSeconds(30) → ofMinutes(1) → ofMinutes(2) →
  ofMinutes(5)`, capped at 5 minutes for all subsequent attempts.
- **`KafkaInitializer`** [done] (R6) — translates `ConnectResponse.kafka` into a property
  map (`security.protocol`, `sasl.mechanism`, `sasl.jaas.config`) and delegates to a
  `KafkaFactory`. The `sasl.jaas.config` is templated against
  `org.apache.kafka.common.security.scram.ScramLoginModule`; `\` and `"` in
  `saslUsername`/`saslPassword` are escaped before inlining. Returns the post-build
  `KafkaState`. Does NOT block on broker reachability — relies on nx-gs-kafka's graceful
  start.
- **`KafkaFactory` / `DefaultKafkaFactory`** [done] (R6) — `KafkaFactory` is the test
  seam over the `NxKafka.configure().build()` singleton (so `KafkaInitializer` can be
  unit-tested without standing up a real Kafka client). `DefaultKafkaFactory` implements
  it: shuts down any live `NxKafka` instance before re-init (for reconnect cycles), then
  composes the builder and calls `build()`.
- **State machine — Kafka coupling** [done] (R5, R6) — `NxAdapter` derives `ACTIVE` /
  `DEGRADED` from a combination of platform handshake outcome and `KafkaState`:
    - 200 from `/connect` + `KafkaState.CONNECTED` post-init → `ACTIVE`; latches `wasActive`
    - 200 + `KafkaState.DISCONNECTED` post-init → `DEGRADED` (Kafka background reconnect)
    - Subsequent `KafkaState.CONNECTED` (background recovery) → `ACTIVE`
    - Subsequent `KafkaState.DISCONNECTED` while in `ACTIVE`/`DEGRADED` → `DEGRADED`
    - `Outcome.TRANSIENT` while `wasActive=false` (initial handshake retry loop) → no-op,
      state stays `REGISTERING`; while `wasActive=true` → `DEGRADED`
    - `wasActive` is a permanent JVM-lifetime latch (only cleared in
      `resetForTesting()`) — terminal `FAILED`/`REJECTED`/`CLOSED` do not reset it
- **`HeartbeatService`** [done] (R7) — single-threaded `ScheduledExecutorService` (daemon,
  named `nx-adapter-heartbeat`). Each tick builds a
  `app.l2nx.gs.adapter.api.kafka.ops.HeartbeatEvent` carrying `serverId` (+
  `tenantId` / `tenantSlug` / `serverSlug` / `serverName` added by
  `adapter-modules`), `adapterVersion`, `uptimeMs` = milliseconds since the most
  recent successful `/connect`, and `enabledModules` from a
  `Supplier<List<ModuleStatus>>` (`ModuleRegistry::currentStatuses` in
  production), and publishes it via the injected `KafkaPublisher` test seam
  (production wraps `NxKafka.instance().send(topic, key, value)`). The `uptime`
  clock is captured fresh on every successful `(re)connect`, so platform-side
  dashboards show session-uptime rather than adapter-uptime. The tick runnable
  is wrapped in `SafeRunnable`; errors are logged, never propagated.
- **JVM shutdown hook** [done] (R9) — registered inline in `NxAdapter.start()` via
  `Runtime.getRuntime().addShutdownHook(new Thread(SafeRunnable.wrap(INSTANCE::shutdown,
  log), "nx-adapter-shutdown"))`. Skipped on `FAILED` (config error) and on `DISABLED`
  (R14 short-circuit) so a misconfigured / disabled adapter doesn't leave a hook
  attached. `shutdown()` itself is idempotent — JVM-exit hook + explicit host call won't
  double-fire CLOSED.
- **Logging** — uses `app.l2nx.log.NxLog` from sibling `:nx-log` subproject (auto-detects
  `org.slf4j.LoggerFactory` via reflection; SLF4J on classpath → uses it; otherwise →
  formatted `System.out.println` fallback). Shadow-included into the published
  `nx-gs-adapter-core` jar so consumers don't see a separate `nx-log` Maven dep. Library
  code never imports SLF4J directly.

## Data flows

1. **Bootstrap** — game core calls `NxAdapter.start()` → `StartupBanner.emit()` →
   `ConfigResolver.resolve()` builds `AdapterConfig`.
    - If `ConfigResolver` throws `IllegalStateException` (missing key, invalid format,
      unreadable file, malformed boolean, multi-match classpath) → caught at the
      `start()` boundary → `NxLog.error(...)` with the actionable message → state
      `INIT → FAILED` (terminal; no daemon threads launched, no shutdown hook
      registered) → return inert `NxAdapter`. **No exception propagates to the host JVM.**
    - If `config.enabled == false` → `INIT → DISABLED` (no further work, banner already
      emitted).
    - Otherwise → `ConnectFlow` schedules POST on the daemon executor → state
      `INIT → REGISTERING`.
2. **Connect success** — `ConnectClient` parses `ConnectResponse` → `ConnectFlow` invokes
   the `onActiveResponse` callback before any `Outcome` is emitted →
   `NxAdapter.initKafka(...)` composes
   `clientId = nx-gs-adapter-<tenantSlug>-<serverSlug>`, calls
   `KafkaInitializer.init(...)` with `NxAdapter::handleKafkaStateChange` as the listener
   → final adapter state derived from the returned `KafkaState`
   (`CONNECTED → ACTIVE` + latches `wasActive`; `DISCONNECTED → DEGRADED`).
   `HeartbeatService.start(serverId, heartbeatTopic)` is invoked here too, capturing
   `connectInstant` fresh so `uptime` is session-scoped.
3. **Connect retry** — non-terminal failure (409/5xx/network) → `BackoffSchedule.next(attempt)`
   → `ScheduledExecutorService.schedule(...)` re-runs the connect attempt → state stays
   `REGISTERING` on first-time bootstrap (`wasActive=false`); transitions to `DEGRADED`
   only if the adapter previously reached `ACTIVE` (`wasActive=true`).
   3a. **Kafka state changes (post-handshake)** — `NxKafka.onStateChange` listener fires
   `handleKafkaStateChange`: `CONNECTED → ACTIVE` (latches `wasActive`),
   `DISCONNECTED → DEGRADED` (only when adapter is `ACTIVE`/`DEGRADED`; ignored in
   terminal states so a late event can't resurrect a CLOSED adapter), `CLOSED` from
   Kafka is ignored (adapter shutdown drives `CLOSED` itself).
4. **Heartbeat tick** — every 60s `HeartbeatService` builds a
   `app.l2nx.gs.adapter.api.kafka.ops.HeartbeatEvent` →
   `NxKafka.instance().send(heartbeatTopic, serverId, event)` via the injected
   `KafkaPublisher`. nx-gs-kafka handles producer-side retries and reconnection;
   failed sends do not change adapter state. The tick runnable is wrapped in
   `SafeRunnable` so an uncaught throwable can't cancel the schedule.
5. **Shutdown** — JVM shutdown hook (`nx-adapter-shutdown`) OR explicit `shutdown()` →
   `closed.compareAndSet` guards idempotency → stop heartbeat → cancel schedulers →
   `NxKafka.shutdown()` (when alive) → state `CLOSED`.

## Integration points

- **`:nx-gs-adapter-api`** (R4, R16) — sibling subproject in this monorepo. Provides
  `ConnectRequest`, `ConnectResponse`, `KafkaConfig`, `Topics` (migrated from
  `nx-tenants/api/rest/dto/` as Java 8 POJOs in this slice — they were
  Lombok-`@Builder` records there) under `app.l2nx.gs.adapter.api.rest`.
  `nx-tenants` consumes the same published artifact (`app.l2nx:nx-gs-adapter-api`)
  via Gradle composite include of the whole `nx-gs-adapter` repo with a
  `dependencySubstitution` mapping — single source of truth for the wire shape.
  Validation annotations (`jakarta.validation`) stay on the controller side; the api
  lib has zero runtime deps. First published version is **0.1.0**.
  `app.l2nx.gs.adapter.api.kafka.ops.HeartbeatEvent` ships in 0.1.0 too — it's the
  wire payload published every 60s; `enabledModules` (and tenant/server identity
  fields) were added by the `adapter-modules` slice in api `0.5.0`. **api/0.6.0
  in source (tag pending)** adds `ConnectResponse.syncTopics: Map<String, String>`
  (R16) carrying per-entity Kafka topic names from the platform — consumed by
  `cdc-engine` via `TopicResolver` and by `db-sync` for DISABLED/DEGRADED triage.
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

- **`enabled` defaults to `false` — explicit operator opt-in.** Adding the JAR to a host
  classpath alone must NEVER produce network calls or daemon threads on a JVM whose
  operator hasn't deliberately turned the adapter on. Setting `l2nx.enabled=true` (sysprop /
  env / classpath) flips the switch. The disabled path still emits the banner — operators
  see the JAR loaded and version, just no work happens.
- **`platformUrl` has no fallback — must be operator-supplied with the tenant-slug
  embedded.** Format: `https://<tenant-slug>.api.l2nx.app`. A wrong URL silently routing
  to the wrong tenant is a worse failure than a missing-config crash on startup. The
  per-tenant subdomain pins requests to the correct tenant on the platform side; a single
  generic host would require an extra header / path segment to disambiguate, which would
  duplicate state already encoded in the subdomain.
- **Two-source config chain, file-first (file → sysprop).** File is the preferred medium
  for operators (drop a `l2nx.properties` next to the game-server JAR or point
  `-Dl2nx.config-file` at an arbitrary path). The file is authoritative — its values win
  over `System.getProperty(...)`. Sysprop is consulted only as a fallback when the file
  does not provide the key, so a CI pipeline that runs without a config file can still
  pass values via `-D` flags. Environment-variable resolution is intentionally absent in
  0.1.0 — file is cleaner for the L2J / Lucera / Essence host scenario (operators already
  maintain per-server config files), and env can be added later as a non-breaking third
  source if a deployment scenario demands it. `.properties` over YAML because SnakeYAML is
  a heavy dep for one config file (zero-deps principle).
- **Explicit `-Dl2nx.config-file=<path>` over recursive classpath search.** Operators who
  want the config file outside the classpath root point `-Dl2nx.config-file` at the
  absolute path. Recursive classpath scanning would mean walking every JAR in the host
  JVM's classpath at startup — high IO cost, ambiguity when duplicates exist, and the
  adapter probing arbitrary host JARs feels invasive for a JVM the operator audited
  before deploying. A single explicit override keeps resolution predictable.
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
- **`NxAdapter.start()` never throws into the host JVM either.** Config-resolution errors
  bubble out of `ConfigResolver` as `IllegalStateException` (its private contract), but
  are caught once at the `start()` boundary, logged via `NxLog`, and surface as state
  `FAILED`. The adapter is an open-core add-on — the host game server must keep running
  even if the adapter cannot bootstrap. Operators inspect via logs (primary) or
  `state()` / `onStateChange` (programmatic). This extends R10's "no exceptions to host"
  philosophy from daemon threads to the calling thread, so consumers truly never need
  a `try/catch` around `NxAdapter.start()`.
- **Backoff cap = 5 minutes.** After 5m the adapter keeps retrying at the cap (no
  give-up); operator intervention (revoke / fix tenant) is signalled via terminal
  `FAILED` / `REJECTED` states instead.
- **Heartbeat key = `serverId`.** Matches `server-registration` spec R9 (per-server message
  ordering on the consumer side).
- **No reflection-heavy DI / Spring.** Wiring inside `NxAdapter.start()` is plain `new`.
  Constructor injection only — keeps bytecode predictable across host classloaders.
- **`CLOSED` state shipped in the FSM.** Mirrors `nx-gs-kafka`'s `KafkaState.CLOSED` and gives
  operators a deterministic terminal state to query / observe via `onStateChange`. Adds one
  enum constant — no behavior cost.
- **`uptimeMs` = milliseconds since `/connect`.** Session-scoped, resets on reconnect.
  Reflects "this adapter session has been online for X" rather than "this JVM has been
  booted for X" — matches the 1:1 heartbeat-lock semantics in `server-registration`
  (a new connect = a new session = a new lock). Unit changed from seconds (api/0.5.0)
  to milliseconds (api/0.6.0) so every duration / instant on the wire shares one unit.
- **DTO migration `nx-tenants` → `nx-gs-adapter-api`.** `ConnectRequest` / `ConnectResponse`
  / `KafkaConfig` / `Topics` were defined as Lombok-`@Builder` Java records in
  `nx-tenants/api/rest/dto/`; this slice converts them to Java 8 POJOs (hand-written
  builders, all-args public constructor for Jackson parameter-name binding) and lands them
  in `nx-gs-adapter-api/src/main/java/app/l2nx/gs/adapter/api/rest/`. nx-tenants becomes a
  consumer via composite include. Wire shape is unchanged. Trade-off: Spring side loses
  record syntactic sugar; gain is single source of truth for the contract and the adapter
  side gets the same types it sees on the wire — no parallel definitions to drift.
- **Package split `api.rest` vs `api.kafka`.** REST request/response DTOs live in
  `app.l2nx.gs.adapter.api.rest` (`ConnectRequest`, `ConnectResponse`, `KafkaConfig`,
  `Topics`). Kafka message payloads live in `app.l2nx.gs.adapter.api.kafka.ops`
  (`HeartbeatEvent` in 0.1.0). The split keeps wire-protocol concerns visually separate
  and mirrors how nx-tenants is structured internally (`api/rest/` vs `infra/kafka/`).

## Extension points

- **New config sources** — extend `ConfigResolver` with an additional resolver function. The
  chain itself is a `List<Function<String, Optional<String>>>`; new sources slot in by
  appending to the list.
- **Custom HTTP transport** — `ConnectClient` is an interface; later slices can swap in
  OkHttp or another transport if a non-trivial HTTP feature (proxy, h2, mTLS) is needed.
  MVP ships only the `HttpURLConnection`-backed implementation.
- **Heartbeat enrichment** — `HeartbeatEvent` does not yet carry `enabledModules`. The
  next slice (`adapter-modules`) wires `ServiceLoader` discovery and adds that field
  with discovered module names.
- **Pluggable backoff strategies** — `BackoffSchedule` is an interface; later slices could
  add jittered exponential or fibonacci variants without touching `ConnectFlow`.
