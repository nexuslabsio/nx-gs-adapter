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

- [done] R1. Adapter MUST resolve `serverKey` on startup from one of two sources, file-first
  (file value wins; JVM system property is consulted only when the file does not provide the
  key):
  1. Properties file with key `l2nx.gs-key=<value>` — either at the absolute filesystem
     path given by JVM system property `-Dl2nx.config-file=<path>` (operator-preferred),
     or, if that property is unset, the classpath resource `l2nx.properties`.
  2. JVM system property `-Dl2nx.gs-key=<value>` — fallback for when the file does not
     provide the key.

  If neither yields a value — adapter logs an operator-actionable ERROR (listing both
  options) via `NxLog` and transitions to terminal `FAILED` state. **No exception
  propagates to the host JVM — see R10.** Environment-variable resolution is intentionally
  NOT included in 0.1.0 — file is the preferred config medium; env support can be added
  later as a non-breaking extension.

  - SC1. Resolution MUST be zero-dep — only JDK `System.getProperty`,
    `ClassLoader.getResourceAsStream`, `java.nio.file` (for the explicit-path case), and
    `java.util.Properties`. No SnakeYAML, no Spring, no third-party config library.

- [done] R2. Adapter MUST validate the resolved server-key format before any platform call: prefix
  `nx_sk_` + total length 38 chars (matches `Base62GameServerKeyGenerator` in nx-tenants).
  Invalid format → adapter logs an actionable ERROR and transitions to `FAILED` (per R10).
- [done] R3. Adapter MUST resolve `platformUrl` from the same two-source chain (key
  `l2nx.platform-url`). `platformUrl` is the bare host with the tenant-slug subdomain already
  embedded (e.g. `https://acme.api.l2nx.app`), NOT a full base URL with context-path. There is
  no fallback — if missing → adapter logs an actionable ERROR (listing both options) and
  transitions to `FAILED` (per R10).
- [done] R4. Adapter MUST POST to `{platformUrl}/api/tenants/servers/connect` with header
  `Authorization: Bearer <serverKey>` and JSON body `{"adapterVersion": "<version>"}`, using JDK
  `HttpURLConnection`. JSON serialization via Gson. The `/api/tenants` servlet context-path of
  nx-tenants is owned by the adapter (hardcoded into the request path), not by `platformUrl`.
- [done] R5. Adapter MUST handle `/connect` HTTP responses per the platform contract:
  - 200 → parse `ConnectResponse`, proceed to Kafka init (R6); final state is **derived
    from the post-init Kafka state** — `ACTIVE` iff `KafkaState.CONNECTED`, else `DEGRADED`
    (Kafka background reconnect drives `ACTIVE ⇄ DEGRADED` thereafter).
  - 401 → state `FAILED`, no retry (key invalid / unknown)
  - 403 (code `GAME_SERVER_DEACTIVATED`) → state `REJECTED`, no retry
  - 409 (code `KAFKA_CREDENTIALS_MISSING`) / 5xx / network / timeout → retry with backoff;
    state stays `REGISTERING` between retries until the **first** successful 200.
    After the first ACTIVE, a follow-up TRANSIENT (e.g. unforeseen re-registration path)
    drives `DEGRADED`. Pre-first-ACTIVE the adapter is still inside its handshake loop and
    a transient failure is not yet a "degraded" condition — it's a "not yet up" condition.
  - SC3. Backoff schedule for retryable failures: 30s → 1m → 2m → 5m capped.
    Each scheduled delay is jittered by ±25% so a fleet-wide platform recovery
    does not produce a thundering herd of simultaneous reconnects. The retry
    attempt counter is capped (no unbounded growth in the `AtomicInteger`).
    A `RejectedExecutionException` from the connect scheduler (e.g. scheduler
    shutdown racing with reschedule) emits `Outcome.FAILED` rather than
    being silently logged.
- [done] R6. Adapter MUST initialize Kafka producer via `nx-gs-kafka` after a successful `/connect`,
  composing builder properties from `ConnectResponse.kafka`:
  - `bootstrap.servers` ← `kafka.bootstrap`
  - `security.protocol` ← `kafka.securityProtocol`
  - `sasl.mechanism` ← `kafka.saslMechanism`
  - `sasl.jaas.config` ← `org.apache.kafka.common.security.scram.ScramLoginModule required
username="<saslUsername>" password="<saslPassword>";`
  - `client.id` ← `nx-gs-adapter-<tenantSlug>-<serverSlug>` — `tenantSlug` is sourced from
    `ConnectResponse.tenantSlug` (authoritative; not parsed from `platformUrl`).
  - **Slug value-shape contract.** Both `tenantSlug` and `serverSlug` MUST be kebab-case
    identifiers (`^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`) with NO `.` characters — the adapter
    composes resource names (Kafka `client.id`, commands `group.id` = `<tenant>.gs.commands.<server>`,
    topic prefix `<tenant>.`) by literal interpolation, and a `.` in either slug would
    escape the per-tenant ACL prefix and silently break authorization. Validation lives
    platform-side: `nx-tenants` enforces kebab-case on `POST /servers` / `POST /internal/.../servers`,
    and `nx-infra/.../create-tenant.sh` rejects non-conforming tenant slugs at SCRAM-creation
    time. Adapter trusts these as platform invariants and does not re-validate.
  - Kafka init MUST NOT block on broker reachability — `nx-gs-kafka` is graceful when the
    broker is unreachable, returning `KafkaState.DISCONNECTED` and reconnecting in the
    background.
  - Adapter state coupling: `ACTIVE` requires platform handshake passed AND
    `KafkaState.CONNECTED`. Kafka transitions (CONNECTED ↔ DISCONNECTED) drive `ACTIVE ↔
DEGRADED` post-handshake. Reconnect cycles (re-fetched creds) MUST shut down the
    existing `NxKafka` singleton before re-init.
- [done] R7. Adapter MUST publish a heartbeat message to `kafka.topics.heartbeat` every 60
  seconds (Kafka message key = `serverId`). Payload fields: `serverId`, `adapterVersion`,
  `uptime` (seconds since the most recent successful `/connect` — session uptime, resets
  on reconnect). The wire type is `app.l2nx.gs.adapter.api.kafka.ops.HeartbeatEvent` in
  `nx-gs-adapter-api` so the platform-side consumer (server-registration R9–R11) can
  compile against the same type when it lands. Module discovery (`enabledModules`) is
  out of scope for this slice — `adapter-modules` will extend the payload when it
  ships.
  - SC4. Heartbeat interval = 60s (matches `server-registration` spec R9 / SC4).
- [done] R8. Adapter MUST expose a public API `NxAdapter`:
  - `static NxAdapter start()` — fire-and-forget bootstrap (returns immediately, all work
    runs on daemon threads)
  - `AdapterState state()` — current FSM state; returns `CLOSED` after `shutdown()` completes
  - `void shutdown()` — graceful stop: cancel heartbeat scheduler, close Kafka producer,
    transition to `CLOSED`, emit a final `onStateChange(CLOSED)` callback. Idempotent.
- [done] R9. Adapter MUST register a JVM shutdown hook on `start()` that invokes `shutdown()`
  idempotently for graceful resource cleanup.
- [done] R10. Adapter MUST never propagate exceptions to host-JVM threads — including the
  calling thread of `NxAdapter.start()`. Every entry point (the caller's `start()` thread,
  connect retry loop, heartbeat scheduler tick, Kafka producer callback) MUST catch
  `Throwable`, log via the adapter's logging facade, transition to an appropriate state
  (`FAILED` / `DEGRADED` / etc.), and never re-throw.
  - SC5. Each daemon-thread entry point has a unit test that asserts the runnable does NOT
    throw when the wrapped logic throws. `NxAdapter.start()` has a unit test that asserts a
    config-resolution failure transitions to `FAILED` without throwing.
- [done] R16. **`ConnectResponse.syncTopics` — per-entity Kafka topics delivered by
  the platform.** The `/connect` response MUST carry a `Map<String, String>
syncTopics` field where the key is an entity name (`"clan"`, `"character"`,
  `"item"`, …) and the value is the fully-qualified Kafka topic the adapter is
  authorized to publish that entity's `SyncEvent`s into (e.g.
  `"bohpts.gs.sync.clans"`). Adapter behavior:
  - On 200 from `/connect`, the parsed `syncTopics` map is stored on
    `ConnectContext` (see [`adapter-modules` R2](002-adapter-modules/spec.md)) and
    surfaced to modules via `ctx.syncTopics()` at `onConnect`.
  - The map is treated as immutable for the lifetime of the connect session;
    reconnect re-fetches.
  - `null` or empty map is a valid wire value — modules that consume it (notably
    `db-sync` per its R-resolution-of-topics behavior) decide their own response
    (db-sync transitions to `DISABLED` with a defensive WARN).
  - Adapter does NOT validate topic names, does NOT pre-flight existence on the
    Kafka cluster, does NOT create topics. It treats values as opaque strings
    passed through to the producer.
  - DB credentials remain absent from the wire (see `jdbc-connection-source`
    Non-goals).
  - Wire shape lives on `app.l2nx.gs.adapter.api.rest.ConnectResponse` alongside
    existing fields (`tenantSlug`, `serverSlug`, `kafka`, …).

  Platform-side (`nx-tenants`) builds `syncTopics` from per-server entity-sync
  configuration; coordinated atomic upgrade with the adapter — no production
  consumers yet so wire-shape extension is safe.

- [done] R18. **Threading model — adapter-owned daemon threads.** Every long-running
  unit of work runs on an adapter-owned daemon thread with an
  `uncaughtExceptionHandler` installed:
  - `nx-adapter-connect` — POST `/connect` retry loop (single-threaded
    `ScheduledExecutorService`)
  - `nx-adapter-heartbeat` — 60s heartbeat scheduler (single-threaded
    `ScheduledExecutorService`)
  - `nx-adapter-shutdown` — JVM shutdown hook delegating to `shutdown()`
  - `nx-events-publisher` — bounded-queue drainer for outbound events
    (see [`messaging`](008-messaging.md))
  - `nx-commands-consumer` — single Kafka consumer + dispatch
    (see [`commands`](009-commands/spec.md))
  - `nx-io-N` — adapter-owned IO pool surfaced via `ConnectContext.io()` /
    `CommandContext.io()` for blocking JDBC / HTTP from module code and
    command handlers
  - per-entity engine threads (e.g. `nx-cdc-<entity>`) — owned by sync modules

  `Throwable` (not just `Exception`) is caught at every poll-loop / tick-loop
  / scheduler-task boundary. `Thread.setUncaughtExceptionHandler` is installed
  everywhere so an escaped throwable still routes through `NxLog` instead of
  the JVM's default handler. Heartbeat start/stop are `synchronized` so a
  reconnect cycle concurrent with shutdown cannot leave the heartbeat scheduler
  running.

- [done] R19. **`l2nx.io.workers` config key.** Resolves the size of the
  adapter-owned IO pool (`nx-io-N` daemon threads) surfaced via
  `ConnectContext.io()` / `CommandContext.io()`. Default `max(2, cores/2)`.
  Same file-first source chain as the other `l2nx.*` keys.

- [done] R20. **JAAS password escaping.** Adapter-core escapes `\` and `"` in
  SCRAM `saslUsername` / `saslPassword` before interpolating them into the
  `sasl.jaas.config` value. Closes a Kafka-init-failure path where credentials
  containing escape-significant characters produced a malformed JAAS string.

- [done] R21. **HTTP response body cap unit rename.**
  `HttpURLConnectionConnectClient.MAX_RESPONSE_BODY_BYTES` renamed to
  `MAX_RESPONSE_BODY_CHARS` — the cap is char-counted (UTF-16 code units in
  the buffer), the previous name implied bytes. Pure rename, no behavior change.

- [done] R14. Adapter MUST resolve an `enabled` flag (boolean) from the same two-source
  chain (key `l2nx.enabled`), defaulting to `false`. When `enabled=false`:
  - `start()` logs an INFO message that the adapter is disabled and returns immediately
    with state `DISABLED`.
  - No `/connect` call is made; no Kafka producer is initialized; no heartbeat is started;
    no JVM shutdown hook is registered.
  - `state()` returns `DISABLED`; `shutdown()` is a no-op.
  - The pre-registered `onStateChange` callback (if any) MUST receive a single `DISABLED`
    notification so the host can observe the no-op outcome.

  Default-false enforces explicit operator opt-in — adding the JAR to a classpath alone does
  NOT produce side effects on the host JVM.

- [done] R17. **`ConnectResponse` topics reorg — namespaced `syncTopics` + root-level
  `heartbeatTopic`.** Supersedes R16 (flat `Map<String,String> syncTopics` shape) and the
  `kafka.topics.heartbeat` reference in R7. The `/connect` response MUST carry:
  - `String heartbeatTopic` at the **root** of `ConnectResponse` — required, immutable
    for the connect session, sourced by the adapter for heartbeat publishes (replacing
    the `kafka.topics.heartbeat` nested path used in R7).
  - `SyncTopics syncTopics` at the root — namespaced by sync-source module:
    ```
    class SyncTopics {
        Map<String, String> db;       // db-sync entity → topic
        Map<String, String> runtime;  // runtime-sync entity → topic
        Map<String, String> dp;       // dp-sync entity → topic (future)
    }
    ```
    Entity names within a namespace are local to that namespace — `"character"` may
    appear in BOTH `db` and `runtime` and resolve to different topics (e.g.
    `bohpts.gs.sync.db.characters` vs `bohpts.gs.sync.runtime.characters`).

  Adapter behavior:
  - On 200, `heartbeatTopic` becomes the destination for all heartbeat publishes
    (R7's reference path is updated by `/specl-sync` once 0.11.0 ships).
  - `syncTopics` is parsed as the new `SyncTopics` POJO and stored on
    `ConnectContext`. Modules read their namespace via accessor: `db-sync` calls
    `ctx.syncTopics().db()`, `runtime-sync` calls `ctx.syncTopics().runtime()`. Both
    return `Map<String, String>` keyed by entity name in their own namespace.
  - `null` or empty `db` / `runtime` / `dp` map is a valid wire value — the
    consuming module decides its own response (`db-sync` / `runtime-sync` transition
    to `DISABLED` if their respective namespace is empty).
  - The full `SyncTopics` object is treated as immutable for the connect session;
    reconnect re-fetches.
  - Adapter does NOT validate topic names across namespaces, does NOT pre-flight
    existence on the Kafka cluster, does NOT create topics. Per-namespace values
    remain opaque strings.

  Wire shape lives on `app.l2nx.gs.adapter.api.rest.ConnectResponse` (root fields
  `heartbeatTopic` + `syncTopics`) plus `app.l2nx.gs.adapter.api.rest.SyncTopics`. This
  is a **breaking change** for `nx-gs-adapter-api` (0.10.0 → 0.11.0). Coordinated
  upgrade between adapter and `nx-tenants` — no production consumers yet on the new
  shape, so atomic flip is safe.

  Rationale for the reorg:
  - `heartbeatTopic` is a required, immutable, single-purpose field — burying it under
    `kafka.topics.heartbeat` mixed it with negotiable per-session state (Kafka SASL
    creds in `kafka`, dynamic topic delivery in `topics`). Promoting it to root
    reflects its real semantics.
  - `runtime-sync` (and future `dp-sync`) introduces entity names that collide with
    `db-sync` (`"character"` lives in both DB and in-memory data sources). A flat
    `Map<entity,topic>` cannot disambiguate. Namespaced map separates concerns
    cleanly without leaking source-type into entity names (operators think in
    domain entities, not in source types).

  - SC6. After 0.11.0, no production code reads `kafka.topics.heartbeat` —
    `/specl-sync` removes the field from `ConnectResponse.kafka.topics` once the
    adapter migration lands.

**Should:**

- [done] R11. Adapter SHOULD expose an `onStateChange(Consumer<AdapterState>)` callback that the
  host (game core) can register before `start()` to surface lifecycle transitions in operator
  UI / logs. Callback dispatch is fire-and-forget on the state-change thread; host is
  responsible for handing off to its own thread if needed.
- [done] R15. Adapter SHOULD emit a multi-line startup banner on `start()` containing the L2NX
  wordmark in ASCII art and the resolved `adapterVersion`, visually separated (blank-line
  padded) from surrounding host-JVM log output. Renders as plain text via the logging facade —
  no ANSI colours (host log sinks vary). Banner is emitted regardless of `enabled` value
  (even a disabled adapter announces itself once).

**Could:**

- [todo] R12. Adapter COULD log the resolved config source at INFO on startup (e.g.
  `server-key from config file /etc/l2nx/adapter.properties` /
  `from classpath l2nx.properties` / `from system property l2nx.gs-key`), always redacting
  the key value (e.g. `nx_sk_xxxx...xxxx`).
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
- DB and datapack sync modules (`nx-gs-db-*`, `nx-gs-dp-*`) — separate features.

## Open questions

- [resolved: `ConnectRequest` / `ConnectResponse` / `KafkaConfig` / `Topics` are migrated
  from `nx-tenants/api/rest/dto/` (Lombok-`@Builder` records) to
  `nx-gs-adapter-api/src/main/java/app/l2nx/gs/adapter/api/rest/` as Java 8 POJOs in this slice
  (REST package — the wire shape of `POST /servers/connect`). nx-tenants becomes a consumer
  of `app.l2nx:nx-gs-adapter-api` via Gradle composite include. Validation annotations
  (`jakarta.validation`) stay out of the api lib — manual validation lives in the nx-tenants
  `AdapterController`. The first published version of the api artifact is **0.1.0**.]
- [resolved: `HeartbeatEvent` ships in 0.1.0 of `nx-gs-adapter-api`
  (`app.l2nx.gs.adapter.api.kafka.ops.HeartbeatEvent`) — fields `serverId`,
  `adapterVersion`, `uptime`. `enabledModules` is intentionally absent and will
  be added when `adapter-modules` lands and ServiceLoader discovery is wired.
  Earlier note about deferring the type to a later minor was reversed once we
  decided the platform-side consumer (`server-registration` R9–R11) would consume
  it directly.]
- [resolved: `state()` and `onStateChange` emit `CLOSED` after `shutdown()` — mirrors
  `nx-gs-kafka`'s `KafkaState.CLOSED`. Predictable lifecycle for operators reading the
  state machine.]
- [resolved: heartbeat-payload `uptime` is seconds since the most recent successful
  `/connect` (session uptime, resets on reconnect). Platform-side dashboards interpret this
  as "current adapter session lifetime"; on reconnect the counter starts fresh, matching
  the 1:1 heartbeat-lock semantics in `server-registration`.]
- [resolved: `platformUrl` is operator-supplied via the three-source chain — full URL
  including the tenant-slug subdomain (e.g. `https://acme.api.l2nx.app`). No hardcoded
  fallback. The adapter appends the nx-tenants servlet context-path `/api/tenants` itself
  when constructing the `/connect` URL. Per-tenant subdomain pins the request to the right
  tenant routing on the platform side.]
- [resolved: `enabled` flag default is `false` — adapter requires explicit operator opt-in
  to do any work. Rationale: drop-in JAR + classpath presence alone must not produce network
  calls or daemon threads on a host JVM that hasn't been deliberately enabled by the
  operator. Setting `l2nx.enabled=true` (in the config file or via `-Dl2nx.enabled=true`)
  flips the switch.]
- [resolved: Config sources in 0.1.0 are JVM system properties + a single properties file
  (path from `-Dl2nx.config-file` or classpath `l2nx.properties` fallback). Environment
  variables intentionally excluded for the first release — file is the preferred medium,
  and env support can be added later as a non-breaking 4th source if a deployment scenario
  demands it. Recursive classpath search is also out — predictable single-source semantics
  win over flexibility.]
- [assumed: Adapter version is read from the JAR manifest's `Implementation-Version`
  attribute via `getClass().getPackage().getImplementationVersion()`, with fallback to
  `"unknown"` for IDE / test runs.]
- [resolved: `NxAdapter.start()` NEVER throws into the host JVM. Config-resolution errors
  surface as `IllegalStateException` from `ConfigResolver` internally, but are caught once
  at the `start()` boundary, logged via `NxLog` with an operator-actionable message, and
  the adapter transitions to terminal `FAILED` state. Operators inspect via logs (primary
  channel) or `state()` / `onStateChange` (programmatic). Rationale: the adapter is an
  open-core add-on for L2J / Lucera / Essence — host game server must keep running even
  if the adapter cannot bootstrap. Matches R10's "no exceptions to host" applied to the
  calling thread, not just daemon threads.]
- [NEEDS CLARIFICATION: `ConfigResolver.resolvePlatformUrl()` rejects non-`https://`
  schemes, URLs with query strings or fragments, missing host, and malformed URIs
  (security tightening from M12-M20 review — bearer would travel plaintext over `http://`,
  query/fragment can poison URL composition). Codify these as MUST under R3, or remove the
  validation from code? ref: `nx-gs-adapter-core/src/main/java/app/l2nx/gs/adapter/core/config/ConfigResolver.java`]
- [NEEDS CLARIFICATION: `HttpURLConnectionConnectClient.readBody` caps response body at
  1 MiB to defend the host JVM from OOM on a hostile / runaway server response. The cap
  is not in R4 / SC. Add as an SC under R4, or document elsewhere? ref:
  `nx-gs-adapter-core/src/main/java/app/l2nx/gs/adapter/core/connect/HttpURLConnectionConnectClient.java`]
- [NEEDS CLARIFICATION: `NxAdapter.start()` is idempotent — duplicate invocations log a
  WARN and return without re-running. Not specified in R8. Add to R8 contract or treat as
  internal hardening? ref: `nx-gs-adapter-core/src/main/java/app/l2nx/gs/adapter/core/NxAdapter.java`]
- [NEEDS CLARIFICATION: 4xx responses without the spec'd error code (e.g. `403` without
  `code=GAME_SERVER_DEACTIVATED`, `409` without `code=KAFKA_CREDENTIALS_MISSING`, `404`,
  `400`, etc.) currently route to terminal `FAILED`. R5 only specifies the codes-with-body
  scenarios. Lock the fall-through as terminal in R5, or specify transient-retry for some
  (e.g. `404` could be platform deployment misroute, transient)?]
- [NEEDS CLARIFICATION: R17 migration coordination with `nx-tenants`. The new
  `ConnectResponse` shape (root `heartbeatTopic` + namespaced `syncTopics`) is a breaking
  change. Plan: ship `nx-gs-adapter-api` 0.11.0 first, update `nx-tenants` to emit the new
  shape in lockstep (no production consumers on the new shape yet), then ship
  `nx-gs-adapter-core` bumped to consume it. Alternative: support both shapes in adapter
  for one minor (0.11.0 reads either) and flip the platform second — adds parser
  branching that's worth avoiding if coordinated atomic flip is feasible.]

## Links

- Server-side `/connect`: `nx-tenants/docs/specs/001-server-registration/spec.md`
- Server-side Kafka creds delivery: `nx-tenants/docs/specs/002-tenant-registration/spec.md`
- Sibling feature consumers of `ConnectResponse.syncTopics`:
  [`docs/specs/005-cdc-engine/spec.md`](005-cdc-engine/spec.md) R17 (engine reads
  topics via `TopicResolver`),
  [`docs/specs/003-db-sync/spec.md`](003-db-sync/spec.md) (`DbSyncModule.onConnect`
  reads `ctx.syncTopics().db()` after R17),
  [`docs/specs/006-runtime-sync.md`](006-runtime-sync.md)
  (`RuntimeSyncModule.onConnect` reads `ctx.syncTopics().runtime()` after R17)

---

## Technical design

### Overview

Точка входа в адаптер — `NxAdapter.start()`, которую вызывает bootstrap-код игрового ядра.
Запуск non-blocking: сразу возвращается, всё дальнейшее выполняется на daemon-потоках.
Внутри — резолв конфига (`AdapterConfig` через file-first chain: properties-file → sysprop;
env-vars в 0.1.0 нет), POST `/connect` через JDK `HttpURLConnection` + Gson, инициализация
Kafka producer'а через `nx-gs-kafka`, и heartbeat-петля на `ScheduledExecutorService`.
Lifecycle FSM (`AdapterState`) — единственный шарящийся state, доступный наружу через
`state()` и опциональный `onStateChange` callback.

### Structure

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
    retry-with-backoff via `AtomicInteger` attempt counter (capped — no unbounded
    growth); each scheduled delay is jittered ±25% (`BackoffSchedule.next` value
    ± `0.25 * value`) to avoid fleet-wide thundering herd on platform recovery;
    `RejectedExecutionException` from the scheduler emits `Outcome.FAILED` rather
    than being logged silently; `sanitize()` redacts `Bearer <token>` patterns
    from log messages
  - `connect/ConnectClient.java` [done] — interface; implementations encode transport /
    parse failures as `ConnectResult` rather than throwing
  - `connect/HttpURLConnectionConnectClient.java` [done] — JDK-only impl: Bearer auth,
    `Connection: close`, 5s/10s timeouts, `BufferedWriter`-wrapped output, 1 Mi hard
    cap on response body via the `MAX_RESPONSE_BODY_CHARS` constant (renamed from
    `MAX_RESPONSE_BODY_BYTES` — counted in UTF-16 code units, not bytes; pure
    rename, no behavior change), UTF-8 char-array read
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
    escapes `\` and `"` in `saslUsername` / `saslPassword` before interpolation into
    the JAAS config string — closes a Kafka-init-failure path where credentials
    containing escape-significant characters produced a malformed JAAS value. Also
    wires the at-least-once producer durability defaults (`acks=all`,
    `enable.idempotence=true`, `max.in.flight=5`, `linger.ms=10`,
    `compression=gzip`, `retries=MAX`, `delivery.timeout.ms=120000`) — all
    overridable via user properties.
  - `kafka/KafkaFactory.java` [done] — interface (test seam over the
    `NxKafka.configure().build()` singleton); contract returns post-build `KafkaState`
    and forwards a state-change listener
  - `kafka/DefaultKafkaFactory.java` [done] — default impl bridging to
    `NxKafka.configure().build()`; shuts down any live `NxKafka` singleton before
    re-init so a `DEGRADED → ACTIVE` reconnect cycle that re-fetches creds is
    idempotent
  - `heartbeat/HeartbeatService.java` [done] — `ScheduledExecutorService`-driven 60s
    loop; tick wrapped in `SafeRunnable` so a runaway throwable can't cancel the
    schedule. `start(...)` and `stop()` are `synchronized` so a reconnect cycle
    cannot leave a scheduler running concurrent with shutdown.
    `KafkaPublisher` interface is the test seam over
    `NxKafka.instance().send(topic, key, payload)`. `start(serverId, topic)`
    captures `connectInstant` fresh each call, so uptime is session-scoped
    (resets on reconnect)
  - `concurrent/SafeRunnable.java` [done] — static `wrap(Runnable, NxLog)`
    helper that swallows `Throwable` from the delegate. Applied at every
    adapter daemon-thread entry point (connect submit, heartbeat tick,
    shutdown hook). `Thread.setUncaughtExceptionHandler` is also installed
    on every adapter-owned thread so an escaped throwable still routes
    through `NxLog` instead of the JVM default handler. Poll/tick loops
    catch `Throwable` (not just `Exception`).
  - `io/IoPool.java` [done] — adapter-owned daemon executor (`nx-io-N`
    threads) sized by `l2nx.io.workers` (default `max(2, cores/2)`).
    Surfaced via `ConnectContext.io()` and `CommandContext.io()` for
    blocking IO (JDBC, HTTP, FS) from module code and command handlers —
    handlers MUST NOT do blocking IO on the consumer thread or via
    `ctx.host().sync(...)`.
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
  - logging via `app.l2nx.gs.log.NxLog` from sibling `:nx-gs-log` subproject (shadow-included
    into the published jar — see Integration points)
- `nx-gs-adapter-core/src/test/java/app/l2nx/gs/adapter/core/` — unit tests for
  `ConfigResolver`, `NxAdapter`, `ConnectFlow` (WireMock-backed, status dispatch via
  `@ParameterizedTest`), `DefaultBackoffSchedule`. `CapturingScheduler` is a hand-rolled
  `ScheduledExecutorService` test double (Mockito 5.x / Byte Buddy doesn't support Java
  25+, so we don't mock the JDK interface). Future: `HeartbeatService` (with mocked
  `NxKafka`) once heartbeat lands.

### Key components

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
  consulted when the file does not provide the value, so the file is authoritative). The
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
- **Logging** — uses `app.l2nx.gs.log.NxLog` from sibling `:nx-gs-log` subproject (auto-detects
  `org.slf4j.LoggerFactory` via reflection; SLF4J on classpath → uses it; otherwise →
  formatted `System.out.println` fallback). Shadow-included into the published
  `nx-gs-adapter-core` jar so consumers don't see a separate `nx-gs-log` Maven dep. Library
  code never imports SLF4J directly.

### Data flows

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

### Integration points

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
- **`:nx-gs-log`** — sibling subproject (internal logging facade, package `app.l2nx.gs.log`).
  Shadow-included into `:nx-gs-adapter-core` and `:nx-gs-kafka` jars at build time, NOT
  published as a separate Maven artifact. Auto-detects SLF4J via reflection, falls back
  to console output. Library code never imports SLF4J directly.
- **`nx-tenants` `POST /api/tenants/servers/connect`** (R4, R5) — wire contract; status codes
  per `server-registration` spec R7/R8 + `tenant-registration` spec R4.
- **JDK `HttpURLConnection`** (R4) — no third-party HTTP. Configured with explicit timeouts
  and `Connection: close`.
- **JDK `java.util.Properties`** (R1) — classpath `l2nx.properties` parsing.
- **Game-core JVM lifecycle** (R9) — `Runtime.getRuntime().addShutdownHook(...)`.

### Decisions

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
  exceptions in adapter threads must NEVER bring down the host JVM. Every poll/tick
  loop catches `Throwable` (not just `Exception`), and every adapter-owned thread
  has a `Thread.setUncaughtExceptionHandler` installed that routes through `NxLog`.
  Same philosophy as `nx-gs-kafka`.

- **Adapter-owned threading inventory.** Every long-running unit of work lives on
  an adapter-owned daemon thread, named so operators can identify owners from
  thread dumps:
  - `nx-adapter-connect` — connect retry loop
  - `nx-adapter-heartbeat` — 60s heartbeat scheduler
  - `nx-adapter-shutdown` — JVM shutdown hook
  - `nx-events-publisher` — events bounded-queue drainer
  - `nx-commands-consumer` — commands Kafka consumer
  - `nx-io-N` — shared IO pool (`l2nx.io.workers`)
  - `nx-cdc-<entity>` — per-entity CDC engine threads (owned by sync modules)

- **Backoff jitter.** Each retry delay is jittered by ±25% to avoid fleet-wide
  thundering-herd reconnect storms on platform recovery. The attempt counter is
  also capped — no unbounded growth.

- **`RejectedExecutionException` from the connect scheduler emits
  `Outcome.FAILED`.** Previously logged-and-ignored, which masked a real
  shutdown-race condition. Surfacing it as `FAILED` lets the orchestrator
  observe and react.

- **Heartbeat start/stop is `synchronized`.** A reconnect cycle concurrent with
  shutdown could otherwise leave the scheduler running while the rest of the
  adapter tears down.
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

### Extension points

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
