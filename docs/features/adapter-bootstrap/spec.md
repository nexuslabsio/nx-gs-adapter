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
      `ConnectContext` (see [`adapter-modules` R2](../adapter-modules/spec.md)) and
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
      (see [`messaging`](../messaging/spec.md))
    - `nx-commands-consumer` — single Kafka consumer + dispatch
      (see [`commands`](../commands/spec.md))
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

- Server-side `/connect`:
  [
  `nx-tenants/docs/features/server-registration/spec.md`](../../../../nx-tenants/docs/features/server-registration/spec.md)
- Server-side Kafka creds delivery:
  [
  `nx-tenants/docs/features/tenant-registration/spec.md`](../../../../nx-tenants/docs/features/tenant-registration/spec.md)
- Sibling feature consumers of `ConnectResponse.syncTopics`:
  [`docs/features/cdc-engine/spec.md`](../cdc-engine/spec.md) R17 (engine reads
  topics via `TopicResolver`),
  [`docs/features/db-sync/spec.md`](../db-sync/spec.md) (`DbSyncModule.onConnect`
  reads `ctx.syncTopics().db()` after R17),
  [`docs/features/runtime-sync/spec.md`](../runtime-sync/spec.md)
  (`RuntimeSyncModule.onConnect` reads `ctx.syncTopics().runtime()` after R17)
