# Adapter Bootstrap MVP — implementation plan

> Covers: R1, R2, R3, R4, R5, R6, R7, R8, R9, R10, R11, R14, R15

## Approach

Реализация целиком ложится в `:nx-gs-adapter-core` (package
`app.l2nx.gs.adapter.core`), wire-DTO берутся из `:nx-gs-adapter-api`
(`app.l2nx.gs.adapter.api.rest.{ConnectRequest, ConnectResponse,
KafkaConfig, Topics}`), Kafka producer — через `:nx-gs-kafka`
(`NxKafka.configure().build()` + `NxKafka.send`). Точка входа —
`NxAdapter.start()`: всегда эмитит ASCII-баннер `L2NX` + версию
(R15), затем собирает `AdapterConfig` через `ConfigResolver`
(file-first: properties-file → sysprop; путь файла из
`-Dl2nx.config-file` или classpath `l2nx.properties` fallback;
env-vars в 0.1.0 нет, файл авторитативен). Если
`config.enabled == false` (R14, default `false`) — лог "adapter
disabled", state `INIT → DISABLED`, return; никаких daemon-потоков,
никаких сетевых вызовов, никакого shutdown hook'а. Иначе —
регистрируется shutdown hook и отправляется POST на
`{platformUrl}/api/tenants/servers/connect` (`platformUrl` —
оператор-supplied full URL с tenant-slug subdomain
`https://<tenant-slug>.api.l2nx.app`, без fallback) через
`ConnectClient` (JDK `HttpURLConnection` + Gson), и по статус-коду
переключается `AdapterState` через `ConnectFlow` (200 → ACTIVE +
`KafkaInitializer` + `HeartbeatService.start`; 401 → FAILED;
403 GAME_SERVER_DEACTIVATED → REJECTED; 409
KAFKA_CREDENTIALS_MISSING / 5xx / network → DEGRADED + retry через
`BackoffSchedule` 30s→1m→2m→5m). Heartbeat — `ScheduledExecutorService`
с tick = 60s, payload — package-private POJO
`heartbeat/HeartbeatPayload` (Gson сериализует напрямую, типизация в
api отложена до момента, когда платформа начнёт читать). Shutdown
идемпотентен и регистрируется через `Runtime.addShutdownHook`. Все
daemon-thread entry points обёрнуты в `try { ... } catch (Throwable)`
с логом через `app.l2nx.log.NxLog` — наружу из адаптера ничего не
улетает. **Сам `NxAdapter.start()` тоже не throws в host** — любой
`Throwable` из `ConfigResolver.resolve()` ловится централизованно,
логируется через `NxLog.error(...)` и переводит state в `FAILED`
(внутренний контракт `ConfigResolver` остаётся "throw
`IllegalStateException`"; central catch в одном месте). State
transitions атомарны через `AtomicReference`,
`onStateChange(Consumer<AdapterState>)` диспатчит callback на потоке
перехода (host сам решает, нужен ли ему hand-off).

## Milestones

### For R1, R2, R3, R14 (config resolution)

- [x] M1. `AdapterConfig` immutable holder (`serverKey`,
  `platformUrl`, `adapterVersion`, `enabled`) + private constructor
    + getters.
- [x] M2. `ConfigResolver` skeleton: `Optional<String>
  resolveString(String key)` chain (file-first):
  properties-file value → `System.getProperty(key)`. File is
  authoritative; sysprop is only consulted when the file does
  not provide the key. File source resolved once at
  construction: if `System.getProperty("l2nx.config-file")` is
  set, read that absolute path via `Files.newInputStream(...)`;
  otherwise read classpath `l2nx.properties` via
  `ClassLoader.getResourceAsStream`. Both parsed via
  `java.util.Properties`. No env vars in 0.1.0.
- [x] M3. `ConfigResolver.resolveServerKey()` — applies the chain,
  fails with `IllegalStateException` listing both sources when
  empty (R1).
- [x] M4. Server-key format validation: prefix `nx_sk_` + total
  length 38; failure → `IllegalStateException` (R2).
- [x] M5. `ConfigResolver.resolvePlatformUrl()` — applies the chain,
  no fallback; missing → `IllegalStateException` listing both
  sources (R3).
- [x] M6. [P] Adapter version resolver:
  `getClass().getPackage().getImplementationVersion()` with fallback
  `"0.0.0-unknown"` (assumed in spec; underpins R4 body).
- [x] M7. `ConfigResolver.resolveBoolean(String key, boolean
  defaultValue)` — same source chain, parses `true`/`false`
  case-insensitively; returns `defaultValue` when no source supplies
  a value. Used by `resolveEnabled()` with default `false` (R14).
- [x] M8. Test: `resolveString_shouldPrefer{File,Sysprop}_*` —
  priority order (file wins), fallback to sysprop, blank-as-absent,
  trim semantics, plus a `loadFromClassLoader` multi-match guard
  test using a tempdir-backed `URLClassLoader`.
- [x] M9. Test: `ConfigResolver_shouldRejectInvalidServerKeyFormat`
  — wrong prefix, wrong length, blank.
- [x] M10. Test:
  `ConfigResolver_shouldFailWhenPlatformUrlMissing` — no fallback,
  expects `IllegalStateException`.
- [x] M11. Test: `ConfigResolver_shouldDefaultEnabledToFalse` —
  missing key returns `false`; sysprop / file `true` / `TRUE`
  / `True` all parse to `true`.
- [x] M11a. Test:
  `ConfigResolver_shouldLoadFromExplicitPath_whenConfigFileSyspropSet`
  — write a temp file via `@TempDir`, point
  `-Dl2nx.config-file` at it, `resolveString` picks up the file's
  values. Bad / unreadable path → throws `IllegalStateException`.

Checkpoint: `AdapterConfig.from(resolver)` returns a fully-populated
config in unit tests covering both sources (sysprop + file), the
validation branch, the missing-platformUrl branch, and the
`enabled` default.

### For R4, R5 (connect flow + state machine)

- [x] M12. `AdapterState` enum: `INIT`, `REGISTERING`, `ACTIVE`,
  `DEGRADED`, `FAILED`, `REJECTED`, `DISABLED`, `CLOSED`.
- [x] M13. `NxAdapter` facade skeleton: `static start()`, `state()`,
  placeholder `shutdown()`; state held in
  `AtomicReference<AdapterState>` initialized to `INIT`. The
  `start()` body wraps `ConfigResolver.resolve()` in a
  `try { ... } catch (Throwable t)` — on failure: `log.error(
  "Adapter failed to start due to config error: {}",
  t.getMessage(), t)`, transition `INIT → FAILED`, fire
  `onStateChange(FAILED)` if registered, return inert instance
  (no shutdown hook registered, no daemon threads launched).
  No exception propagated to caller.
- [x] M13a. Test:
  `NxAdapter_shouldEnterFailedState_whenConfigResolutionFails`
  — `start()` with missing `l2nx.gs-key`: assert no exception
  reaches the test, `state()` returns `FAILED`, registered
  `onStateChange` saw `FAILED` exactly once. Covers R10/SC5
  for the calling thread.
- [x] M14. `ConnectClient` interface + `HttpURLConnectionConnectClient`
  impl: opens connection, sets `Authorization: Bearer <serverKey>`,
  `Content-Type: application/json`, `Connection: close`, connect
  timeout 5s / read timeout 10s, writes `ConnectRequest` JSON via
  Gson, reads status + body, surfaces a typed result
  (status-code + parsed `ConnectResponse` or error envelope).
- [x] M15. `BackoffSchedule` interface + default impl: `next(int
  attempt) → Duration` over the canonical 30s → 1m → 2m → 5m capped
  schedule (R5/SC3).
- [x] M16. `ConnectFlow.run()`: composes URL
  `{platformUrl}/api/tenants/servers/connect`, drives the state
  machine on response — 200 → `ACTIVE` + downstream init (placeholder
  for R6/R7), 401 → `FAILED` (terminal), 403 +
  `code=GAME_SERVER_DEACTIVATED` → `REJECTED` (terminal), 409 +
  `code=KAFKA_CREDENTIALS_MISSING` / 5xx / IOException → `DEGRADED`
    + reschedule via `BackoffSchedule`.
- [x] M17. Wire `ConnectFlow` onto a daemon `ScheduledExecutorService`
  named `nx-adapter-connect`; `NxAdapter.start()` submits the first
  attempt and returns immediately.
- [x] M18. Test: `ConnectFlow_shouldEnter*State_when*` (5 scenarios:
  200, 401, 403, 409, 5xx) — WireMock-backed.
- [x] M19. Test: `ConnectFlow_shouldRetryWithBackoff_onTransientFailure`
  — asserts 30s → 1m → 2m → 5m schedule via mocked clock /
  `ScheduledExecutorService` capture.
- [x] M20. Test: `BackoffSchedule_shouldCapAt5m_afterAttempt4`.

Checkpoint: against a WireMock `/api/tenants/servers/connect`,
`NxAdapter.start()` transitions through INIT → REGISTERING → ACTIVE
on a stubbed 200 response, and through INIT → REGISTERING →
DEGRADED → REGISTERING on a 5xx then 200.

### For R6 (Kafka producer init + state coupling)

- [x] M20a. Refine TRANSIENT state mapping: add `wasActive`
  `AtomicBoolean` to `NxAdapter`; `Outcome.TRANSIENT` pre-first-ACTIVE
  is a no-op (state stays `REGISTERING` between retries), post-ACTIVE
  drives `DEGRADED`. Justification: `DEGRADED` semantically means
  "platform handshake completed, service degraded (Kafka or transient
  platform fault)" — not "still trying to do the very first
  handshake". Sets up R6's "ACTIVE iff Kafka CONNECTED" rule.
- [x] M21. `kafka/KafkaFactory` interface + `DefaultKafkaFactory`
  impl (test seam over `NxKafka.configure().build()` singleton; default
  shuts down any live `NxKafka` before re-init for reconnect cycles)
    + `kafka/KafkaInitializer` that composes properties from
      `KafkaConfig` and delegates to the factory: `security.protocol`,
      `sasl.mechanism`, `sasl.jaas.config` templated against
      `org.apache.kafka.common.security.scram.ScramLoginModule` with
      escaped username/password, `clientId` =
      `nx-gs-adapter-<tenant-slug>-<server-slug>` composed by the caller.
      Returns the post-build `KafkaState` (CONNECTED or DISCONNECTED).
- [x] M22. Wire `ConnectFlow` 200-path: extend the outcome callback
  surface with a `Consumer<ConnectResponse>` invoked before
  `Outcome.ACTIVE`, so `NxAdapter` can run `KafkaInitializer.init(...)`
  using the response's `KafkaConfig`. AdapterState derived from the
  returned `KafkaState` (CONNECTED → `ACTIVE` + flip `wasActive=true`;
  DISCONNECTED → `DEGRADED`). Subsequent `KafkaState` transitions —
  forwarded via the `onStateChange` listener — drive `ACTIVE ⇄ DEGRADED`
  while `wasActive=true`. Init must NOT block on broker reachability —
  `nx-gs-kafka` returns immediately on `DISCONNECTED` with a background
  reconnect loop.
- [x] M23. Tests:
    * `KafkaInitializer_shouldComposeBuilderProperties_fromConnectResponse`
      — every `KafkaConfig` field lands on the captor factory; SASL JAAS
      correctly templated; quotes/backslashes in credentials escaped.
    * `KafkaInitializer_shouldForwardStateListener_toFactory` — the
      listener passed to `init(...)` is the same instance handed to
      `factory.build(...)`.
    * `NxAdapter_shouldEnterActive_whenKafkaConnected` /
      `NxAdapter_shouldEnterDegraded_whenKafkaDisconnected` — with a stub
      factory returning each state.
    * `NxAdapter_shouldStayInRegistering_whenTransientPreActive` /
      `NxAdapter_shouldEnterDegraded_whenTransientPostActive` — wasActive
      flag behavior.

### For R7 (heartbeat)

- [x] M24. `HeartbeatEvent` POJO in `nx-gs-adapter-api`
  (`app.l2nx.gs.adapter.api.kafka`) — wire fields `serverId`,
  `adapterVersion`, `uptime` (long seconds). Hand-written builder +
  `toBuilder`, equals/hashCode/toString. No `enabledModules` —
  module discovery lands in a later slice and will extend the
  payload then.
- [x] M25. `HeartbeatService.start(serverId, heartbeatTopic, kafka)`:
  capture `connectInstant = Instant.now()`,
  `ScheduledExecutorService` (1 daemon thread, named
  `nx-adapter-heartbeat`) runs at fixed delay 60s, each tick builds a
  `HeartbeatPayload` with `uptime = ChronoUnit.SECONDS.between(
  connectInstant, Instant.now())` and calls `kafka.send(topic,
  serverId, payload)`. Errors caught + logged.
- [x] M26. `HeartbeatService.stop()` cancels the scheduler. On
  reconnect (DEGRADED → ACTIVE again) `connectInstant` is recaptured
  so `uptime` resets to session-scope.
- [x] M27. Test: `HeartbeatService_shouldPublishEvery60s` —
  fake-scheduler-driven tick capture, asserts payload fields.
- [x] M28. Test: `HeartbeatService_shouldNotPropagate_whenSendThrows`
  (covers R10/SC5 for heartbeat tick).

### For R8, R9 (lifecycle + shutdown hook)

- [x] M29. `NxAdapter.shutdown()` idempotent: if state is already
  `CLOSED` or `DISABLED`, return; otherwise stop heartbeat scheduler,
  stop connect scheduler, `kafka.shutdown()` (when present), set
  state to `CLOSED`, fire final `onStateChange(CLOSED)`.
- [x] M30. `ShutdownHook` registration in `NxAdapter.start()` via
  `Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown,
  "nx-adapter-shutdown"))` — only when config resolved successfully
  AND `enabled=true`. Skipped on `FAILED` (config error, M13) and
  on `DISABLED` (R14 short-circuit, M33).
- [x] M31. Test: `NxAdapter_shouldBeIdempotent_onShutdown` — call
  twice, only one CLOSED transition emitted.
- [x] M32. Test: `NxAdapter_shouldEmitClosedState_onShutdown`.

### For R14 (disabled short-circuit)

- [x] M33. `NxAdapter.start()` short-circuit: after banner emit and
  config resolve, branch on `config.enabled` — if `false`, log INFO
  "L2NX adapter is disabled (l2nx.enabled=false) — set
  l2nx.enabled=true to activate", transition `INIT → DISABLED`
  (which fires `onStateChange(DISABLED)`), and return immediately.
  Skip shutdown-hook registration, skip connect scheduling, skip
  Kafka init, skip heartbeat.
- [x] M34. Test: `NxAdapter_shouldShortCircuit_whenEnabledFalse` —
  `state()` ends as `DISABLED`, no HTTP call observed (WireMock not
  hit), no daemon threads named `nx-adapter-*` running after
  `start()` returns.
- [x] M35. Test:
  `NxAdapter_shouldFireDisabledCallback_whenEnabledFalse` —
  `onStateChange` registered before `start()` receives a single
  `DISABLED` notification.

### For R10 (no-throw — daemon threads + start() calling thread)

- [x] M36. Wrap each daemon entry point (connect retry runnable,
  heartbeat tick runnable, Kafka producer callback) with a
  `try { runnable.run(); } catch (Throwable t) { log.error(...); }`
  helper (`SafeRunnable.wrap`). The `start()` calling-thread catch
  is already wired in M13 — no separate wrapper needed there.
- [x] M37. [P] Test: `SafeRunnable_shouldNotPropagate_whenWrappedThrows`.
- [x] M38. [P] Test: `ConnectFlow_runnable_shouldNotPropagate_whenAttemptThrows`.
- [x] M39. [P] Test: `HeartbeatService_runnable_shouldNotPropagate`
  (already covered in M28 — verify and don't duplicate). The
  calling-thread `start()` no-throw test is M13a.

### For R11 (onStateChange callback)

- [x] M40. `NxAdapter.onStateChange(Consumer<AdapterState>)`
  registration: `volatile Consumer<AdapterState>` slot, must be
  callable BEFORE `start()`.
- [x] M41. State-transition helper that performs the
  `AtomicReference.compareAndSet` and, on success, dispatches the
  callback on the calling thread (fire-and-forget; host owns
  thread-handoff).
- [x] M42. Test: `NxAdapter_shouldNotifyOnStateChange_whenTransitioning`
  — register callback, drive a stubbed connect flow through INIT →
  REGISTERING → ACTIVE → CLOSED, capture all transitions in order.

### For R15 (startup banner)

- [x] M43. `lifecycle/StartupBanner.emit(NxLog log, String version)`
  — multi-line `L2NX` ASCII wordmark with the version string
  rendered to one side, blank lines top + bottom for visual
  separation. Plain text only, no ANSI. Hardcoded ASCII string in
  the class.
- [x] M44. Wire `StartupBanner.emit(...)` as the first statement
  inside `NxAdapter.start()` — runs before config resolution so
  even a disabled adapter still announces itself.
- [x] M45. Test: `StartupBanner_shouldRenderVersionInOutput` —
  capture `NxLog` output, assert wordmark and version both present.

## Notes

- `AdapterConfig.adapterVersion` source: JAR manifest's
  `Implementation-Version` (assumed in spec). Test runs / IDE → falls
  back to `"0.0.0-unknown"`. R13 (`l2nx.adapter-version` override) is
  out of scope — pin a TODO at `AdapterConfig.from(...)` for the
  later slice.
- `nx-gs-kafka` is graceful when broker is unreachable — no
  `await`/`block` on `KafkaInitializer.init` paths; the producer
  starts in `DISCONNECTED` and reconciles in the background. Adapter
  state stays `ACTIVE` regardless.
- Heartbeat key MUST be `serverId` (matches `server-registration`
  spec R9 — per-server message ordering on the consumer side).
- `BackoffSchedule` interface is part of the extension-points listed
  in tech.md — keep it interface-shaped from M15 even though only
  one impl ships in MVP.
- Don't import `org.slf4j.*` from core code — use
  `app.l2nx.log.NxLogFactory.getLogger(...)`. The `:nx-log` facade
  is shadow-included into the published jar.
- WireMock for `ConnectFlow` tests — declared as a test dep; verify
  it's wired in `nx-gs-adapter-core/build.gradle.kts` before M18.
- Manifest `Implementation-Version` only populates when packaged —
  `./gradlew test` won't see it. Tests should use a stubbed version
  to avoid flakiness.
- `nx-tenants` consumes `app.l2nx:nx-gs-adapter-api` via composite
  include; keep `ConnectRequest` / `ConnectResponse` field shapes
  identical to what the controller expects (no rename / reorder
  during this slice — that's a separate co-ordinated change).
