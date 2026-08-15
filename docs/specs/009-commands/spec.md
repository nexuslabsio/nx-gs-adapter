# Commands — Inbound RPC Surface

> Owner: @n1rmata

Formal infra contract for the inbound command-RPC surface. Companions:
[`guide.md`](./guide.md) — handler-author walkthrough (threading, registration, what to return);
[`catalog.md`](./catalog.md) — per-command business contract (inputs, result fields, error
statuses). Neither is duplicated here.

## Problem

The L2 game-server core needs a structured way to receive operational commands from the
platform's web side — kicks, mail sends, item operations, account punishments,
character ↔ telegram pairings — and reply with a typed success payload or a structured
problem.

Legacy bohpts used a hand-rolled RabbitMQ surface
(`l2e.gameserver.infrastructure.rabbitMq`) that:

1. Auto-acks messages BEFORE the handler runs (drops on handler failure)
2. Runs handlers on the RabbitMQ consumer thread (races against game-state mutations)
3. Routes by "who sent it" (admin queue vs telegram queue) instead of "what to do"
   (same DTO duplicated across queues)
4. Couples handlers directly to game-server internals (`GameObjectsStorage`, `MailManager`,
   `CharacterDAO`)
5. Carries no tenant / server identity in the envelope
6. Replies with a free-form `message` field (no structured status)
7. Embeds correlation id in the message body instead of headers

Sibling [`messaging`](../008-messaging.md) shipped the **outbound** events surface and left the
inbound wire-shape as a Javadoc placeholder. This feature owns the runtime: Kafka consumer +
dispatch + handler SPI + reply publishing, plus the command catalog that grew on top of it.

Audience: bohpts-core (and future per-tenant) command-handler authors; platform-side operators
who consume reply events.

## Requirements

> Sibling features carry the SPI plumbing and topic delivery contract:
>
> - Tier-1 SPI (`AdapterModule` + ServiceLoader) lives in [`adapter-modules`](../002-adapter-modules/spec.md).
>   Commands runtime is **not** a discovered Tier-1 module — it lives inside `nx-gs-adapter-core`
>   as a built-in capability surfaced through `ConnectContext`, symmetric to the
>   [`messaging`](../008-messaging.md) `events` surface.
> - `Nx-Server-Id` / `Nx-Message-Type` / `Nx-Correlation-Id` header contracts from
>   [`per-server-sync`](../007-per-server-sync.md) and [`messaging`](../008-messaging.md)
>   are reused unchanged. `Nx-Target-Server-Id` (R22) is introduced here.

**Must:**

- [done] R1. `nx-gs-adapter-api.rest.MessagingTopics` MUST carry two single-topic fields
  alongside the unchanged `events: Map<String,String>`:
  - `@Nullable String commandsTopic` — fully-qualified inbound topic
    (e.g. `<tenant>.gs.commands`). `null` / blank → commands surface disabled: no consumer
    thread is spawned, `ctx.commands().on(...)` still accepts registrations that are never
    invoked.
  - `@Nullable String commandsRepliesTopic` — fully-qualified outbound replies topic
    (e.g. `<tenant>.gs.commands.replies`). `null` / blank → handlers run but replies are
    dropped.
  - The Phase-1 `Map<String,String> commands` placeholder is gone; a platform still emitting
    `"commands": {}` is harmless under Gson's ignore-unknown-fields default.

- [done] R2. One inbound Kafka topic carries every command type; routing inside the topic is by
  `Nx-Message-Type` header, never by topic name. The adapter does not read the record key —
  partition-key choice is the platform producer's contract, and the adapter neither enforces nor
  depends on it. What the producer actually does (`nx-gameservers` `CommandsSender.publish`): a
  nullable `Long` key, `charId` for character-scoped commands so cross-domain ordering per character
  is preserved, `null` for commands with no natural character. Ordering guarantees therefore hold
  only as far as the producer's keying does.

- [done] R3. `nx-gs-adapter-api.kafka.commands.NxCommand<R>` MUST be a type-parameterized marker
  interface for inbound command DTOs. The type parameter `R` declares the command's
  success-payload type — fixed at the command class declaration, not at handler-registration
  time. This makes the wire reply contract **statically typed**: platform-web and the host-side
  handler read the same `NxCommand<R>` binding and cannot disagree about reply shape.

  Concrete DTOs live under `app.l2nx.gs.adapter.api.kafka.commands.<group>.*` (group = code-org
  bucket: `announcement`, `ban`, `character`, `gd`, `item`, `mail`, `privatestore`, `sync`,
  `telegram`). The topic stays single; the package split exists for Javadoc / discovery.

  Every command declares a dedicated `{X}Result` payload class so even a "void-success" command
  echoes confirmation data. The per-command contracts live in [`catalog.md`](./catalog.md) — this
  spec does not enumerate them.

- [done] R4. `nx-gs-adapter-api.kafka.commands.CommandResult<R>` MUST be the single reply
  envelope, Gson-friendly, with three fields:
  - `CommandStatus status` — REQUIRED (R23).
  - `@Nullable R payload` — non-null iff `status == OK`.
  - `@Nullable CommandProblem problem` — non-null iff `status != OK` (R24).

  The constructor enforces that invariant for programmatic construction; wire-path Gson bypasses
  it, so consumers assume rather than re-derive it. Static factories: `ok()`, `ok(R)`,
  `error(status, problem)`, `error(status, title)`, `error(status, title, extKey, extValue)`,
  plus per-status sugar (`notFound`, `invalidState`, `forbidden`, `validationFailed`,
  `rateLimited`, `unavailable`, `internalError`). Domain success data belongs in `R`, never in
  `problem.extensions`.

- [dropped] R5. The original `ErrorCode` enum + `boolean success` + `Map<String,String>
errorDetails` triple. Dropped before first release in favour of R23 + R24: three fields whose
  legal combinations were expressed only in prose ("`errorCode` REQUIRED iff `success=false`")
  collapse into one enum plus one problem body, and the free-form string map could not carry
  numeric or list context (required-vs-available adena, rejected item ids) without stringifying
  it. No `ErrorCode` type exists in the api.

- [done] R6. `nx-gs-adapter-api.spi.CommandHandler<C extends NxCommand<R>, R>` MUST ship as a SAM
  returning `CommandResult<R>` from `handle(C command, CommandContext ctx)`. The bound
  `C extends NxCommand<R>` forces the handler's reply payload type to match the command class's
  declared type at compile time. The handler runs synchronously on the adapter's commands
  consumer thread; game-state mutations require an explicit `ctx.host().sync(...)` hop and
  blocking IO requires an explicit `ctx.io()` hop.

- [done] R7. `nx-gs-adapter-api.spi.CommandContext` MUST expose:
  - `UUID correlationId()` — inbound `Nx-Correlation-Id`, never null (fallback UUIDv7 when the
    header is absent or malformed).
  - `HostExecutor host()` — game-thread hop (R8).
  - `NxEvents events()` — handlers MAY publish side-effect events.
  - `Executor io()` — adapter-owned IO pool for blocking JDBC / HTTP, backed by `nx-io-N`
    daemon threads sized by `l2nx.io.workers` (default `max(2, cores/2)`). Handlers MUST use it
    instead of blocking the consumer thread or burning game-thread capacity on IO.
  - `NxSync sync()` — out-of-band sync trigger (R25).

  `host()`, `events()`, `io()` and `sync()` are session-scoped; only `correlationId()` is
  per-record.

- [done] R8. `nx-gs-adapter-api.spi.HostExecutor` MUST expose `void sync(Runnable)`,
  `<T> T sync(Supplier<T>)` and `void async(Runnable)`.

  `sync` blocks the caller on a latch until the host executor finishes the task OR
  `l2nx.commands.host-sync-timeout-ms` elapses (default 30s). On timeout it throws
  `HostExecutorTimeoutException`; the dispatcher maps that to a `UNAVAILABLE` reply carrying
  `error.cause = "host-executor-timeout"` + `timeout.ms`. The bound is load-bearing — an
  unbounded await would let a saturated host pool wedge the consumer thread indefinitely.
  Exceptions thrown by the task propagate to the handler unchanged (sneaky-throw, so checked
  exceptions are not in scope); a rejected submission propagates too. Caller interrupt restores
  the flag and throws `RuntimeException` while the submitted task keeps running unobserved.
  `async` is fire-and-forget, wrapped in `SafeRunnable` so any `Throwable` is logged via `NxLog`
  rather than reaching the host thread's UEH. Every method throws `IllegalStateException` when no
  host executor is registered (R12).

- [done] R9. `nx-gs-adapter-api.spi.NxCommands` MUST expose
  `<R, C extends NxCommand<R>> void on(Class<C> type, CommandHandler<C, R> handler)`, acquired via
  `ConnectContext.commands()`. The bound makes handler/command reply types agree at registration
  time. The registration window opens at `onConnect(ctx)`; late registration after the consumer
  thread has started is permitted (`ConcurrentHashMap.put`). Re-registering the same class
  overwrites the previous handler and logs WARN. Routing key is `Class.getSimpleName()`, so simple
  names MUST be unique across the catalog. `on(...)` never blocks longer than a map put and never
  propagates a failure — `null` type or handler is ignored with a WARN.

- [done] R10. `nx-gs-adapter-api.spi.ConnectContext` MUST expose `NxCommands commands()`,
  `Executor io()` and `NxSync sync()` accessors alongside `events()`. The `commands()` facade is
  non-null even when `commandsTopic` is unconfigured or the commands bootstrap failed, so host
  code can register unconditionally.

  Facade identity is **stable across reconnect cycles**: `NxAdapter` caches one `NxCommandsImpl`
  per JVM and `CommandsBootstrap.swap(...)` rebuilds the consumer behind it, reusing the existing
  `CommandTypeRegistry`. Handlers registered once survive every later handshake re-roll without
  re-registration — closing the race where a host that cached the facade after the first
  `onConnect` would keep calling a dead consumer.

- [done] R11. `nx-gs-adapter-core` MUST implement an internal `CommandsConsumer` with
  **at-most-once** delivery:
  - One `KafkaConsumer<byte[], byte[]>` polling `commandsTopic` from a single daemon thread
    `nx-commands-consumer`, whose body is wrapped in `SafeRunnable` so no `Throwable` escapes
    into the host JVM.
  - Loop: `poll(pollTimeoutMs)` → skip empty batches → `commitSync()` **before** any record is
    dispatched → dispatch each record sequentially. A failed commit increments
    `commit-failures-total`, logs WARN and **drops the batch undispatched** rather than risking
    a redelivery loop against a broken broker. `enable.auto.commit=false` is pinned so the
    manual-commit point is the only one.
  - Consequences, which the handler contract depends on: a crash or graceful stop mid-batch
    loses the in-flight records — they never redeliver, the caller times out, and a human
    re-issues the command with a fresh correlation id. **Handlers therefore do NOT need to be
    idempotent**, and the adapter keeps no dedup cache.
  - Per-record dispatch: filter by `Nx-Target-Server-Id` (R22) → read `Nx-Message-Type` and
    `Nx-Correlation-Id` → look up the binding in `CommandTypeRegistry` → Gson-deserialize the
    value into the binding's class → build `CommandContext` → invoke the handler → publish the
    reply fire-and-forget (R13).
  - Error boundaries (all of them reply and move on; none of them re-dispatch):
    - missing / empty `Nx-Message-Type` → `UNSUPPORTED_COMMAND`,
      `error.cause = "missing-message-type-header"`, reply type header falls back to the
      literal `CommandResult`
    - header value with no registered handler → `UNSUPPORTED_COMMAND` with the `messageType`
      extension
    - Gson `JsonSyntaxException` (or a `null` object from a non-null body) →
      `VALIDATION_FAILED` with the parse message as `detail` + `error.class`
    - any other deserialization `Throwable` → `INTERNAL_ERROR`
    - `HostExecutorTimeoutException` → `UNAVAILABLE` (`error.cause = "host-executor-timeout"`,
      `timeout.ms`)
    - handler `RuntimeException` → `INTERNAL_ERROR` with `error.class` + message
    - handler returns `null` → `INTERNAL_ERROR`, `error.cause = "handler-returned-null"`
    - `Error` (OOM, StackOverflow) is deliberately NOT caught: no reply, the consumer thread
      unwinds into `SafeRunnable`, which logs it and lets the thread die. Offsets are already
      committed, so nothing replays.

  The at-least-once design this requirement originally carried (commit after the batch, plus a
  per-batch reply-flush gate that skipped the commit on interrupt) was dropped: these are
  operator-issued RPCs with a human on the other end, and silent double-execution of "delete
  item" / "send mail" is strictly worse than a visible timeout the operator retries. Deleting the
  gate also removed the idempotency burden from every host handler.

- [done] R12. `nx-gs-adapter-core` MUST expose `NxAdapter.hostExecutor(Executor)`, called by the
  host during bootstrap **before** `start()`, symmetric to `NxAdapter.onStateChange(...)`. Without
  it every `ctx.host()` call throws
  `IllegalStateException("HostExecutor not registered — call NxAdapter.hostExecutor(...) before start()")`,
  so the misconfiguration surfaces at the first hop instead of dropping work silently.

- [done] R13. The reply path MUST publish directly through the existing
  `NxKafka.sendBytesKeyRecord(record, callback)` producer, not through the events publisher's
  bounded queue — the events queue's drop policy is right for stale snapshots and wrong for
  replies (a dropped reply is an unrecoverable web-side timeout), while the producer's internal
  record accumulator absorbs back-pressure. Reply construction:
  - `key = bigEndian(correlationId.getMostSignificantBits())`, so replies for one correlation
    co-locate on a partition (tooling aid only)
  - headers: `Nx-Correlation-Id` (echoed, or the generated fallback), `Nx-Message-Type` (R26),
    and `Nx-Server-Id` stamped by the Kafka facade's static headers
  - value = the `CommandResult` object, serialized by the facade's `GsonSerializer`
  - target topic = `commandsRepliesTopic`; when it is unconfigured the reply is dropped with a
    DEBUG log and `replies-failed-total` is incremented, so `{consumed > 0,
replies-published == 0}` is visible as a failure rather than as silence
  - the send callback bumps `replies-published-total` or `replies-failed-total`; a synchronous
    throw from `send(...)` is treated as a failed reply

- [done] R14. Adapter-core MUST surface a built-in `commands` heartbeat module slot in
  `HeartbeatEvent.enabledModules`, carrying a `CommandsStats` POJO (parallel to `EventsStats`)
  with `consumed-total`, `other-server-skipped-total`, `handled-total`, `unsupported-total`,
  `validation-failed-total`, `internal-errors-total`, `replies-published-total`,
  `replies-failed-total`, `commit-failures-total` and `registered-types`.

  `state` is `DISABLED` before start, `DEGRADED` while running without a replies topic, and
  `ACTIVE` otherwise; the slot is omitted entirely when commands are disabled or the bootstrap
  failed. `DEGRADED` is deliberately structural rather than an error-ratio threshold: commands
  still execute with no replies topic, but every reply is dropped, so each caller waits out its
  timeout and may re-issue a command that already ran. A ratio over a window cannot be derived
  from the monotonic counters the slot ships — `replies-failed-total` rising tells an operator
  nothing about whether the loss is total or a one-off — so ratio alerting stays operator-side.

- [done] R15. Engine config under `l2nx.commands.*` (file-first source chain, parallel to
  `l2nx.events.*`):
  - `l2nx.commands.poll-timeout-ms` (default `100`)
  - `l2nx.commands.shutdown-timeout-ms` (default `5000`)
  - `l2nx.commands.host-sync-timeout-ms` (default `30000`) — the R8 bound
  - `l2nx.commands.kafka.<property>` — proxied to `KafkaConsumer` properties
    (e.g. `l2nx.commands.kafka.max.poll.records=50`)
    `l2nx.commands.reply-flush-timeout-ms` was removed together with the reply-drain gate it fed
    (see R11): under at-most-once it gated nothing, and `KafkaProducer.close(timeout)` on adapter
    shutdown already waits out in-flight reply sends. An operator who still has the key in
    `l2nx.properties` is harmless — unknown keys are ignored.

- [done] R16. Kafka consumer config, composed from the platform-issued `KafkaCredentials` plus
  `l2nx.commands.kafka.*` overrides, in that layering order — internal defaults first, operator
  overrides second, identity / security / commit semantics pinned last so they cannot be
  overridden:
  - defaults: `auto.offset.reset=earliest`, `max.poll.records=50`
  - pinned: `bootstrap.servers` from the connect response;
    `client.id = <clientIdBase>-commands` (broker logs only, not ACL-checked);
    `group.id = <tenant>.gs.commands.<server>`, under the per-tenant `<tenant>.` prefix so the
    `User:<tenant>` SCRAM principal's group ACL covers it, with per-server isolation from the
    `<server>` suffix; `enable.auto.commit=false`; `security.protocol` / `sasl.mechanism` /
    `sasl.jaas.config` from the connect response
  - key/value deserializers are `ByteArrayDeserializer` (passed at construction, not via props)

- [done] R17. Lifecycle integration with `NxAdapter`:
  - The host executor registered via R12 is held statically and read at each connect.
  - After Kafka init, `CommandsBootstrap.start(...)` builds registry + facade + consumer and
    returns a `Started` bundle (parallel to `EventsBootstrap.Started`); later connects go
    through `CommandsBootstrap.swap(...)`, which reuses the facade and registry (R10). The
    caller stops the previous consumer first.
  - Commands bootstrap is isolated from events bootstrap and from module discovery: a failure in
    either (e.g. `NoClassDefFoundError` from an api/core version skew on the host classpath) is
    logged and leaves that surface disabled instead of aborting sync, which is the adapter's
    primary job. `ConnectContext` null-coalesces the missing facade to a no-op.
  - The `ConnectContext` handed to `ModuleRegistry.connect(ctx)` carries `commands()`, so host
    modules register handlers from their `onConnect` callback.
  - `NxAdapter.shutdown()` stops the commands consumer **before** the IO pool and the events
    publisher, so in-flight handlers finish and their replies reach the producer before it is
    torn down. `CommandsConsumer.stop()` wakes the poll, joins the daemon within
    `shutdown-timeout-ms` (+1s grace) and closes the consumer.

- [dropped] R18. The original pinned module versions (`api = 0.14.0`, `core = 0.7.0`). Dropped as
  a standing requirement: a version pin in a living spec is stale one release later, and the
  authority is the per-module literal in `build.gradle.kts` plus the `api/vX.Y.Z` / `core/vX.Y.Z`
  tags. The evolution rule it encoded stands and lives in the root `CLAUDE.md`: additive wire
  changes are forward+backward compatible, removals and retypes take the two-release
  `@Deprecated` path. `nx-gs-kafka` is untouched by this feature — adapter-core builds its own
  `KafkaConsumer` and reuses the existing producer for replies.

**Should:**

- [done] R19. Adapter-core SHOULD log a WARN at bootstrap when `commandsTopic` is configured but no
  host executor is registered, and another when `commandsTopic` is configured without
  `commandsRepliesTopic`. Both are operator misconfigurations that otherwise only surface as
  handler exceptions or web-side timeouts.

**Could:**

- [todo] R20. Multi-thread consumer pool — one thread per assigned partition, scaling parallelism
  while preserving the producer's per-key ordering. Still single-threaded; upgrade if cadence
  becomes a bottleneck.

- [dropped] R21. Adapter-side idempotency cache (bounded LRU of handled `correlationId`s that
  re-publishes the cached reply for a duplicate). Moot under R11: at-most-once means a record is
  dispatched at most once, and an operator re-issue carries a fresh correlation id, so there is
  nothing to deduplicate.

**Must (added after the initial slice):**

- [done] R22. The commands topic is per-tenant and shared by every game-server under that tenant,
  so each record MUST carry `Nx-Target-Server-Id` — the 16-byte encoded server UUID issued in the
  `/connect` response. The adapter drops any record whose header is missing, malformed, or does
  not match its own server id, counting it in `other-server-skipped-total` and emitting **no
  reply**. The contract is strict on purpose: an unheadered record has no addressee, and replying
  would mean every adapter in the tenant answers it. Missing / malformed headers log WARN;
  wrong-target logs DEBUG (it is the normal case on a shared topic).

- [done] R23. `nx-gs-adapter-api.kafka.commands.CommandStatus` MUST be the single outcome enum,
  each constant carrying a nested `Tier` (`OK` / `CLIENT_ERROR` / `SERVER_ERROR`) for coarse
  HTTP-aligned caller routing, with `tier()` and `isOk()` accessors. Wire form is the constant
  name. Constants: `OK`; `NOT_FOUND`, `INVALID_STATE`, `FORBIDDEN`, `VALIDATION_FAILED`,
  `RATE_LIMITED`, `UNSUPPORTED_COMMAND`, `COMMAND_EXPIRED` (all `CLIENT_ERROR`); `UNAVAILABLE`,
  `INTERNAL_ERROR` (both `SERVER_ERROR`). `UNSUPPORTED_COMMAND` is adapter-emitted only.

  `COMMAND_EXPIRED` means the command carried an execution deadline that had already passed when
  the host picked it up, so it was refused without running — nothing read, charged or moved. It is
  deliberately distinct from `INVALID_STATE`: the world was never consulted, so retrying with a
  fresh deadline is meaningful whereas re-sending the same command never is. First user is
  `BuyFromPrivateStoreCommand.getDeadline()` (REQUIRED field, guards a command that sat in the
  ~3h Kafka backlog while the game-server was down). No host emits it yet — bohpts does not read
  `getDeadline()` today. Rollout ordering is below.

- [done] R24. `nx-gs-adapter-api.kafka.commands.CommandProblem` MUST carry the failure context of
  a non-OK reply as a transport-neutral RFC 9457 subset: `title` (required, stable per problem
  kind), `@Nullable detail` (per-instance), and an `extensions` map of Gson-serializable values,
  frozen unmodifiable and normalized from `null` to empty. HTTP-specific fields (status, type URI,
  instance URI) live elsewhere — `CommandStatus` and `Nx-Correlation-Id`. Object-valued extensions
  are what let a problem carry numeric or list context (required vs available adena, rejected item
  ids) that the dropped string map (R5) could not.

- [done] R25. `CommandContext` MUST expose `NxSync sync()` so a handler can trigger an immediate
  sync pass for the entities it just mutated instead of waiting for the next scheduled CDC tick.
  `requestNow(...)` is fire-and-forget and never throws; an entity with no registered trigger is a
  silent no-op. The capability itself, `requestResync(...)` and trigger registration are specified
  in [`force-resync`](../021-force-resync.md); the per-command trigger mapping is in
  [`catalog.md`](./catalog.md).

- [done] R26. The reply's `Nx-Message-Type` MUST be derived from the **command class simple name**:
  strip a trailing `Command`, append `Result` (`TransferItemToCharacterCommand` →
  `TransferItemToCharacterResult`). It is derived, not read off the `R` type, so a routable reply
  type exists even when the command has no registered binding (unknown-type replies derive from
  the header value) and when sibling commands share one `R` class. The bytes are encoded once at
  registration and cached on the binding, keeping the dispatch hot path allocation-free.

**Non-goals:**

- **Per-domain Kafka topics** — single topic; cross-domain ordering per character is the more
  useful invariant than per-domain isolation.
- **Synchronous RPC mode** — replies always go through the replies topic; the web side correlates
  by `Nx-Correlation-Id`.
- **Per-handler threading flags** — the SPI is sync plus explicit `ctx.host().sync(...)` /
  `ctx.io()` hops. No `@OnGameThread` annotations or registration-time threading hints.
- **Idempotency / dedup / retry on the adapter side** — at-most-once (R11) makes dedup moot, and a
  failed command is re-issued by the operator, not by the adapter.
- **DLQ for poison records** — a record that fails to deserialize becomes a `VALIDATION_FAILED`
  reply and is left behind. A DLQ becomes a Could when ops shows a real need.
- **Per-handler rate limiting** — `RATE_LIMITED` exists so a handler can self-limit and say so;
  the adapter enforces nothing.
- **Schema registry / wire versioning beyond additive evolution** — same rule as events: adding a
  nullable field is compatible both ways, removing or retyping takes the two-release path.

### Edge cases

- **`Nx-Target-Server-Id` missing / malformed / other server** — record dropped, no reply,
  `other-server-skipped-total`++ (R22). A missing header is a producer bug and logs WARN; a
  wrong-target record is routine on a shared topic and logs DEBUG.
- **`Nx-Message-Type` missing** — the adapter cannot route and cannot derive a reply type; it
  replies `UNSUPPORTED_COMMAND` under the literal `CommandResult` type header.
- **`Nx-Correlation-Id` missing or malformed** — the handler still runs (correlation is opaque to
  the adapter) and gets a generated UUIDv7 so log tagging works; the reply carries that fallback,
  so the platform cannot correlate it and the caller times out. WARN logged.
- **Handler returns `null`** — wrapped as `INTERNAL_ERROR` with
  `error.cause = "handler-returned-null"`; the SAM contract says non-null.
- **Handler throws `RuntimeException`** — replied as `INTERNAL_ERROR`, stack logged at WARN. There
  is no redelivery to retry into, which is the point: the same input would hit the same bug.
- **Reply producer back-pressure** — replies go into the producer's record accumulator (R13). A
  saturated accumulator blocks `send(...)` up to `delivery.timeout.ms` on the consumer thread,
  which slows the topic but cannot lose the offset position, since the offset was already
  committed.
- **`commandsRepliesTopic` unconfigured** — handlers run to completion, every reply is dropped and
  counted as failed; the web side times out and republishes. Operator misconfiguration, loud in
  the heartbeat.
- **Commit failure** — the whole batch is dropped undispatched (R11). Callers time out and
  `commit-failures-total` is the page-worthy signal; it usually means a broker or ACL problem.
- **Adapter shutdown with in-flight handlers** — `stop()` wakes the poll and joins within
  `shutdown-timeout-ms`; the current record finishes and its reply is handed to the producer
  before the IO pool and the events publisher go down (R17). Records already committed but not yet
  dispatched are lost by design.
- **`NxAdapter.hostExecutor(...)` never called** — read-only handlers keep working; anything
  hopping to `ctx.host()` throws `IllegalStateException`. WARN at bootstrap (R19).
- **Handshake re-roll mid-life** — the previous consumer is stopped and a new one built behind the
  same facade and registry (R10/R17); registered handlers are untouched. In-flight commands either
  finish or are lost with the dropped batch.

## Technical design

### Overview

Two artifacts. `nx-gs-adapter-api` owns the wire and SPI types — `NxCommand<R>`, `CommandResult`,
`CommandStatus`, `CommandProblem`, the per-group command/result DTOs, and the `NxCommands` /
`CommandHandler` / `CommandContext` / `HostExecutor` / `NxSync` interfaces — with zero runtime
dependencies so hosts and the platform can both compile against it. `nx-gs-adapter-core` owns the
runtime: one Kafka consumer thread, a registry keyed by `Nx-Message-Type`, and a reply publisher
riding the existing producer.

### Structure

`nx-gs-adapter-core/.../core/commands/`:

| file                  | role                                                                               |
| --------------------- | ---------------------------------------------------------------------------------- |
| `CommandsBootstrap`   | public factory: `start(...)` / `swap(...)`, consumer property composition          |
| `CommandsConsumer`    | poll loop, commit-before-dispatch, per-record dispatch, reply publish, stats       |
| `CommandTypeRegistry` | `ConcurrentHashMap<simpleName, CommandTypeBinding>`, populated by `NxCommands.on`  |
| `CommandTypeBinding`  | command class + handler + pre-encoded reply-type bytes (R26)                       |
| `NxCommandsImpl`      | the `NxCommands` facade, `AtomicReference<CommandTypeRegistry>` for reconnect swap |
| `CommandContextImpl`  | per-record context (correlation id + session-scoped capabilities)                  |
| `HostExecutorImpl`    | latch-based bounded hop onto the host executor                                     |
| `CommandsConfig`      | resolved `l2nx.commands.*` knobs                                                   |

Everything except `CommandsBootstrap`, `CommandsConsumer` and `CommandsConfig` is
package-private — callers see only the api-side interfaces.

`nx-gs-adapter-api/.../kafka/commands/` holds the envelope trio in the package root and the
command/result DTOs in per-group subpackages; `.../spi/` holds the capability interfaces.

### Key components

- **`CommandsConsumer`** (R11, R13, R22) — the whole dispatch story: one daemon thread, one
  `commitSync` per non-empty batch before any handler runs, one reply per record. Holds all
  counters as `AtomicLong` and renders them into `CommandsStats` on the heartbeat thread.
- **`CommandTypeRegistry` + `CommandTypeBinding`** (R9, R26) — the routing table. The binding
  pre-computes the reply-type bytes at registration so the hot path neither re-derives the name
  nor re-encodes UTF-8.
- **`NxCommandsImpl`** (R10) — the indirection that makes the facade outlive the consumer: hosts
  cache `ctx.commands()` once, the reference behind it is swapped on every reconnect.
- **`HostExecutorImpl`** (R8) — `CountDownLatch` + `AtomicReference` result/error holders, bounded
  `await`. On timeout it abandons the task (which keeps running on the host pool, unobserved) and
  throws rather than waiting forever.

### Dispatch flow

```
poll(pollTimeoutMs)
  └─ empty? → continue
  └─ commitSync()                              ← at-most-once gate
       └─ failed → commit-failures-total++, WARN, drop the whole batch
  └─ per record:
       1. Nx-Target-Server-Id != own → other-server-skipped-total++, drop, no reply
       2. Nx-Message-Type → CommandTypeRegistry.lookup  → miss: UNSUPPORTED_COMMAND
       3. Nx-Correlation-Id → UUID (fallback UUIDv7 on missing/malformed)
       4. gson.fromJson(value, binding.commandClass) → JsonSyntaxException: VALIDATION_FAILED
       5. handler.handle(cmd, new CommandContextImpl(...))
            HostExecutorTimeoutException → UNAVAILABLE
            RuntimeException             → INTERNAL_ERROR
            null return                  → INTERNAL_ERROR
            Error                        → no reply; thread unwinds into SafeRunnable
       6. sendReply(corrId, replyTypeBytes, result)   ← fire-and-forget
```

### Decisions

- **At-most-once over at-least-once.** Commands are operator-issued RPCs with a human watching the
  result, not a data stream. Double-executing "delete item" or "send mail" is worse than a visible
  timeout, and pushing idempotency onto every host handler was a permanent tax to buy a guarantee
  nobody wanted. Committing before dispatch also removes the poison-record replay loop for free.
  Cost: a crash mid-handler silently loses the record, which is why `commit-failures-total` and
  the caller-side timeout are the operational signals.
- **Commit failure drops the batch.** The alternative — leave the offsets and retry — turns a
  broker or ACL problem into an infinite redelivery loop that re-runs handlers each time. Dropping
  keeps the failure mode aligned with the at-most-once contract.
- **One status enum with a tier, not a boolean plus an error code.** Callers need two granularities
  — "retry / surface / page" and "exactly what went wrong" — and a tier on each constant gives both
  from one field, with no illegal combinations to validate.
- **A problem body, not a string map.** Failures carry numbers and lists (required vs available
  adena, rejected item ids); `Map<String,String>` forced stringification at the producer and
  re-parsing at the consumer. `CommandProblem` also keeps failure context out of the success
  payload type, so `R` stays a clean success contract.
- **A dedicated `{X}Result` per command.** Even for a "void success" the platform wants
  confirmation data (what was actually deleted, whether the character was logged out), and a
  shared payload type would have made every command's reply shape a runtime question instead of a
  compile-time one.
- **Reply type derived from the command name, not the `R` class.** The adapter must name a reply
  type even for commands it cannot bind (unknown `Nx-Message-Type`), and sibling commands
  legitimately share one `R`. Deriving from the command name keeps the header 1:1 with the request
  in both cases.
- **Target-server filtering in the adapter, not per-server topics.** One topic per tenant keeps
  ACLs, topic count and platform-side producer wiring flat; the cost is that every adapter reads
  every tenant record and drops most of them, which is cheap at command volumes.
- **Replies bypass the events queue.** The events publisher's bounded queue drops on overflow,
  which is correct for a stale snapshot and wrong for a reply — a dropped reply has no semantic
  recovery, only a timeout.

## Rollout

`COMMAND_EXPIRED` (R23) is an added enum constant, which is safe only in platform-first order:
release `api/vX.Y.Z`, deploy the platform consumer, and only then let a host start emitting it.

Ordering is enforced at compile time rather than by convention: nx-gameservers has three exhaustive
`switch` expressions over `CommandStatus` with no `default` branch
(`domain/commands/CommandFailures.java`, `domain/privatestore/PrivateStoreBuyService.java`,
`infra/rest/GlobalExceptionHandler.java`), so it will not build against the new api until each one
handles the constant. Picking the HTTP status it maps to is nx-gameservers' call, not this repo's —
but it is owed before the constant can be emitted.

The runtime hazard is the fallback case only: a platform pod still running an older api jar parses
replies with Jackson and fails on the unknown value. The blast radius is one reply —
`CommandReplyConsumer` catches `JacksonException` per record — so the caller times out rather than
the consumer dying.

Separately, dropping `l2nx.commands.reply-flush-timeout-ms` and the drain gate is a config-surface
change in `nx-gs-adapter-core` → `core/vX.Y.Z` minor bump. No coordination is needed: the knob was
inert, and a host that still sets it in `l2nx.properties` is unaffected because unknown keys are
ignored. `CommandsConfig`'s constructor loses a parameter, so any code constructing it directly
(tests, alternate wiring) must drop the argument.

## Open questions

- Reply `Nx-Message-Type` is derived from the command class name (R26), but `guide.md` states it
  "matches the R class name" and its lifecycle diagram shows `TransferItemToCharacterCommandResult`
  (keeping the `Command` infix). Both are wrong against the code, and the divergence is observable:
  `DeleteAutoAnnouncementCommand` (`R = Void`) replies as `DeleteAutoAnnouncementResult`, and
  `StartPrivateStorePackageSellCommand` (`R = StartPrivateStoreResult`) replies as
  `StartPrivateStorePackageSellResult`. [resolved: code wins; `guide.md` now states the derivation
  source and both divergent cases.]
- [resolved: `guide.md`'s "no `Void`" absolute softened — `DeleteAutoAnnouncementCommand implements
NxCommand<Void>` is the sanctioned exception, and payload-less `CommandResult.ok()` exists for it.]
- [resolved: `CommandHandler`'s Javadoc described the pre-R11 world ("at-least-once", "handlers MUST
  be idempotent", "the record is redelivered", `errorDetails`) and was rewritten to at-most-once. The
  same stale idempotency advice was removed from the five command DTOs that carried it —
  `TransferCharToAccountCommand`, `DeleteItemCommand`, `TransferItemToCharacterCommand`,
  `SendMailCommand`, `TelegramCharLinkCommand`. A correlation-id dedup cache was never useful anyway:
  the platform mints a fresh id per dispatch, so a re-issue after a reply timeout does not match it.]
- [resolved: the dead reply-drain machinery is gone — `awaitRepliesDrain()`, `replyFlushTimeoutsTotal`,
  the `pendingReplies` / `replyDrainLock` bookkeeping, and the `l2nx.commands.reply-flush-timeout-ms`
  knob with its parsing and validation. It gated nothing under at-most-once, and
  `KafkaProducer.close(timeout)` in `NxKafka.doShutdown` already waits out in-flight reply sends
  (the consumer stops before the producer closes). Config-surface change → core takes a minor bump.]
- [resolved: inbound partition key is the producer's call and both docs were half right —
  `nx-gameservers` `CommandsSender.publish` passes a nullable `Long`: `charId` for character-scoped
  commands, `null` otherwise. `MessagingTopics` Javadoc and `guide.md` now both say that.]
- [resolved: single Kafka topic; cross-character cross-domain ordering beats per-domain isolation.]
- [resolved: sync handler + explicit `ctx.host().sync(...)` hop. Auto-hop was rejected because it
  bottlenecks read-only handlers through the game executor.]
- [resolved: DTOs live in `nx-gs-adapter-api` — the adapter owns the canonical catalog — split into
  per-group subpackages for discovery only.]
- [resolved: replies ride a separate topic (`<tenant>.gs.commands.replies`), not a family inside the
  events stream. Different retention SLA, semantically RPC not fact.]
- [resolved: migration off RabbitMQ is a per-command cutover; both consumers ran in parallel and
  RabbitMQ retired with the last migrated command.]
- [resolved: handler `RuntimeException` → `INTERNAL_ERROR` reply. There is no redelivery to retry
  into, and retrying would hit the same bug.]

## Links

- Companion: [`guide.md`](./guide.md) — handler-author walkthrough
- Companion: [`catalog.md`](./catalog.md) — per-command wire contract
- Sibling feature (outbound events, inbound placeholder):
  [`docs/specs/008-messaging.md`](../008-messaging.md)
- Sibling feature (Tier-1 SPI plumbing):
  [`docs/specs/002-adapter-modules/spec.md`](../002-adapter-modules/spec.md)
- Sibling feature (topic delivery contract):
  [`docs/specs/001-adapter-bootstrap.md`](../001-adapter-bootstrap.md)
- Sibling feature (`Nx-Server-Id` header stamping):
  [`docs/specs/007-per-server-sync.md`](../007-per-server-sync.md)
- Sibling feature (`NxSync` / force-resync): [`docs/specs/021-force-resync.md`](../021-force-resync.md)
- Follow-up command slice: [`docs/specs/010-commands-send-mail.md`](../010-commands-send-mail.md)
- Follow-up command slice: [`docs/specs/024-ban-commands.md`](../024-ban-commands.md)
- Legacy reference (RabbitMQ command surface, web side):
  `E:/bohpts/code/bohpts-rabbitmq/src/main/java/com/bohpts/messaging/`
- Legacy reference (RabbitMQ command surface, core side):
  `E:/projects/bohpts/bohpts-core/core/src/main/java/l2e/gameserver/infrastructure/rabbitMq/`
