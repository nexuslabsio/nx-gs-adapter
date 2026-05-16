# Commands — Inbound RPC Surface

> Owner: @n1rmata

> **Update:** This spec was written for the initial slice (at-least-once,
> `boolean success + ErrorCode` envelope, `*Payload` reply type). The
> implementation has since evolved — see the canonical [`guide.md`](./guide.md)
> for the current contract. Key deltas:
> - **At-most-once consumer** — commit before dispatch; handlers do NOT need
    > to be idempotent; reply-flush gate is dormant
> - **Single `CommandStatus` enum** with nested `Tier` (OK / CLIENT_ERROR /
    > SERVER_ERROR) replaces `boolean success + ErrorCode`
> - **`CommandProblem`** (RFC 9457-subset: title + detail + extensions)
    > replaces the free-form `errorDetails` map
> - **`*Result` per command** (no `Void`, no shared `*Payload`) — every
    > command echoes confirmation data
> - **Wire `Nx-Message-Type` reply header** derives via strip `Command` +
    > add `Result` (e.g. `TransferItemCommand` → header `TransferItemResult`)
> - **`NxSync`** SPI added for out-of-band sync trigger from handlers

## Problem

The L2 game-server core needs a structured way to receive operational commands from the
platform's web side — kicks, mail sends, item operations, account punishments,
character ↔ telegram pairings — and reply with success / structured error.

Today bohpts uses a hand-rolled RabbitMQ surface
(`l2e.gameserver.infrastructure.rabbitMq`) that:

1. Auto-acks messages BEFORE the handler runs (drops on handler failure)
2. Runs handlers on the RabbitMQ consumer thread (races against game-state mutations)
3. Routes by "who sent it" (admin queue vs telegram queue) instead of "what to do"
   (same DTO duplicated across queues)
4. Couples handlers directly to game-server internals (`GameObjectsStorage`, `MailManager`,
   `CharacterDAO`)
5. Carries no tenant / server identity in the envelope
6. Replies with a free-form `message` field (no structured errorCode)
7. Embeds correlation id in the message body instead of headers

Sibling [`messaging`](../messaging/spec.md) feature already shipped the **outbound** events
surface (Phase 1) and committed the wire-shape sketch for inbound commands as a Javadoc
placeholder. This slice realizes the runtime: Kafka consumer + dispatch + handler SPI +
manual-ack + reply-via-publisher.

The DTO catalog itself is **out of scope** of this slice — the legacy bohpts command set
(15 commands across 2 queues) will be redesigned in a follow-up slice once the wire is
live. This slice ships the **infrastructure** so the catalog work is purely additive.

Audience: bohpts-core (and future per-tenant) command-handler authors who write business
logic for individual command types; platform-side operators who consume reply events.

## Requirements

> Sibling features carry the SPI plumbing and topic delivery contract:
> - Tier-1 SPI (`AdapterModule` + ServiceLoader) lives in [`adapter-modules`](../adapter-modules/spec.md).
    > Commands runtime is **not** a discovered Tier-1 module — it lives inside `nx-gs-adapter-core`
    > as a built-in capability surfaced through `ConnectContext`, symmetric to the
    > [`messaging`](../messaging/spec.md) `events` surface.
> - Existing `Nx-Server-Id` + `Nx-Message-Type` + `Nx-Correlation-Id` header contracts from
    > [`per-server-sync`](../per-server-sync/spec.md) and [`messaging`](../messaging/spec.md)
    > are reused unchanged.

**Must:**

- [todo] R1. `nx-gs-adapter-api.rest.MessagingTopics` MUST replace its Phase-1
  placeholder `commands: Map<String,String>` field with two single-topic fields:
    - `@Nullable String commandsTopic` — fully-qualified inbound topic
      (e.g. `<tenant>.gs.commands`). `null` / absent → commands surface disabled.
    - `@Nullable String commandsRepliesTopic` — fully-qualified outbound replies topic
      (e.g. `<tenant>.gs.commands.replies`). `null` / absent → replies disabled (handlers
      run, but reply records go nowhere — useful for fire-and-forget admin commands).
    - The Phase-1 `Map<String,String> commands` field is dropped; Phase 1 shipped it empty,
      so platform-side payloads emitting `"commands": {}` are harmless under Gson's
      ignore-unknown-fields default.
    - `events: Map<String,String>` — UNCHANGED.

- [todo] R2. Single inbound Kafka topic, partitioned by character id. Web side composes
  `ProducerRecord(commandsTopic, key=longBytesBe(charId), headers=[Nx-Message-Type,
  Nx-Correlation-Id])` for every command. Cross-domain ordering per character is preserved
  by partitioning. Web side MUST resolve human identifiers (account name, character name)
  to `charId` BEFORE sending; account-level commands (rare — IP bans, account-wide
  punishments) MAY use a synthetic key derived from `accountName.hashCode()` and accept
  reduced cross-character ordering.

- [todo] R3. `nx-gs-adapter-api.kafka.commands.NxCommand<R>` MUST be a
  type-parameterized marker interface for inbound command DTOs. The type
  parameter `R` declares the command's success-payload type — fixed at the
  command class declaration, not at handler-registration time. This makes
  the wire reply contract **statically typed**: the platform-web side and
  the host-side handler look at the same `NxCommand<R>` binding and cannot
  disagree about reply shape.

  Concrete DTOs MUST live under
  `app.l2nx.gs.adapter.api.kafka.commands.<group>.*` (group = code-org bucket:
  `character`, `item`, `mail`, `account`) — the topic remains single, the
  package split is for Javadoc / discovery.

  Use `NxCommand<Void>` for commands that produce success/error envelopes
  with no typed payload. Use `NxCommand<MyPayload>` when the reply carries
  typed data (e.g. `CharInfoCommand implements NxCommand<CharInfoPayload>`).

  **Phase-2.0 starter catalog** (proof-of-life DTO so the released api artifact
  has a usable command type for integration testing on the bohpts side):
    - `kafka.commands.item.DeleteItemCommand implements NxCommand<Void>` —
      replaces legacy `DeleteItemRequestV1`. Fields: `Long charId`,
      `Long itemObjectId`, `Long count` (semantically REQUIRED, builder
      defaults `count` to `1`, `count` MUST be positive). Reply payload:
      `Void` — only success/error envelope. Constructor enforces non-null +
      positive-count via `IllegalArgumentException` for programmatic
      construction; wire-path Gson bypasses the constructor via `Unsafe`, so
      handler is responsible for null-checking and emitting
      `VALIDATION_FAILED` on missing wire fields. Field renames vs legacy:
      `charIdFrom` → `charId` (no "from" semantic for delete); `count` is
      new (legacy always deleted full stack).

  Other legacy commands (`KickPlayerRequestV1`, `SendMailRequestV1`,
  `PunishmentRequestV1`, …) land in follow-up slices once the catalog is finalized;
  spec lists 15 known legacy entries in the brainstorm dialog log for reference.

- [todo] R4. `nx-gs-adapter-api.kafka.commands.CommandResult<R>` MUST ship as a
  Gson-friendly POJO replacing legacy `ResponseV2`:
    - `boolean success` — REQUIRED.
    - `@Nullable ErrorCode errorCode` — REQUIRED iff `success=false`.
    - `@Nullable Map<String,String> errorDetails` — optional structured context
      (`{"charId": "12345"}` / `{"error.class": "IllegalStateException"}`); getter
      normalizes `null` → empty map.
    - `@Nullable R payload` — typed payload for success cases.
    - Static factories: `success()`, `success(R)`, `error(ErrorCode)`,
      `error(ErrorCode, String key, String value)`. Fluent builder for
      multi-detail errors.
    - Hand-written `Builder` + `toBuilder()` + `equals` / `hashCode` / `toString`.

- [todo] R5. `nx-gs-adapter-api.kafka.commands.ErrorCode` enum MUST ship with the
  following members:
    - `NOT_FOUND`, `INVALID_STATE`, `FORBIDDEN`, `RATE_LIMITED`, `UNAVAILABLE`,
      `VALIDATION_FAILED`, `INTERNAL_ERROR` — handler-emitted.
    - `UNSUPPORTED_COMMAND` — adapter-emitted when no handler is registered for the
      received `Nx-Message-Type` header.

- [todo] R6. `nx-gs-adapter-api.spi.CommandHandler<C extends NxCommand<R>, R>` MUST
  ship as a SAM:
    ```java
    @FunctionalInterface
    public interface CommandHandler<C extends NxCommand<R>, R> {
        CommandResult<R> handle(C command, CommandContext ctx);
    }
    ```
  The bound `C extends NxCommand<R>` forces the handler's reply payload type to
  match the command class's declared type at compile time — a handler for
  `DeleteItemCommand` (`NxCommand<Void>`) cannot return
  `CommandResult<String>`; the compiler rejects it. Handler runs synchronously
  on the adapter's commands consumer thread. Game-state mutations require an
  explicit `ctx.host().sync(...)` hop.

- [todo] R7. `nx-gs-adapter-api.spi.CommandContext` MUST expose:
    - `UUID correlationId()` — the inbound `Nx-Correlation-Id`, useful for log tagging.
    - `HostExecutor host()` — the host's Executor wrapper for game-state hops.
    - `NxEvents events()` — handler MAY publish side-effect events during processing.
    - `Executor io()` — adapter-owned IO pool for blocking JDBC / HTTP work
      inside handlers. Backed by `nx-io-N` daemon threads sized by
      `l2nx.io.workers` (default `max(2, cores/2)`). Handlers MUST hop to
      `ctx.io()` for blocking IO instead of running JDBC on the consumer
      thread or burning game-thread capacity via `ctx.host().sync(...)`.
      `ctx.host().sync(...)` is reserved for game-state mutations that
      must run on the game loop.

- [todo] R8. `nx-gs-adapter-api.spi.HostExecutor` MUST expose:
    ```java
    public interface HostExecutor {
        void sync(Runnable task);
        <T> T sync(Supplier<T> task);
        void async(Runnable task);
    }
    ```
  `sync` blocks the caller until the host's executor finishes the task OR the
  configured `l2nx.commands.host-sync-timeout-ms` window elapses (default 30s),
  whichever comes first. On timeout `sync` throws
  `app.l2nx.gs.adapter.api.spi.HostExecutorTimeoutException` (a typed
  `RuntimeException`); the dispatcher catches and emits a
  `CommandResult.error(UNAVAILABLE)` reply with
  `error.cause = "host-executor-timeout"` + `timeout.ms` detail. Bounded await
  is load-bearing — an unbounded await would let a saturated host pool wedge
  the consumer thread indefinitely. Thrown exceptions other than the timeout
  propagate to the handler. `async` is fire-and-forget (handler does not
  wait); the adapter wraps the task in `SafeRunnable` so any `Throwable` is
  logged via `NxLog` rather than reaching the host thread's UEH. Implementation
  wraps a host-supplied `java.util.concurrent.Executor`.

- [todo] R9. `nx-gs-adapter-api.spi.NxCommands` registration SPI MUST expose:
    ```java
    public interface NxCommands {
        <R, C extends NxCommand<R>> void on(Class<C> type, CommandHandler<C, R> handler);
    }
    ```
  Acquired via `ConnectContext.commands()`. The bound `C extends NxCommand<R>`
  ensures the handler's reply type matches the command's declared payload at
  registration time — there is no runtime way for the host and platform to
  disagree about the reply contract. Registration window opens at the host's
  `onConnect(ctx)` callback; late registration (after the consumer thread has
  started) is permitted (idempotent `Map.put`). Re-registering the same
  `Class` overwrites the previous handler (last write wins).

- [todo] R10. `nx-gs-adapter-api.spi.ConnectContext` MUST expose `NxCommands commands()`
  accessor — symmetric to existing `events()`. Returned facade is non-null; when
  `commandsTopic` is unconfigured, the facade still accepts `on(...)` calls (so host code
  can call it unconditionally) but no consumer thread runs. `ConnectContext` also
  exposes `Executor io()` (module-level) returning the adapter-owned IO pool so
  module code (non-handler) can dispatch blocking IO without burning the game-thread
  executor.

  `NxCommandsImpl` façade identity is **stable across reconnect cycles**: the
  façade handed out from `ConnectContext.commands()` survives platform handshake
  re-rolls — an internal `AtomicReference` is swapped to the live consumer instance
  on every reconnect. Handler registry survives reconnect too: handlers registered
  via `ctx.commands().on(...)` persist across reconnect cycles without re-registration.
  Closes the reconnect race where a host that cached the façade after the first
  `onConnect` would have continued to call a dead consumer.

- [todo] R11. `nx-gs-adapter-core` MUST implement an internal `CommandsConsumer`:
    - One bounded `KafkaConsumer<byte[], byte[]>` polling `commandsTopic` from a single
      daemon thread `nx-commands-consumer` (uncaught-handler installed, `Throwable`
      caught in the poll loop). Phase-2 START is single-threaded across all
      assigned partitions; multi-thread (one-per-partition) is a follow-up scaling
      step.
    - Manual offset management: `consumer.poll(...)` → process each record sequentially
      → await per-batch reply-flush gate → `consumer.commitSync()` after the batch.
      `enable.auto.commit=false` is enforced at the Kafka facade level (the messaging
      slice flipped the default), so manual-commit semantics are now actually
      enforceable. The reply-flush gate (controlled by
      `l2nx.commands.reply-flush-timeout-ms`, default 5000) blocks the consumer
      thread until every reply send issued during the batch has fired its
      Kafka producer callback (success OR failure), bounding the at-most-once
      reply window. `CommandsConsumer.awaitRepliesDrain` returns `boolean`:
      `true` on drained-cleanly, `false` on interrupted-with-pending-replies.
      On `false` the consumer **skips** `commitSync` for the batch so unacked
      replies redeliver on next start — this closes a window in the
      at-least-once contract under shutdown. Setting `reply-flush-timeout-ms=0`
      opts out — replies become at-most-once on JVM crash.
    - Per-record dispatch:
        1. Read `Nx-Message-Type` header → lookup binding in `CommandTypeRegistry`
        2. Read `Nx-Correlation-Id` header → capture for reply
        3. Deserialize value bytes via Gson into the binding's `Class`
        4. Build `CommandContext`; invoke `handler.handle(cmd, ctx)`
        5. Catch `HostExecutorTimeoutException` → reply
           `CommandResult.error(UNAVAILABLE)` with
           `error.cause = "host-executor-timeout"` + `timeout.ms` details
        6. Catch handler `RuntimeException` → reply `INTERNAL_ERROR` with
           `error.class` + `error.message` details
        7. Reply published asynchronously via the supplied `ReplySender`
           (production: `NxKafka.sendBytesKeyRecord`); per-batch reply-flush
           gate blocks before offset commit
    - Exception handling boundaries:
        - Unknown `Nx-Message-Type` → reply `UNSUPPORTED_COMMAND`, commit offset
        - Gson `JsonSyntaxException` on deserialization → reply `VALIDATION_FAILED` with
          `parse` detail, commit offset
        - Handler `HostExecutorTimeoutException` → reply `UNAVAILABLE` with
          `error.cause = "host-executor-timeout"`, commit offset
        - Handler `RuntimeException` → reply `INTERNAL_ERROR` with class+message, commit
          offset
        - Handler returns `null` → reply `INTERNAL_ERROR` with
          `error.cause = "handler-returned-null"`, commit offset
        - Handler `Throwable` (OOM, StackOverflow) → log ERROR, do NOT reply, do NOT
          commit, rethrow (let JVM-level handler take over)

- [todo] R12. `nx-gs-adapter-core` MUST expose a `NxAdapter.hostExecutor(Executor)`
  static method that the host calls during bootstrap to register its game-side
  Executor. Symmetric to existing `NxAdapter.onStateChange(...)`. Calling
  `ctx.host().sync(...)` without a registered executor MUST throw
  `IllegalStateException("HostExecutor not registered — call NxAdapter.hostExecutor(...) before start()")`.
  The handler-author sees the misconfiguration at the first hop.

- [todo] R13. Reply path MUST publish directly via the existing
  {@code NxKafka.sendBytesKeyRecord(record, callback)} producer. The Kafka producer's
  internal record-accumulator absorbs back-pressure; we do NOT route replies through the
  events publisher's bounded queue — the events queue's drop policy (whether `newest`
  or `oldest`) makes sense for stale snapshots but is wrong for command replies (a
  dropped reply means web-side timeout, no semantic recovery). Reply construction:
    - `key = longBytesBe(corrId.getMostSignificantBits())` (so replies for the same
      correlation hash are co-located on a partition — useful for tooling)
    - `headers: Nx-Server-Id` (auto via static-headers), `Nx-Correlation-Id` (= inbound
      corrId), `Nx-Message-Type = "<originalType>Result"` (e.g.
      `"KickCommandResult"`)
    - `value = gson.toJson(result)`
    - target topic = `commandsRepliesTopic` from `MessagingTopics`
    - send-callback bumps `replies-published-total` on success, `replies-failed-total`
      on Kafka exception

- [todo] R14. Adapter-core MUST surface a built-in `commands` heartbeat module slot
  in `HeartbeatEvent.enabledModules`:
    - `state` ∈ `{ACTIVE, DEGRADED, DISABLED}` — `DISABLED` when commandsTopic is
      unconfigured, `DEGRADED` when `internal-errors-total / consumed-total` exceeds 5%
      over the last minute (deferred in implementation; raw counters first).
    - `stats` exposes a new `CommandsStats` POJO (parallel to existing `EventsStats`):
        - `consumed-total` — records pulled from Kafka
        - `handled-total` — successfully dispatched (success OR business-error reply)
        - `unsupported-total` — `UNSUPPORTED_COMMAND` replies
        - `validation-failed-total` — Gson deserialization failures
        - `internal-errors-total` — handler `RuntimeException` count
        - `replies-published-total` — replies acknowledged by Kafka
        - `replies-failed-total` — reply send-callback failures
        - `registered-types` — list of `Nx-Message-Type` strings the registry knows about
          (debugging aid; static across runtime once host onConnect completes)
        - `commit-failures-total` — Kafka offset commit errors (ops alert signal)

- [todo] R15. Engine config under `l2nx.commands.*` (file-first source chain, parallel
  to `l2nx.events.*`):
    - `l2nx.commands.poll-timeout-ms` (default `100`)
    - `l2nx.commands.shutdown-timeout-ms` (default `5000`)
    - `l2nx.commands.host-sync-timeout-ms` (default `30000`) — bounded await
      on `ctx.host().sync(...)`; on timeout `HostExecutorTimeoutException`
      → reply `UNAVAILABLE`. Load-bearing safety against host-pool wedges.
    - `l2nx.commands.reply-flush-timeout-ms` (default `5000`) — per-batch
      reply-flush gate before offset commit; bounds at-most-once reply
      window. Set to `0` to opt out (replies become at-most-once on JVM crash).
    - `l2nx.commands.kafka.<property>` — proxied to `KafkaConsumer` properties
      (e.g. `l2nx.commands.kafka.max.poll.records=50`)

- [todo] R16. Kafka consumer config (composed from the platform-issued `KafkaConfig`
    + `l2nx.commands.kafka.*` overrides):
        - `bootstrap.servers` = `KafkaConfig.bootstrap` (from connect response)
        - `client.id` = `nx-gs-adapter-<tenant>-<server>-commands` (broker logs only,
          not ACL-checked)
        - `group.id` = `<tenant>.gs.commands.<server>` — lives under the per-tenant
          `<tenant>.` prefix so the `User:<tenant>` SCRAM principal's group ACL
          (prefixed on `<tenant>.` by `nx-infra/.../create-tenant.sh`) covers it.
          Single-consumer group; per-server isolation comes from the `<server>` suffix.
          **Migration note:** on first redeploy after this group.id rename, an
          adapter that previously committed offsets under the legacy group
          (`nx-gs-adapter-<tenant>-<server>-commands`) starts fresh under the new
          group with `auto.offset.reset=earliest` — the retained commands window
          is replayed once. Aligns with the at-least-once contract; host handlers
          are already expected to dedupe by `correlationId` (see "Idempotency
          dedup on adapter side" below). The legacy group's offsets become
          orphaned in `__consumer_offsets`.
        - `key.deserializer` / `value.deserializer` = `ByteArrayDeserializer`
        - `enable.auto.commit` = `false` (manual commit per R11)
        - `auto.offset.reset` = `earliest` (replay from beginning if offsets missing — at-least-once)
        - `max.poll.records` = `50` (default; tuneable for throughput vs latency)
        - `security.protocol` / `sasl.*` = same as producer-side

- [todo] R17. Lifecycle integration with `NxAdapter`:
    - `NxAdapter.start()` accepts a pre-registered `hostExecutor` (set via
      `NxAdapter.hostExecutor(Executor)` before `start()`); stored statically.
    - On Kafka init complete (`initKafka` in `NxAdapter`), build `CommandsBootstrap.start(...)`
      with `messagingTopics`, the host executor, the events publisher (for reply path),
      and the resolved `CommandsConfig`. Returns a `Started` bundle (parallel to
      `EventsBootstrap.Started`) carrying the `CommandsConsumer` (for shutdown +
      heartbeat) and the `NxCommands` facade (for `ConnectContext.commands()`).
    - The `ConnectContext` handed to `ModuleRegistry.connect(ctx)` carries the new
      `commands()` accessor — host modules call `ctx.commands().on(...)` from their
      `onConnect` callback to register handlers.
    - `NxAdapter.shutdown()` invokes `consumer.stop()` on the `CommandsConsumer`
      AHEAD of `EventsPublisher.stop()` so in-flight replies have a chance to drain
      via the publisher's existing shutdown drain.

- [todo] R18. Module versions:
    - `nx-gs-adapter-api` = `0.14.0` — additive (new commands SPI types, `MessagingTopics`
      reshape, `ConnectContext.commands()`, `CommandsStats`, `ModuleStatus.Stats.commands`).
      0.13.0 platform → 0.14.0 adapter is forward-compatible (old `commands: Map` field
      ignored on the wire). 0.14.0 platform → 0.13.0 adapter is forward-compatible
      (new fields ignored).
    - `nx-gs-adapter-core` = `0.7.0` — new `commands/*` package, `NxAdapter.hostExecutor`,
      heartbeat slot, lifecycle wiring. Bumps API dep to `0.14.0`.
    - `nx-gs-kafka` — UNCHANGED. Adapter-core constructs its own `KafkaConsumer<byte[],
      byte[]>` directly using the platform-issued broker config; reply path reuses the
      existing `NxKafka.sendBytesKeyRecord`.

**Should:**

- [todo] R19. Adapter-core SHOULD log a WARN at start-up when `commandsTopic` is
  configured but `hostExecutor` is not registered. Handlers can still be registered
  and the consumer still runs — but any handler attempting `ctx.host().sync(...)` will
  throw, which is a misconfiguration the operator deserves to see early.

**Could:**

- [todo] R20. Multi-thread consumer pool — one thread per assigned partition, scales
  parallelism while preserving per-charId ordering. Phase-2 START is single-thread; this
  upgrades the consumer if cadence becomes a bottleneck.

- [todo] R21. Idempotency cache — adapter-core could maintain a small bounded LRU of
  recently-handled `correlationId`s and short-circuit duplicates (re-publish the cached
  reply). Phase 2 punts this to host handlers per the at-least-once contract.

**Non-goals:**

- **Concrete command DTOs** — `KickCommand`, `SendMailCommand`, etc. ship in a separate
  follow-up slice. This slice is the infrastructure only.
- **Per-domain Kafka topics** — single topic; the prior multi-topic sketch is dropped
  (per design dialog: cross-character cross-domain ordering on a single character is the
  more useful invariant than per-domain isolation).
- **Synchronous RPC mode** — replies always go via the events stream. Web side polls /
  consumes the replies topic to correlate.
- **Per-handler threading flags** — the SPI is sync + explicit `ctx.host().sync(...)`.
  No `@OnGameThread` annotations or threading mode hints at registration.
- **Idempotency dedup on adapter side** — at-least-once contract; host handlers
  responsible for dedup keyed on `correlationId`.
- **Command-level retries on adapter side** — handler returns `error(...)`, reply is
  published once, web side decides whether to retry by republishing with the same
  correlationId. No automatic retry inside adapter.
- **DLQ for poison records** — Phase 2 commits offsets after every batch (including
  records that failed to deserialize). Poison records become `VALIDATION_FAILED` replies
  and move on. DLQ is a future Could when ops shows a real need.
- **Per-handler rate limiting** — no built-in rate limiter. `RATE_LIMITED` is in the
  enum so handlers can self-rate-limit and reply with that code, but the adapter does
  not enforce.
- **Schema registry / wire versioning beyond additive evolution** — same as for events:
  adding a nullable field is backward+forward compatible; removing or retyping is a major
  api bump.

### Edge cases

- **`Nx-Message-Type` header missing** — adapter cannot route. Reply
  `UNSUPPORTED_COMMAND` with `error.cause = "missing-header"`, commit offset.
  Equivalent to "header value not in registry" from the platform's POV.
- **`Nx-Correlation-Id` header missing** — handler still runs (correlation is opaque
  to the adapter). The reply CANNOT be correlated by the platform; replies-publisher
  emits with a generated UUIDv7 fallback in the header so the record is not
  malformed. WARN logged.
- **Handler returns `null`** — adapter wraps as `INTERNAL_ERROR` with
  `error.cause = "handler-returned-null"`. CommandHandler contract says non-null.
- **Handler-side RuntimeException** — caught by adapter, replied as `INTERNAL_ERROR`,
  offset committed. Stacktrace logged at WARN. Redelivery (via Kafka rebalance) is an
  ops-recovery path — the same exception will fire and yield the same error reply, so
  redelivery is not a useful retry.
- **Reply producer back-pressure** — replies go directly through the Kafka
  producer's internal record accumulator (per R13), not the events bounded
  queue. If the accumulator saturates, `send(...)` will block on the producer
  thread up to `delivery.timeout.ms`; the per-batch reply-flush gate
  (R11/`l2nx.commands.reply-flush-timeout-ms`) then bounds how long the
  consumer waits before deciding to skip the offset commit. On gate timeout
  the batch is **not** committed → records redeliver → handlers re-emit
  (idempotency required on the host side).
- **`commandsTopic` configured but `commandsRepliesTopic` is null** — handlers run
  fully, replies are ATTEMPTED to publish but the publisher detects the missing topic
  and increments `replies-failed-total` + DEBUG log. Web side never sees a reply →
  timeout → republish. Operator misconfiguration; loud heartbeat counter helps debug.
- **Adapter shutdown with in-flight handlers** — `consumer.stop()` waits up to
  `l2nx.commands.shutdown-timeout-ms` for the current poll iteration to finish.
  In-flight handlers complete; their replies enqueued before the events publisher
  shutdown drain. Mid-batch crash → uncommitted records → redelivery on next start
  (handlers idempotent).
- **`NxAdapter.hostExecutor(...)` not called** — handler-side `ctx.host().sync(...)`
  throws ISE. Read-only handlers (no game state) keep working. WARN logged at start.
- **Re-`onConnect` (handshake re-roll mid-life)** — `CommandsConsumer.stop()` +
  fresh `start()` with the new context. Registry rebuilt (handlers re-registered by
  host's `onConnect`). Pre-stop in-flight commands either complete or get redelivered.

## Open questions

- [resolved: Single Kafka topic, key=charId. Cross-character cross-domain ordering
  is the more valuable invariant than per-domain isolation.]
- [resolved: Sync handler + explicit `ctx.host().sync(...)` hop. Auto-hop variant
  rejected because it bottlenecks read-only handlers through the game executor.]
- [resolved: DTOs live in `nx-gs-adapter-api` (adapter owns canonical catalog).
  Sub-packages by code-org domain (`character` / `item` / `mail` / `account`) for
  Javadoc / discovery; topic remains single.]
- [resolved: Reply-events stream is a separate topic
  (`<tenant>.gs.commands.replies`), not a family inside the events stream. Different
  retention SLA, semantically RPC-not-fact.]
- [resolved: Migration is per-command cutover. Web side feature-flags each command
  to Kafka or RabbitMQ during transition; bohpts-core runs both consumers in parallel
  during transition; RabbitMQ retired when last command migrated.]
- [resolved: Handler `RuntimeException` → reply `INTERNAL_ERROR` + commit offset.
  No Kafka-level redelivery on handler failure (would just hit the same error).]
- [assumed: Handler ergonomics — `commands.on(Class, handler)` registration name,
  `CommandResult.success() / error(code, key, val)` factories, `ctx.host().sync(...)`
  game-state hop, `INTERNAL_ERROR` auto-wrap on RuntimeException — all confirmed in
  the brainstorm dialog before this spec landed.]

## Links

- Sibling feature (outbound events + Phase-1 commands placeholder):
  [`docs/features/messaging/spec.md`](../messaging/spec.md)
- Sibling feature (Tier-1 SPI plumbing):
  [`docs/features/adapter-modules/spec.md`](../adapter-modules/spec.md)
- Sibling feature (topic delivery contract):
  [`docs/features/adapter-bootstrap/spec.md`](../adapter-bootstrap/spec.md)
- Sibling feature (`Nx-Server-Id` header stamping):
  [`docs/features/per-server-sync/spec.md`](../per-server-sync/spec.md)
- Companion document:
  [`docs/features/commands/guide.md`](./guide.md) — handler-author walkthrough
- Legacy reference (RabbitMQ command surface, web side):
  `E:/bohpts/code/bohpts-rabbitmq/src/main/java/com/bohpts/messaging/`
- Legacy reference (RabbitMQ command surface, core side):
  `E:/projects/bohpts/bohpts-core/core/src/main/java/l2e/gameserver/infrastructure/rabbitMq/`
