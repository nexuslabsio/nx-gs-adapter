# Commands — Handler Author's Guide

> Companion to [`spec.md`](./spec.md) (formal infra contract) and
> [`catalog.md`](./catalog.md) (per-command business contract — inputs,
> result fields, error statuses; what the platform-web side codes against).
> For developers writing command handlers in `bohpts-core` (or any host
> embedding `nx-gs-adapter-core`).

## What is a command?

A command is a Gson-encoded JSON message the platform's web side sends to
the game-server core asking it to **do something** — transfer items, send
mail, link a Telegram user to a character. Every command:

- Implements `app.l2nx.gs.adapter.api.kafka.commands.NxCommand<R>` where
  `R` is a dedicated `{X}Result` class declaring the success-payload
- Travels on a single Kafka topic `<tenant>.gs.commands`
- Carries two headers: `Nx-Message-Type` (the simple class name) and
  `Nx-Correlation-Id` (UUID issued by web side)
- Is dispatched by the adapter to a registered `CommandHandler`
- Produces a `CommandResult<R>` reply on `<tenant>.gs.commands.replies`

**Naming convention.** Every command's reply type is `{X}Result`
(strip `Command` suffix, append `Result`): `TransferItemToCharacterCommand` →
`TransferItemToCharacterResult`, `DeleteItemCommand` → `DeleteItemResult`. Wire
`Nx-Message-Type` header on the reply matches the R class name
(`TransferItemToCharacterResult`, not `TransferItemToCharacterCommandResult`).

## End-to-end lifecycle

```
┌──────────────────────────────────────┐
│ web caller                           │
│   cmd = TransferItemToCharacterCommand(...)     │
│   nxSender.sendSync(cmd, Void.class, │
│     correlationId).join()            │
└──────────────┬───────────────────────┘
               │ 1. register(corrId → Void, future)
               ▼
┌──────────────────────────────────────┐
│ PendingReplyRegistry                 │
│   ConcurrentHashMap<UUID, Slot>      │
└──────────────────────────────────────┘
               │ 2. publish
               ▼
┌──────────────────────────────────────┐
│ Kafka topic <tenant>.gs.commands     │
│   key:    null (partition by round-  │
│           robin; web-side may set    │
│           key=charId for ordering)   │
│   headers:                           │
│     Nx-Message-Type =                │
│       "TransferItemToCharacterCommand"          │
│     Nx-Correlation-Id = <uuid>       │
│   value:  Gson(cmd) bytes            │
└──────────────┬───────────────────────┘
               │ 3. poll
               ▼
┌──────────────────────────────────────┐
│ CommandsConsumer (adapter-core)      │
│   nx-commands-consumer daemon        │
└──────────────┬───────────────────────┘
               │ 4. commitSync FIRST (at-most-once)
               │ 5. lookup by Nx-Message-Type
               ▼
┌──────────────────────────────────────┐
│ CommandTypeRegistry                  │
│   "TransferItemToCharacterCommand" →            │
│      TransferItemToCharacterHandler             │
│   (populated by host module via      │
│    NxCommands.on(...))               │
└──────────────┬───────────────────────┘
               │ 6. Gson.fromJson(value, cmdClass)
               ▼
┌──────────────────────────────────────┐
│ handler.handle(cmd, ctx)             │
│                                      │
│   validate wire fields               │
│   ctx.host().sync(...)  ─── online →  game pool
│   ctx.io() supplyAsync  ─── offline → IO pool
│                                      │
│   on success: ctx.sync().requestNow( │
│     entity, pk)                      │
│                                      │
│   return CommandResult.ok(result)    │
└──────────────┬───────────────────────┘
               │ 7. fire-and-forget reply
               ▼
┌──────────────────────────────────────┐
│ Kafka topic                          │
│   <tenant>.gs.commands.replies       │
│   key:    bigEndian(corrId.msb)      │
│   headers:                           │
│     Nx-Message-Type =                │
│       "TransferItemToCharacterCommandResult"    │
│     Nx-Correlation-Id = <echoed>     │
│   value:  Gson(CommandResult) bytes  │
└──────────────┬───────────────────────┘
               │ 8. @KafkaListener
               ▼
┌──────────────────────────────────────┐
│ web-side reply consumer              │
│   read Nx-Correlation-Id             │
│   lookup slot → payloadType          │
│   Gson.fromJson(value,               │
│     CommandResult<payloadType>)      │
│   pending.complete(corrId, envelope) │
└──────────────┬───────────────────────┘
               │ 9. future.complete
               ▼
┌──────────────────────────────────────┐
│ web caller's .join() returns         │
│   CommandResult<Void>                │
│     success → state SUCCESS          │
│     error   → state + refund + ...   │
└──────────────────────────────────────┘
```

## Your first handler — the 30-second tour

```java
import app.l2nx.gs.adapter.api.kafka.commands.CommandResult;
import app.l2nx.gs.adapter.api.kafka.commands.character.KickCommand;
import app.l2nx.gs.adapter.api.kafka.commands.character.KickResult;
import app.l2nx.gs.adapter.api.spi.CommandContext;
import app.l2nx.gs.adapter.api.spi.NxCommands;

public final class BohptsCommandHandlers {

    public static void register(NxCommands commands) {
        commands.on(KickCommand.class, BohptsCommandHandlers::handleKick);
    }

    static CommandResult<KickResult> handleKick(KickCommand cmd, CommandContext ctx) {
        boolean wasOnline = ctx.host().sync(() -> {
            Player p = GameObjectsStorage.getPlayer(cmd.getCharId().intValue());
            if (p == null) return false;
            p.kick();
            return true;
        });
        if (!wasOnline) {
            return CommandResult.notFound("Character not online", "charId", cmd.getCharId());
        }
        return CommandResult.ok(new KickResult(cmd.getCharId()));
    }
}
```

Bootstrap (BEFORE `NxAdapter.start()`):

```java
NxAdapter.hostExecutor(task -> ThreadPoolManager.getInstance().executeGeneral(task));
NxAdapter.start();
```

Registration (inside the adapter's `onConnect` callback — typically an
`AdapterModule`):

```java
ctx.commands().on(KickCommand.class, BohptsCommandHandlers::handleKick);
```

That's the whole story. The rest is "why" and edge cases.

## Reply type — declared on the command, not on the handler

Every command declares its reply via `NxCommand<R>`. The handler MUST
return `CommandResult<R>` matching that `R` — the compiler enforces it.
Every command ships with a dedicated `{X}Result` class (no `Void` /
shared `Payload` types) so the reply echoes confirmation data even for
"void-success" commands.

```java
public final class TransferCharToAccountCommand implements NxCommand<TransferCharToAccountResult> { ... }

commands.on(TransferCharToAccountCommand.class, (cmd, ctx) -> {
    boolean wasLoggedOut = doRebind(cmd);
    return CommandResult.ok(new TransferCharToAccountResult(
            cmd.getCharId(), cmd.getAccountTo(), wasLoggedOut));
});
```

The platform-web side imports the same `nx-gs-adapter-api`, sees the
generic binding, and statically knows the reply shape. The wire contract
is the type system.

The envelope itself is always `CommandResult<R>`:

```
CommandResult {
  status:  CommandStatus       // OK | NOT_FOUND | INVALID_STATE | FORBIDDEN |
                               //   VALIDATION_FAILED | RATE_LIMITED |
                               //   UNAVAILABLE | INTERNAL_ERROR |
                               //   UNSUPPORTED_COMMAND
  payload: R?                  // non-null iff status == OK
  problem: CommandProblem?     // non-null iff status != OK
}

CommandProblem {                // RFC 9457-compatible subset
  title:      String           // short human-readable summary
  detail:     String?          // longer per-instance message
  extensions: Map<String,Object>// structured context (field, error.class, ...)
}
```

**Status routing.** Each `CommandStatus` carries a `Tier`
(`OK / CLIENT_ERROR / SERVER_ERROR`) for coarse HTTP-aligned routing:

```java
switch (reply.getStatus().tier()) {
    case OK            -> /* apply payload */
    case CLIENT_ERROR  -> /* surface to user; do not retry */
    case SERVER_ERROR  -> /* alert ops; maybe retry */
}
```

Fine-grained branching uses the status itself: `switch (reply.getStatus())`.

## At-most-once and fail-fast

Adapter consumer runs **at-most-once**: it commits the Kafka offset
**BEFORE** dispatching the batch. Consequences:

- A JVM crash (or graceful stop) mid-handler **drops** the in-flight
  records. They do NOT redeliver.
- Caller-side (`nxSender.sendSync`) sees a reply timeout (default 5 s) and
  surfaces `TimeoutException`.
- The human operator re-issues the command (admin UI button, bot retry,
  etc.) when they see a failure.

**Handlers do NOT need to be idempotent.** Every handler invocation is
"the one shot" — no retry, no dedup cache, no `correlationId` lookup
required.

```
poll → if (empty) continue
     → commitSync                ← at-most-once gate
     →   on commit failure → drop batch (log WARN, continue)
     → for each record:
         processRecord(record)   ← handler runs; reply fire-and-forget
```

What this trades off:

| Scenario               | At-most-once (current)               | Hypothetical at-least-once                                          |
|------------------------|--------------------------------------|---------------------------------------------------------------------|
| Handler succeeds       | reply sent                           | reply sent                                                          |
| JVM crash mid-handler  | record lost → operator re-issues     | record redelivers → handler re-invoked → idempotency required       |
| Reply lost in flight   | caller timeout → operator re-issues  | caller timeout → redeliver → handler re-invoked → dedup required    |
| Producer broker outage | adapter commit fails → batch dropped | offsets re-fetched → infinite redelivery loop until broker recovers |

The legacy `l2nx.commands.reply-flush-timeout-ms` knob is no longer
consulted by the dispatch loop; it stays in `CommandsConfig` for binary
compat and will be removed in a future minor.

## Threading model — what runs where

The adapter's commands consumer is a **single daemon thread**
(`nx-commands-consumer`). Per record:

1. Resolve handler from the type registry by `Nx-Message-Type` header
2. Deserialize via Gson
3. Call `handler.handle(cmd, ctx)` synchronously on the consumer thread
4. Publish the reply (fire-and-forget)

The handler is sync. Whatever you do in `handle(...)` blocks the consumer
thread for the duration. Slow handlers slow the topic.

### When to hop, and where

The consumer thread is NOT the host's game thread. Mutating game state
from the consumer thread races against the host's own loops (packet
handlers, AI ticks, scheduled tasks). Three pools, each with a clear
role:

| Pool            | Accessor                               | Use for                                                                  |
|-----------------|----------------------------------------|--------------------------------------------------------------------------|
| Game thread     | `ctx.host().sync(...)` / `.async(...)` | Game-state mutations, packet sends, in-memory game-loop touches          |
| Adapter IO pool | `ctx.io()` (Executor)                  | Blocking JDBC, blocking HTTP, FS calls — anything that waits on syscalls |
| Consumer thread | (no hop)                               | Pure CPU / non-blocking work only                                        |

Rules:

- **State mutations** (`Player.kick()`, `Player.destroyItem(...)`,
  `MailManager.sendMail(...)`) — MUST hop via `ctx.host().sync(() -> {...})`.
- **Blocking IO** (JDBC, HTTP, FS) — MUST hop to `ctx.io()`. Do NOT run
  JDBC on the consumer thread (blocks the entire topic) and do NOT run
  JDBC via `ctx.host().sync(...)` (burns game-thread capacity for IO that
  has nothing to do with the game loop).
- **Read-only non-blocking reads** of thread-safe caches (e.g.
  `GameObjectsStorage.getPlayer(...)` which is concurrent-safe) — fine to
  do directly on the consumer thread.

```java
static CommandResult<SendMailResult> handleSendMail(SendMailCommand cmd, CommandContext ctx) {
    // safe to read GameObjectsStorage from consumer thread
    Player p = GameObjectsStorage.getPlayer(cmd.getCharId().intValue());
    if (p == null) {
        return CommandResult.notFound("Character not found", "charId", cmd.getCharId());
    }

    // db write — hop to the adapter IO pool
    long messageId = CompletableFuture
            .supplyAsync(() -> MailDAO.insert(toRecord(cmd, p)), ctx.io())
            .join();

    // game state mutation — hop to the game executor
    ctx.host().sync(() -> MailManager.getInstance().notifyDelivery(p, messageId));

    return CommandResult.ok(new SendMailResult(List.of(messageId), List.of()));
}
```

### Dispatching online vs offline

The bohpts `DeleteItemHandler` and `TransferItemToCharacterHandler` are the canonical
patterns. Detect online state on the consumer thread, then dispatch:

```java
final Player online = GameObjectsStorage.getPlayer(charId);
if (online != null) {
    return ctx.host().sync(() -> doOnline(online, ...));
}
try {
    return CompletableFuture
            .supplyAsync(() -> doOffline(charId, ...), ctx.io())
            .join();
} catch (CompletionException ce) {
    ...
}
```

For multi-party operations (e.g. `TransferItemToCharacter` with two characters),
hop to the game pool if **any** party is online, and pass a dedicated
"offline-only" service entry point on the `ctx.io()` branch — the
service MUST NOT re-fetch `GameObjectsStorage` on the IO pool, or it
risks dispatching to game-state mutations from the wrong thread when a
concurrent login lands between the handler check and service execution.

### `sync` vs `async` on the host executor

`ctx.host().sync(...)` blocks the consumer thread up to
`l2nx.commands.host-sync-timeout-ms` (default 30 s). On timeout it
throws `HostExecutorTimeoutException` and the adapter replies
`CommandResult.error(CommandStatus.UNAVAILABLE, ...)` with `error.cause = "host-executor-timeout"`.

Use sync when:

- You need the return value of the host-side operation
- You want to fail-fast: if the host throws, you want to return
  `CommandResult.error(...)`
- You want strict ordering between hops

`ctx.host().async(...)` is fire-and-forget. Use it for:

- Side effects whose failure the platform doesn't care about
- Operations that can happen after the reply (audit log, follow-up
  broadcast)

Default to `sync`. Reach for `async` only with a specific reason.

## Out-of-band sync — `NxSync`

After a handler mutates persistent state (item transfer, character
account change, mail attach, …), platform consumers should observe the
new state immediately rather than wait for the next scheduled CDC tick
(typically 60 s). Use `ctx.sync().requestNow(entity, pk)`:

```java
static CommandResult<Void> handleTransferItem(TransferItemToCharacterCommand cmd, CommandContext ctx) {
    // ... do the transfer (online or offline)
    if (success) {
        ctx.sync().requestNow("item", cmd.getItemObjectId());
        ctx.sync().requestNow("character", Arrays.asList(fromId, toId));
        return CommandResult.ok(new TransferItemToCharacterResult(
                cmd.getItemObjectId(), countTransferred, fromId, toId));
    }
    // ...
}
```

The contract:

- **Sync semantic** — `requestNow` MUST NOT block longer than a queue
  submission. NEVER throws.
- **Unknown entity** — dropped silently with a DEBUG log. Handlers MAY
  call `requestNow` unconditionally; the no-op fallback applies when no
  sync module has registered a trigger for that entity.
- **Coalescing** — back-to-back `requestNow` for the same entity
  coalesce: a per-entity `ticking` guard prevents overlap with the
  scheduled CDC tick. Calling 3× for `"character"` does not run 3 full
  scans — at most one out-of-band cycle is queued behind the in-flight
  tick.
- **PK hint** — today's `nx-gs-db-sync-core` trigger **ignores** the `pks`
  argument and runs a full-entity cycle. Pass the PKs anyway; a future
  implementation will use them as targeted spot-scans.

### Flow

```
ctx.sync().requestNow("character", charId)
        │
        ▼
NxSyncImpl  (session-scoped façade in adapter-core)
        │ lookup trigger registered by DbSyncModule.start
        ▼
trigger lambda: pks -> CdcEngine.triggerEntityNow("character")
        │
        ▼
ScheduledThreadPoolExecutor (nx-cdc-pool-<schema>-N)
        │ pool.execute(runGuardedCycle)
        │ per-entity `ticking` AtomicBoolean: skip if already running
        ▼
EntitySyncTask.runCycle()
        │ Phase 1: CRC32 hash rows, diff vs in-memory snapshot
        │ Phase 2: fetch changed rows, build DTO
        ▼
SyncEventPublisher → Kafka topic
        <tenant>.gs.sync.db.<entity>
        (platform consumer observes the new state)
```

### Registering a trigger (module author)

If your module owns sync for an entity (e.g. a custom CDC scheme),
register a trigger in `onConnect`:

```java
@Override
public void onConnect(ConnectContext ctx) {
    ctx.sync().registerTrigger("inventory", pks -> myEngine.refreshNow(pks));
}
```

`registerTrigger` is module-side only; host handlers MUST NOT call it.
Last-write-wins on duplicate entity names. Triggers are cleared on
disconnect; modules re-register on the next handshake.

## CommandResult — what to return

### Success cases

Every command has a `{X}Result` class; populate it with confirmation data:

```java
return CommandResult.ok(new DeleteItemResult(itemObjectId, deletedCount, fullyDeleted));
return CommandResult.ok(new TransferCharToAccountResult(charId, newAccount, wasLoggedOut));
return CommandResult.ok(new SendMailResult(createdMailIds, itemErrors));
```

The payload is anything Gson can serialize — POJOs, primitives, lists,
maps. Records are NOT supported in `nx-gs-adapter-api` itself (Java 8
target); host-side `R` types may be records when the host JVM is Java 14+.

### Error cases — sugar factories

```java
// Common 1-liner patterns:
return CommandResult.notFound("Character not found");
return CommandResult.notFound("Item not found", "itemObjectId", 12345L);
return CommandResult.invalidState("Capacity exceeded");
return CommandResult.forbidden("Cannot punish self");
return CommandResult.validationFailed("count must be positive", "count");
return CommandResult.rateLimited("Too many link attempts for this user");
return CommandResult.unavailable("Database unreachable");
return CommandResult.internalError("Unexpected handler state");
```

Each factory builds a `CommandProblem` with the title (+ optional single
extension). For richer problem bodies use the explicit factory:

```java
return CommandResult.error(CommandStatus.VALIDATION_FAILED,
        CommandProblem.builder()
                .title("Mail attachment validation failed")
                .detail("3 of 5 items rejected by template catalog")
                .extension("rejectedItemIds", List.of(12345L, 67890L, 11111L))
                .extension("first.reason", "unknown template id")
                .build());
```

### Picking the right `CommandStatus`

| Status                | Tier         | Meaning                                 | Example                                                    |
|-----------------------|--------------|-----------------------------------------|------------------------------------------------------------|
| `OK`                  | OK           | Handler executed; payload non-null      | Transfer succeeded; mail created                           |
| `NOT_FOUND`           | CLIENT_ERROR | Subject doesn't exist                   | `getPlayer(charId)` returned null                          |
| `INVALID_STATE`       | CLIENT_ERROR | Subject exists but cannot accept op now | Player in jail; capacity exceeded                          |
| `FORBIDDEN`           | CLIENT_ERROR | Operation rejected by policy            | Cannot punish self; cannot kick admin                      |
| `VALIDATION_FAILED`   | CLIENT_ERROR | Request payload malformed               | Null wire field; negative quantity                         |
| `RATE_LIMITED`        | CLIENT_ERROR | Too many requests (HTTP 429 convention) | Handler self-limits to 1/sec/account                       |
| `UNSUPPORTED_COMMAND` | CLIENT_ERROR | Adapter-emitted: no registered handler  | Deploy skew between platform & core                        |
| `UNAVAILABLE`         | SERVER_ERROR | Dependency broken; retry later may work | DB unreachable; host-executor timeout                      |
| `INTERNAL_ERROR`      | SERVER_ERROR | Unexpected error                        | Caught explicitly, or auto-wrapped from `RuntimeException` |

### Just throw

Handlers MAY throw `RuntimeException` and the adapter catches it,
auto-wrapping as `INTERNAL_ERROR` with the exception class+message in
the problem's extensions. Use this for genuinely unexpected failures;
for known-bad inputs prefer explicit error returns.

`Throwable` (OOM, StackOverflow) is intentionally NOT caught — the
consumer thread aborts and the JVM-level handler takes over. Reply is
not emitted; caller sees a timeout.

## Registering handlers

```java
public final class BohptsCommandsModule implements AdapterModule {

    @Override
    public String name() { return "bohpts-commands"; }

    @Override
    public void onConnect(ConnectContext ctx) {
        NxCommands commands = ctx.commands();
        commands.on(DeleteItemCommand.class, new DeleteItemHandler());
        commands.on(TransferItemToCharacterCommand.class, new TransferItemToCharacterHandler());
        commands.on(TransferCharToAccountCommand.class, new TransferCharToAccountHandler());
        commands.on(SendMailCommand.class, new SendMailHandler());
        commands.on(TelegramCharLinkCommand.class, new TelegramCharLinkHandler());
    }

    @Override public void start() {}
    @Override public void stop() {}
    @Override public void onDisconnect() {}
}
```

The handler `Class` is matched against the inbound `Nx-Message-Type`
header by **simple class name**. So
`app.l2nx.gs.adapter.api.kafka.commands.character.TransferCharToAccountCommand`
matches header value `TransferCharToAccountCommand`. Two distinct classes with
the same simple name would collide — keep simple names unique within
the catalog.

Late registration (after `start()` returns and the consumer is polling)
is allowed. Records arriving before registration → `UNSUPPORTED_COMMAND`
reply.

## `HostExecutor` — what to register

You register the host's game-side `Executor` once at bootstrap, BEFORE
`NxAdapter.start()`:

```java
NxAdapter.hostExecutor(task -> ThreadPoolManager.getInstance().executeGeneral(task));
```

If you forget: `ctx.host().sync(...)` throws `IllegalStateException` the
first time a handler tries to hop. The adapter logs a WARN at startup
when `commandsTopic` is configured but `hostExecutor` is missing.

## Replies — how the platform sees them

The adapter publishes replies to `<tenant>.gs.commands.replies` with:

- **Key** = big-endian bytes of `correlationId.getMostSignificantBits()`
  (so replies for the same correlation co-locate on a partition; useful
  for tooling)
- **Headers**:
    - `Nx-Server-Id` (auto, from connect handshake)
    - `Nx-Correlation-Id` (echoed from inbound)
    - `Nx-Message-Type = "<OriginalCommandClass>Result"` (e.g.
      `"TransferItemToCharacterCommandResult"`)
- **Value** = `gson.toJson(commandResult)`

The platform-side correlator listens on the replies topic, extracts
`Nx-Correlation-Id` from headers, matches against pending requests,
returns the result to the original web caller. Web doesn't have to
consume the body if it doesn't care — `null` body is a valid
`CommandResult` (`success=true, payload=null`).

## Heartbeat & observability

Every heartbeat tick the adapter ships a per-module health slot. The
commands runtime exposes:

```json
{
  "name": "commands",
  "state": "ACTIVE",
  "stats": {
    "commands": {
      "consumed-total":         1234,
      "handled-total":          1230,
      "unsupported-total":      0,
      "validation-failed-total": 1,
      "internal-errors-total":  3,
      "replies-published-total": 1230,
      "replies-failed-total":   0,
      "registered-types":       ["DeleteItemCommand", "TransferItemToCharacterCommand",
                                 "TransferCharToAccountCommand", "SendMailCommand",
                                 "TelegramCharLinkCommand"],
      "commit-failures-total":  0
    }
  }
}
```

What to alert on:

- `internal-errors-total / consumed-total > 5%` for >5 minutes — handlers
  bugged (or new poison input).
- `unsupported-total > 0` — web sent a command for which no handler is
  registered. Either web is ahead of core, or core forgot to register.
- `replies-failed-total / replies-published-total > 1%` — Kafka publisher
  unhappy; check the `events` slot for queue overflow / failed-total.
- `commit-failures-total > 0` — Kafka offset commit errors. In
  at-most-once mode this means batches are being **dropped** without
  dispatch; callers see timeouts and operators have to re-issue. Indicates
  a broker / ACL problem worth paging.

## Common mistakes

**Mutating game state without hopping**

```java
// WRONG — race against game thread
Player p = GameObjectsStorage.getPlayer(...);
p.setExp(p.getExp() + 1000);

// RIGHT
ctx.host().sync(() -> {
    Player p = GameObjectsStorage.getPlayer(...);
    p.setExp(p.getExp() + 1000);
});
```

**Running JDBC on the game pool**

```java
// WRONG — burns game thread capacity for blocking IO
ctx.host().sync(() -> {
    try (Connection con = DatabaseFactory.getInstance().getConnection()) {
        // ... blocking JDBC ...
    }
});

// RIGHT — hop to ctx.io() for pure JDBC
CompletableFuture.supplyAsync(() -> {
    try (Connection con = DatabaseFactory.getInstance().getConnection()) {
        // ... blocking JDBC ...
    }
}, ctx.io()).join();
```

**Returning `null`**

```java
// WRONG — adapter wraps as INTERNAL_ERROR
return null;

// RIGHT
return CommandResult.ok(new MyCommandResult(...));
return CommandResult.notFound("...");
```

**Forgetting to register**

```
Symptom: web sees UNSUPPORTED_COMMAND replies for KickCommand
Cause:   ctx.commands().on(KickCommand.class, ...) was never called
```

**Slow handler that doesn't hop**

```java
// WRONG — blocks the entire commands topic for 5s
static CommandResult<SlowResult> handleSlowOp(SlowCommand cmd, CommandContext ctx) {
    Thread.sleep(5_000);
    return CommandResult.ok(new SlowResult(...))
}
```

Long-running ops should use the host's scheduler with a delay, not block
inline.

**Trying to be idempotent**

```java
// UNNECESSARY in at-most-once mode
if (alreadyProcessedCache.contains(ctx.correlationId())) {
    return CommandResult.ok(...);  // dedup not needed
}
alreadyProcessedCache.add(ctx.correlationId());
```

Drop the dedup cache. At-most-once means every invocation is a fresh
attempt; double-execution is impossible (operator re-issue produces a
fresh `correlationId`).

## Glossary

- **`NxCommand<R>`** — type-parameterized marker every command DTO
  implements; `R` is the dedicated `{X}Result` class
- **`{X}Result`** — per-command reply payload (`TransferItemToCharacterResult`,
  `DeleteItemResult`, …); always carries confirmation data even for
  "void-success" commands
- **`CommandHandler<C, R>`** — SAM `(cmd, ctx) -> CommandResult<R>`
- **`CommandContext`** — per-invocation context with `correlationId()`,
  `host()`, `events()`, `io()`, `sync()`
- **`HostExecutor`** — wrapper around the host's game-side `Executor`
- **`NxCommands`** — registration SPI; `ctx.commands().on(Class, handler)`
- **`NxSync`** — out-of-band sync trigger SPI;
  `ctx.sync().requestNow(entity, pk)`
- **`CommandResult<R>`** — reply envelope: `status` + `payload?` + `problem?`
- **`CommandStatus`** — enum of outcomes (`OK`, `NOT_FOUND`,
  `INVALID_STATE`, …) with nested `Tier` (`OK / CLIENT_ERROR / SERVER_ERROR`)
- **`CommandProblem`** — RFC 9457-compatible problem body: `title`,
  `detail?`, `extensions` map
- **`Nx-Message-Type`** — Kafka header carrying the simple class name
  (routing key)
- **`Nx-Correlation-Id`** — Kafka header carrying the platform-issued
  UUID (used by web to match reply to request)

## See also

- [`catalog.md`](./catalog.md) — per-command business contract: inputs,
  result fields, error statuses, side effects (sync triggers)
- [`spec.md`](./spec.md) — formal requirements + decisions
- [`docs/specs/008-messaging/spec.md`](../008-messaging/spec.md) — sibling
  outbound-events feature
- `app.l2nx.gs.adapter.api.kafka.commands.NxCommand` — Javadoc on the
  marker
- `app.l2nx.gs.adapter.api.kafka.commands.CommandStatus` — outcome enum
  with `Tier` classification
- `app.l2nx.gs.adapter.api.kafka.commands.CommandProblem` — RFC 9457
  problem body
- `app.l2nx.gs.adapter.api.spi.NxCommands` — Javadoc on the registration
  SPI
- `app.l2nx.gs.adapter.api.spi.NxSync` — Javadoc on the sync trigger SPI
