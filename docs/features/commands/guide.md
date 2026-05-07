# Commands — Handler Author's Guide

> Companion to [`spec.md`](./spec.md). This document is for developers writing
> command handlers in `bohpts-core` (or any other host that embeds
> `nx-gs-adapter-core`). Spec is the formal source of truth for the wire shape;
> this guide is the friendly walkthrough.

## What is a command?

A command is a JSON message the platform's web side sends to the game-server core
asking it to **do something** — kick a player, send mail, transfer items, ban an
account. Each command:

- Implements the marker interface `app.l2nx.gs.adapter.api.kafka.commands.NxCommand`
- Travels on a single Kafka topic `<tenant>.gs.commands` (partitioned by character id)
- Carries two headers: `Nx-Message-Type` (the simple class name) and
  `Nx-Correlation-Id` (UUID issued by web side)
- Is dispatched by the adapter to a registered `CommandHandler`
- Produces a `CommandResult` reply on `<tenant>.gs.commands.replies`

The command flow:

```
web ──[command + Nx-Correlation-Id]──► commandsTopic ──► adapter consumer
                                                              ▼
                                                     handler.handle(cmd, ctx)
                                                              ▼
                                                       CommandResult<R>
                                                              ▼
web ◄──[result + Nx-Correlation-Id]──── repliesTopic ◄── adapter publisher
```

## Your first handler — the 30-second tour

```java
import app.l2nx.gs.adapter.api.kafka.commands.CommandResult;
import app.l2nx.gs.adapter.api.kafka.commands.ErrorCode;
import app.l2nx.gs.adapter.api.spi.CommandContext;
import app.l2nx.gs.adapter.core.NxAdapter;

public final class BohptsCommandHandlers {

    public static void register(NxCommands commands) {
        commands.on(KickCommand.class, BohptsCommandHandlers::handleKick);
    }

    static CommandResult<Void> handleKick(KickCommand cmd, CommandContext ctx) {
        ctx.host().sync(() -> {
            Player p = GameObjectsStorage.getPlayer(cmd.getCharId().intValue());
            if (p != null) p.kick();
        });
        return CommandResult.success();
    }
}

// During bohpts startup, BEFORE NxAdapter.start():
NxAdapter.

hostExecutor(task ->
        ThreadPoolManager.

getInstance().

executeGeneral(task));
        NxAdapter.

start();

// During the adapter's onConnect callback (typically inside an AdapterModule
// or a host-supplied connect hook):
ctx.

commands().

on(KickCommand .class, BohptsCommandHandlers::handleKick);
```

That's the whole story. The rest of this guide is the "why" and edge cases.

## Threading model — what runs where

The adapter's commands consumer is a **single daemon thread**
(`nx-commands-consumer`). When a record arrives, it:

1. Resolves the handler from the type registry
2. Deserializes the JSON via Gson
3. Calls `handler.handle(cmd, ctx)` synchronously on the consumer thread
4. Enqueues the reply into the events publisher's queue
5. Commits the Kafka offset

The handler is sync. Whatever you do in `handle(...)` blocks the consumer thread
for the duration. Slow handlers slow the topic. **This is by design** — slow
handler on charA does not block fast handler on charB because charA and charB
hash to different partitions (and each partition gets its own consumer thread
in a future scaling step).

### When to hop to the host executor

The Kafka consumer thread is NOT the host's game thread. Mutating game state
from the consumer thread races against the host's own loops (packet handlers,
AI ticks, scheduled tasks).

Three rules:

- **Read-only operations** — `Player.getName()`, `GameObjectsStorage.getPlayer(...)`
  for reads, querying caches that are safe to read from any thread — DO NOT hop.
  Read on the consumer thread, return the result.
- **State mutations** — `Player.kick()`, `Player.setNoblesse(true)`,
  `MailManager.sendMail(...)` — MUST hop via `ctx.host().sync(() -> {...})`.
- **DB writes / external calls** — these are typically synchronous-blocking
  anyway (host JDBC). Run them on the consumer thread; don't burn host-executor
  capacity. Example: `PunishmentDAO.insert(record)` — fine on consumer thread.

```java
static CommandResult<Void> handleSendMail(SendMailCommand cmd, CommandContext ctx) {
    // read on consumer thread — fine
    Player p = PlayerCache.getInstance().findById(cmd.getCharId());
    if (p == null) {
        return CommandResult.error(ErrorCode.NOT_FOUND,
                "charId", String.valueOf(cmd.getCharId()));
    }

    // db write on consumer thread — fine, JDBC blocks anyway
    long messageId = MailDAO.insert(toRecord(cmd, p));

    // game state mutation — MUST hop
    ctx.host().sync(() -> {
        MailManager.getInstance().notifyDelivery(p, messageId);
    });

    return CommandResult.success();
}
```

### Sync vs Async on the host executor

`ctx.host().sync(...)` blocks the consumer thread until the host executor
finishes the task. Use it when:

- You need the result of the host-side operation
- You want to fail-fast: if the host call throws, you want the exception
  to propagate so you can return `CommandResult.error(...)`
- You want strict ordering: "first hop completes before second hop starts"

`ctx.host().async(...)` does NOT block the consumer thread. Use it for:

- Fire-and-forget side effects (broadcast a message, schedule a delayed
  follow-up, log to host-side audit)
- Operations whose failure the platform doesn't need to know about
- Operations that can run after the reply is sent (e.g. "log to audit table"
  shouldn't delay the reply to web)

Rule of thumb: **default to `sync`**. Only reach for `async` when you have a
specific reason.

```java
ctx.host().

async(() ->{
        AuditLog.

recordCommand(cmd, "kick","by-admin-X");
});
        return CommandResult.

success();   // reply goes out without waiting for audit
```

## CommandResult — what to return

### Success cases

```java
return CommandResult.success();                 // void payload
return CommandResult.

success(payload);          // typed payload
```

The payload is anything Gson can serialize — POJOs, primitives, lists, maps.
Records (Java 14+) are NOT supported (the host JVM is Java 8).

### Error cases

```java
// single detail
return CommandResult.error(ErrorCode.NOT_FOUND);
return CommandResult.

error(ErrorCode.NOT_FOUND, "charId","12345");

// multi detail (fluent)
return CommandResult .

<Void> builder()
        .

errorCode(ErrorCode.VALIDATION_FAILED)
        .

errorDetail("field","amount")
        .

errorDetail("reason","negative")
        .

errorDetail("got",String.valueOf(cmd.getAmount()))
        .

build();
```

### Picking the right ErrorCode

| Code                  | Meaning                                      | Example                                                             |
|-----------------------|----------------------------------------------|---------------------------------------------------------------------|
| `NOT_FOUND`           | Subject doesn't exist (char, account, item)  | `getPlayer(charId)` returned null                                   |
| `INVALID_STATE`       | Subject exists but cannot accept this op now | Player is in jail; cannot transfer item                             |
| `FORBIDDEN`           | Operation not allowed regardless of state    | Cannot punish self; cannot kick admin                               |
| `RATE_LIMITED`        | Too many requests; try later                 | Handler self-limits to 1/sec/account                                |
| `UNAVAILABLE`         | Dependency broken; retry later might work    | DB unreachable; queue full                                          |
| `VALIDATION_FAILED`   | Request payload itself is malformed          | Negative quantity, missing required field                           |
| `INTERNAL_ERROR`      | Unexpected (and you'd rather not say more)   | Caught by handler explicitly, or auto-wrapped from RuntimeException |
| `UNSUPPORTED_COMMAND` | (adapter-emitted, not for handlers)          | Type registry has no binding for the inbound message type           |

### Just throw

Handlers MAY throw `RuntimeException` and the adapter will catch it,
auto-wrap as `INTERNAL_ERROR`, and reply with the exception class+message in
`errorDetails`. Use this for genuinely unexpected failures; for known-bad
inputs prefer explicit `error(...)` returns.

## Registering handlers

Handlers register via `ctx.commands().on(Class, handler)`. The registration
window opens at the host's `onConnect(ctx)` callback. The simplest pattern
in bohpts-core:

```java
public final class BohptsAdapterModule implements AdapterModule {
    @Override
    public void onConnect(ConnectContext ctx) {
        BohptsCommandHandlers.register(ctx.commands());
    }

    @Override
    public ModuleStatus currentStatus() { ...}

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }
}
```

(Or use the dedicated bohpts connect-hook plumbing — depends on bohpts internals.)

The handler `Class` is matched against the inbound `Nx-Message-Type` header
by **simple class name**. So `app.l2nx.gs.adapter.api.kafka.commands.character.KickCommand`
matches header value `KickCommand`. Two distinct classes with the same simple
name would collide — keep simple names unique within the catalog.

Late registration (after `start()` returns and the consumer is polling) is
allowed. Records arriving before registration → `UNSUPPORTED_COMMAND` reply.

## HostExecutor — what to register

The handler's `ctx.host().sync(...)` / `.async(...)` calls dispatch to a
host-supplied `java.util.concurrent.Executor`. You register it once at
bootstrap, BEFORE `NxAdapter.start()`:

```java
NxAdapter.hostExecutor(task ->
        ThreadPoolManager.

getInstance().

executeGeneral(task));
```

Bohpts already has a general thread pool. Wrap its execute method as above.

If you forget: `ctx.host().sync(...)` will throw `IllegalStateException` the
first time a handler tries to hop. The adapter logs a WARN at startup when
`commandsTopic` is configured but `hostExecutor` is missing — so an empty
config of "we ship commands but you forgot to register" gets surfaced loudly.

## Idempotency

Kafka offers at-least-once delivery. The adapter commits offsets per batch
AFTER all records in the batch finish; if the JVM crashes mid-batch, redelivery
on next start replays the partial batch.

**Your handler must be idempotent.** Specifically: if `handler.handle(cmd, ctx)`
runs twice with the same `correlationId`, the observable game state must be the
same as if it ran once.

Strategies:

- **Naturally idempotent** — `KickCommand`, `RestartPointCommand`, `SetCharVar`
  are idempotent by their semantic. Easy.
- **Existence check + insert** — `SendMailCommand` should check whether a mail
  with this correlationId was already sent (e.g. a `correlation_id` column on
  the mail table) before inserting.
- **Generation tokens** — `TransferItemCommand` could write the item's
  `last_correlation_id` and short-circuit if equal to the inbound id.

The adapter does not maintain a built-in dedup cache. The platform also does not.
The host owns idempotency.

## Handler registration patterns

**Single-class organizing by domain:**

```java
// BohptsCharacterHandlers.java — one class per domain
public static void register(NxCommands commands) {
    commands.on(KickCommand.class, BohptsCharacterHandlers::handleKick);
    commands.on(SetCharVarCommand.class, BohptsCharacterHandlers::handleSetVar);
    // ...
}
```

**Methods or instance methods, equivalent:**

```java
// static method reference
commands.on(KickCommand .class, BohptsCharacterHandlers::handleKick);

// instance method on a stateful service
KickService kickService = new KickService(...);
        commands.

on(KickCommand .class, kickService::handle);

// inline lambda
commands.

on(KickCommand .class, (cmd, ctx) ->{...return...;});
```

Whichever is most idiomatic for the bohpts codebase. The SPI doesn't care.

## Replies — how the platform sees them

The adapter publishes replies to `<tenant>.gs.commands.replies` with:

- Key = first 8 bytes of `correlationId.getMostSignificantBits()` (so replies
  for the same correlation hash co-locate on a partition; useful for tooling)
- Headers:
    - `Nx-Server-Id` (auto, from connect)
    - `Nx-Correlation-Id` (echoed from inbound)
    - `Nx-Message-Type = "<OriginalCommandClass>Result"` (e.g. `"KickCommandResult"`)
- Body = `gson.toJson(commandResult)`

The platform-side correlator listens on the replies topic, extracts
`Nx-Correlation-Id` from headers, matches against pending requests, returns the
result to the original web caller. Web doesn't have to consume the body if it
doesn't care — `null` body is a valid `CommandResult` (`success=true, payload=null`).

## Heartbeat & observability

Every 60 seconds the adapter ships a `HeartbeatEvent` with a per-module health
slot. The commands runtime exposes:

```json
{
  "name": "commands",
  "state": "ACTIVE",
  "stats": {
    "commands": {
      "consumed-total": 1234,
      "handled-total": 1230,
      "unsupported-total": 0,
      "validation-failed-total": 1,
      "internal-errors-total": 3,
      "replies-published-total": 1230,
      "replies-failed-total": 0,
      "registered-types": [
        "KickCommand",
        "SendMailCommand",
        ...
      ],
      "commit-failures-total": 0
    }
  }
}
```

What to alert on:

- `internal-errors-total / consumed-total > 5%` for >5 minutes — handlers
  bugged (or new poison input)
- `unsupported-total > 0` — web sent a command for which no handler is
  registered. Either web is ahead of core (new command not yet deployed) or
  core forgot to register.
- `replies-failed-total / replies-published-total > 1%` — Kafka publisher
  unhappy; check `events` slot for queue overflow / failed-total
- `commit-failures-total > 0` — Kafka commit errors; could indicate broker
  unhealth or rebalance storm. Records will be redelivered.

## Common mistakes

**Mutating game state without hopping**

```java
// WRONG — race against game thread
Player p = GameObjectsStorage.getPlayer(...);
        p.

setExp(p.getExp() +1000);    // mutating from consumer thread

// RIGHT
        ctx.

host().

sync(() ->{
Player p = GameObjectsStorage.getPlayer(...);
        p.

setExp(p.getExp() +1000);
        });
```

**Returning null**

```java
// WRONG — adapter wraps as INTERNAL_ERROR
return null;

// RIGHT
        return CommandResult.success();        // for void
return CommandResult .

<Void> error(ErrorCode.NOT_FOUND);
```

**Forgetting to register**

```java
// Symptom: web sees UNSUPPORTED_COMMAND replies for KickCommand
// Cause: ctx.commands().on(KickCommand.class, ...) was never called
```

**Slow handler that doesn't hop**

```java
// WRONG — blocks consumer for 5s, blocks the entire topic for 5s
static CommandResult<Void> handleSlowOp(SlowCommand cmd, CommandContext ctx) {
    Thread.sleep(5_000);    // blocks the consumer thread!
    return CommandResult.success();
}

// BETTER — hop and let the host's pool absorb the delay
ctx.

host().

async(() ->{
        Thread.

sleep(5_000);
});
        return CommandResult.

success();
```

(Note: even the "better" version is suspicious — sleeping 5s on the host
executor pool burns host capacity. Real long-running ops should use the host's
scheduler with a delay, not block.)

**Throwing sneaky `Throwable`**

```java
// adapter catches RuntimeException → INTERNAL_ERROR reply
// adapter does NOT catch Throwable (OOM, StackOverflow) → rethrows
// → consumer thread exits → no offset commit → record redelivered → loop

// If your handler can throw Error, fix it. The adapter intentionally lets
// JVM-fatal errors propagate so the JVM-level handler can act.
```

## Glossary

- **`NxCommand`** — marker interface every command DTO implements
- **`CommandHandler<C, R>`** — SAM `(cmd, ctx) -> CommandResult<R>`
- **`CommandContext`** — per-invocation context (correlationId, host(), events())
- **`HostExecutor`** — wrapper around the host's game-side `Executor`
- **`NxCommands`** — registration SPI; `ctx.commands().on(Class, handler)`
- **`CommandResult<R>`** — reply envelope (success, errorCode, errorDetails, payload)
- **`ErrorCode`** — enum of standardized error categories
- **`Nx-Message-Type`** — Kafka header carrying the simple class name (routing key)
- **`Nx-Correlation-Id`** — Kafka header carrying the platform-issued UUID
  (used by web to match reply to request)

## See also

- [`spec.md`](./spec.md) — formal requirements + decisions
- [`docs/features/messaging/spec.md`](../messaging/spec.md) — sibling outbound-events
  feature; same publisher infrastructure under the hood
- `app.l2nx.gs.adapter.api.kafka.commands.NxCommand` — Javadoc on the marker
- `app.l2nx.gs.adapter.api.spi.NxCommands` — Javadoc on the SPI
