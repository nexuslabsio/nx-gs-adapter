# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Workflow

- Always confirm before creating or modifying files — never write code without user approval
- Keep `CLAUDE.md`, `README.md`, and `docs/` in sync with code changes
- Do not run `compile`/`build` after every change — only when explicitly asked or all changes are complete

## Project

`nx-gs-adapter` is the multimodule Gradle project hosting the runtime modules of the L2NX game-server
adapter. The adapter embeds into the JVM of a game-server core (L2J / Lucera / Essence and forks),
connects to the L2NX platform via outbound HTTPS, and synchronizes data via Kafka.

Architecture is documented per-feature under `docs/features/<feature-name>/spec.md` +
`tech.md` as features land.

## Modules

- `:nx-gs-adapter-api` — wire contracts (REST + Kafka DTOs) and SPI types (Tier-1
  `AdapterModule`, Tier-2 `DbSchemaProvider` / `RuntimeStateProvider`, Tier-3
  `JdbcConnectionSource`) shared with the platform and module authors. Java 8 POJOs,
  zero runtime deps, package root `app.l2nx.gs.adapter.api`. Includes
  `kafka.NxHeaders` — the wire-level Kafka header contract (`NX_SERVER_ID` raw-16-byte
  UUID stamped on every record post-`/connect`, `NX_MESSAGE_TYPE` simple class name
  for polymorphic dispatch on outbound events / inbound commands / replies,
  `NX_CORRELATION_ID` carrying the platform-issued UUID on inbound commands and
  echoed onto reply records). Hosts the per-family event DTOs under
  `kafka.events.<family>` — Phase 1 families:
  `events.premiumpurchase.PremiumPurchaseEvent` (single-event family;
  multi-line items + services + per-line multi-currency `Payment`s, plus
  `WellKnownServices` constants);
  `events.serveronline.ServerOnlineSnapshotEvent` (single-event family;
  periodic population breakdown carrying UUIDv7 `eventId` + open
  `Map<String, Long> buckets` keyed by `WellKnownServerOnlineBuckets`
  lower_snake_case constants — required `total` / `unique`, optional
  canonical `offline_trade` / `fishing`; hosts MAY publish arbitrary
  non-canonical keys; cadence host-managed);
  `events.privatestore` (multi-event family: `PrivateStorePurchaseEvent`
  for closed deals + `PrivateStoreSnapshotEvent` for per-`(itemId, side)`
  order book, with `TradeLine` / `Offer` line types, `PrivateStoreSide`
  enum, and `WellKnownElements` constants — no abstract base; one publish
  method per concrete subtype on `NxEvents`). The inbound-commands marker
  `kafka.commands.NxCommand<R>` + reply envelope `kafka.commands.CommandResult<R>`
  with structured `kafka.commands.ErrorCode` enum (`NOT_FOUND` / `INVALID_STATE` /
  `FORBIDDEN` / `RATE_LIMITED` / `UNAVAILABLE` / `VALIDATION_FAILED` /
  `INTERNAL_ERROR` / `UNSUPPORTED_COMMAND`). Concrete command DTOs ship under
  `kafka.commands.<group>.*` (group = code-org bucket: `character` / `item` /
  `mail` / `account`); shipped today: `commands.item.DeleteItemCommand`
  (`NxCommand<Void>`) and `commands.mail.SendMailCommand`
  (`NxCommand<SendMailPayload>` carrying `Long charId` + `String author?` +
  `String title` + `String body?` + `List<MailItem>` attachments;
  `SendMailPayload` carries `List<Long> createdMailIds` + `List<ItemDeliveryError>`
  partial-failure entries on the success envelope). Capability SPIs live in `spi.*`: `NxEvents` (events fanout,
  acquired via `ConnectContext.events()`), `NxCommands` (handler registration,
  acquired via `ConnectContext.commands()`), `CommandHandler<C, R>` SAM,
  `CommandContext` (per-invocation correlationId / host() / events()),
  `HostExecutor` (game-thread hop helper with `sync(Runnable)` / `<T> sync(Supplier<T>)`
  / `async(Runnable)`). Both contexts also surface an adapter-owned IO `Executor`
  via `ConnectContext.io()` (module-level) and `CommandContext.io()` (handler-level)
  for blocking IO (JDBC, HTTP) — handlers MUST hop here instead of running JDBC on
  the game thread or the Kafka consumer thread.
- `:nx-gs-commons` — shared utilities for adapter modules and tenant providers:
  `concurrent.SafeRunnable` (exception-swallowing Runnable wrapper), `hash.Fnv1a64`
  (FNV-1a 64-bit hash), `Nulls` (sentinel-to-null), `jdbc.JdbcNulls` (null-aware
  `ResultSet` readers), `UUIDv7` (RFC 9562 time-ordered ids — pure JDK, monotonic
  per-JVM, used as `eventId` on outbound events so platform consumers extract
  `occurredAt` from the id alone). Java 8, deps: `jspecify` only. Package root
  `app.l2nx.gs.commons`. `:nx-gs-log` shadow-included. Published to Maven Central.
- `:nx-gs-kafka` — lightweight Kafka client facade. Java 8, depends on `kafka-clients` + `gson`,
  `slf4j-api` compileOnly. Package root `app.l2nx.gs.kafka`. `:nx-gs-log` shadow-included.
  Producer supports connection-scoped static headers (`KafkaConfig.Builder.producerStaticHeader` /
  `NxProducer.create(props, gson, headers)`) — adapter-core stamps `Nx-Server-Id`
  (raw 16-byte UUID) on every record post-`/connect`. Per-record headers (e.g.
  `Nx-Message-Type`) attach via `NxKafka.sendBytesKeyRecord(record, callback)`
  — used by adapter-core's events publisher for outbound family dispatch.
- `:nx-gs-adapter-core` — runtime: config resolution, POST `/connect`, heartbeat, ServiceLoader-based
  module discovery, lifecycle. Hosts the built-in `NxEvents` capability — bounded-queue
    + daemon-thread fan-out (`events.EventsPublisher`, `events.NxEventsImpl`,
      `events.EventTypeRegistry`) reading per-family topic addressing from
      `ConnectResponse.messagingTopics.events`. Default `drop-policy` is `newest`
      (drop incoming on overflow — preserves queue order). `oldest` (evict head)
      remains an option but over-counts `dropped-total` under multi-producer
      contention because the displaced envelope is counted on the eviction path
      even when concurrent enqueuers race for the same slot. Heartbeat surfaces an
      `events` module slot (`queue-depth`, `published-total`, `dropped-total`,
      `failed-total`, `disabled-families`) via `ModuleStatus.Stats.events`. The
      `NxEvents` façade handed out via `ConnectContext.events()` survives reconnect
      — an `AtomicReference` inside the façade is swapped to the live publisher on
      every reconnect cycle, so host modules cache the reference once at `start()`
      and never re-acquire. Also hosts the built-in `NxCommands` capability —
      single Kafka consumer + dispatch table (`commands.CommandsConsumer`,
      `commands.NxCommandsImpl`, `commands.CommandTypeRegistry`) reading inbound
      topic from `MessagingTopics.commandsTopic` and publishing replies to
      `MessagingTopics.commandsRepliesTopic` via the existing producer. Manual
      offset commit per batch; handler `RuntimeException` auto-wraps as
      `INTERNAL_ERROR` reply. The `NxCommands` façade follows the same
      survive-reconnect pattern (AtomicReference swap on reconnect). Heartbeat
      surfaces a `commands` module slot (`consumed-total` / `handled-total` /
      `unsupported-total` / `validation-failed-total` / `internal-errors-total` /
      `replies-published-total` / `replies-failed-total` / `commit-failures-total`
      / `registered-types`) via `ModuleStatus.Stats.commands`. Host registers its
      game-thread `Executor` via static `NxAdapter.hostExecutor(Executor)` BEFORE
      `start()`; the adapter wraps it as `HostExecutor` for handler-side
      `ctx.host().sync(...)` / `.async(...)` hops. The adapter also owns a shared
      IO pool (`nx-io-N` daemon threads, sized by `l2nx.io.workers`, default
      `max(2, cores/2)`) surfaced via `ConnectContext.io()` and
      `CommandContext.io()` — handlers doing JDBC/HTTP MUST hop here. Connect-flow
      retry uses ±25% jitter on the configured interval to avoid thundering-herd
      reconnect on platform recovery. Heartbeat start/stop is `synchronized` so
      reconnect concurrent with shutdown cannot leave a scheduler running. Engine
      configs under `l2nx.events.*` (queue-capacity / drop-policy /
      shutdown-drain-timeout-ms) and `l2nx.commands.*` (poll-timeout-ms /
      shutdown-timeout-ms / kafka.<prop>); both file-first source chain. Depends
      on `:nx-gs-adapter-api` + `:nx-gs-kafka` + `:nx-gs-commons` + `gson`.
      Package root `app.l2nx.gs.adapter.core`. `:nx-gs-log` shadow-included.
- `:nx-gs-db-sync-core` — DB-sync `AdapterModule` shipped to Maven Central. Owns the
  CRC32 two-phase CDC engine (shared bounded pool — `l2nx.cdc-engine.workers` daemon
  threads, default `max(2, min(entities, cores/2))` — replaces the legacy
  thread-per-entity model; entities are scheduled as tasks onto the pool, server-side
  CRC32 hashing, per-row snapshot swap on Kafka ack) and resolves the Tier-2
  `DbSchemaProvider` SPI (defined in `nx-gs-adapter-api` so client providers depend
  only on the api artifact). Reads its per-entity Kafka topics from
  `ctx.syncTopics().db()` (db namespace of the namespaced `SyncTopics` bundle).
  Surfaces both `pool` (from `JdbcConnectionSource.stats()`) and `entities`
  (per-entity `EntityStats`) slots in the heartbeat. Engine config lives under
  `l2nx.cdc-engine.*` (file-first source chain) — including `workers` and
  `fetch-size` (default `10_000`). JDBC dialect is auto-detected from the
  connection URL: MySQL Connector/J (`jdbc:mysql:`) → `Integer.MIN_VALUE`
  streaming fetch (row-by-row, the only mode it honors for large result
  sets); MariaDB Connector/J (`jdbc:mariadb:`) → positive `fetch-size` hint
  (3.x rejects negative fetchSize; add `useCursorFetch=true` to the URL for
  true server-side cursors); Postgres / others → `fetch-size` as a cursor-batch hint. All identifiers passed via
  `EntityMapping` /
  `PrimarySource` / `ChildSource` (tableName / pkColumn / fkColumn / hashedColumns)
  MUST match `^[A-Za-z_][A-Za-z0-9_]{0,63}$` — schema-qualified or quoted names are
  rejected at engine start — and the same regex now also guards
  `mapping.entityName()` and `provider.schemaName()` since both are
  interpolated into the on-disk snapshot file path. **Snapshot persistence**
  (sub-package `app.l2nx.gs.db.sync.engine.persist`): always on,
  per-entity binary file `<persist.dir>/<schemaName>/<entityName>.snap`
  (`nx-cdc-snapshot` default dir relative to JVM cwd) written
  tmp → fsync → atomic-rename on every successful cycle (per-entity
  throttle `l2nx.cdc-engine.persist.checkpoint-min-interval-seconds`
  default 300s), force-flushed on `engine.stop()`, reloaded on
  `engine.start()` before the first tick. Directory-level
  `FileChannel.tryLock` on `.lock` refuses a second adapter JVM pointed at
  the same dir → module `STATE_FAILED`. Closes the orphan-on-restart bug
  where rows deleted from the host DB while the adapter was offline were
  never observed by the next cycle (diff against empty snapshot
  misclassified everything as CREATE). Depends on `:nx-gs-adapter-api` +
  `:nx-gs-kafka` + `:nx-gs-commons` + `fastutil-core` + `gson`. Package
  root `app.l2nx.gs.db.sync`. `:nx-gs-log` shadow-included.
- `:nx-gs-runtime-sync-core` — Runtime-sync `AdapterModule` shipped to Maven Central.
  Owns the in-memory snapshot+diff engine (shared bounded pool —
  `l2nx.runtime-sync.workers` daemon threads, default `max(2, min(entities,
  cores/2))` — replaces the legacy thread-per-entity model; FNV-1a 64-bit hashing
  in Java, replay-on-failed-publish per the at-least-once contract) and resolves
  the Tier-2 `RuntimeStateProvider` SPI for in-memory game-server stores. Reads
  per-entity topics from `ctx.syncTopics().runtime()`. No tombstone on logout —
  `db-sync` owns "permanently gone" semantics. Engine config lives under
  `l2nx.runtime-sync.*` (per-module — independent of `l2nx.cdc-engine.*` because
  the two engines have different tick cadences). Depends on `:nx-gs-adapter-api` +
  `:nx-gs-kafka` + `:nx-gs-commons` + `fastutil-core` + `gson`. Package root
  `app.l2nx.gs.runtime.sync`. `:nx-gs-log` shadow-included.
- `:nx-gs-log` — internal logging facade (`app.l2nx.gs.log`). NOT published; classes are bundled
  into `:nx-gs-commons`, `:nx-gs-kafka`, `:nx-gs-adapter-core`, `:nx-gs-db-sync-core`,
  and `:nx-gs-runtime-sync-core` jars at build time. Auto-detects SLF4J via reflection,
  falls back to console output. Library code never imports SLF4J directly.

## Threading model

Adapter-owned threads (all daemon — never block JVM exit):

| Thread name              | Count        | Purpose                                               |
|--------------------------|--------------|-------------------------------------------------------|
| `nx-adapter-connect`     | 1            | `POST /connect` + reconnect retries (±25% jitter)     |
| `nx-adapter-heartbeat`   | 1            | Periodic heartbeat POSTs                              |
| `nx-gs-kafka-shutdown`   | 1            | JVM shutdown hook                                     |
| `nx-gs-kafka-health`     | 1            | Persistent `AdminClient.describeCluster` health ticks |
| `nx-events-publisher`    | 1            | Bounded-queue fan-out for `NxEvents`                  |
| `nx-commands-consumer`   | 1            | Kafka poll + dispatch for `NxCommands`                |
| `nx-io-N`                | configurable | Adapter-owned IO pool (`ctx.io()` for JDBC/HTTP hops) |
| `nx-cdc-pool-<schema>-N` | configurable | Shared CDC engine pool (db-sync, all entities)        |
| `nx-runtime-sync-pool-N` | configurable | Shared runtime-sync engine pool (all entities)        |
| Kafka producer I/O       | 1            | Internal to `KafkaProducer` (kafka-clients-managed)   |

Pool sizing keys: `l2nx.io.workers`, `l2nx.cdc-engine.workers`,
`l2nx.runtime-sync.workers` (all default `max(2, cores/2)` or
`max(2, min(entities, cores/2))` for engines).

## Constraints

- **Java 8 source + target** — host JVMs span Java 8 to 25+. No `var`, no `Stream.toList()`, no
  records, no `Map.of`, no text blocks, no switch expressions, no pattern matching. Stream API +
  lambda + Optional + `try-with-resources` are fine.
- **No Spring** — adapter loads into a host JVM that may have its own classpath; Spring would clash.
- **Minimum dependencies** — only what is justified:
    - `nx-gs-adapter-api` (contracts)
    - `nx-gs-kafka` (Kafka facade)
    - `gson` (JSON for `/connect`)
    - `slf4j-api` (compileOnly — never imported directly; use the local logging facade)
    - JDK `HttpURLConnection` for HTTP (no OkHttp / Apache HttpClient)
    - JDK `java.util.Properties` for config (no SnakeYAML)
- **Never block game-server threads** — connect / heartbeat / sync run on dedicated daemon threads.
  Any uncaught exception must be caught and logged, never propagated to the host JVM.
- **No reflection-heavy DI** — wiring is plain `new`. Constructor injection only.
- **No Lombok** — keep bytecode plain to avoid surprise across host classloaders.

## Distribution & licensing

Open-core. This repo and every artifact published from it (`nx-gs-adapter-api`,
`nx-gs-commons`, `nx-gs-kafka`, `nx-gs-adapter-core`, `nx-gs-db-sync-core`,
`nx-gs-runtime-sync-core`) are public under Apache 2.0 and published to Maven Central.
The vanilla sync modules (`nx-gs-db-l2j`, `nx-gs-db-lucera`, `nx-gs-dp-l2j`,
`nx-gs-dp-lucera`, `nx-gs-runtime-l2j`, …) — when they land — ship from this repo on
the same terms.

Per-client overrides (`nx-gs-db-l2j-<client>`, `nx-gs-dp-<core>-<client>`) live in
private repos and are shipped privately to that client only — never to Maven Central — to avoid
leaking client-proprietary DB schemas / datapack layouts. They extend a vanilla module via the
template-method pattern.

Implication: nothing committed to this repo, `nx-gs-adapter-api`, or `nx-gs-kafka` may reference
client-proprietary schemas, table names, or column layouts.

## Versioning

Per-module independent versioning via slash-namespaced git tags:

- `api/vX.Y.Z` → publish `nx-gs-adapter-api`
- `commons/vX.Y.Z` → publish `nx-gs-commons`
- `kafka/vX.Y.Z` → publish `nx-gs-kafka`
- `core/vX.Y.Z` → publish `nx-gs-adapter-core`
- `db-sync/vX.Y.Z` → publish `nx-gs-db-sync-core`
- `runtime-sync/vX.Y.Z` → publish `nx-gs-runtime-sync-core`
- future: `db-l2j/vX.Y.Z`, `dp-l2j/vX.Y.Z`, `runtime-l2j/vX.Y.Z`, ...

Each module's `build.gradle.kts` declares
`version = findProperty("${project.name}.version") as String? ?: "<base>"`. CI parses the tag,
maps prefix → subproject (api → nx-gs-adapter-api, commons → nx-gs-commons, kafka →
nx-gs-kafka, core → nx-gs-adapter-core, db-sync → nx-gs-db-sync-core, runtime-sync →
nx-gs-runtime-sync-core), passes `-P<subproject>.version=X.Y.Z`, and publishes ONLY
that module. Other modules stay at fallback. `nx-gs-log` is internal-only (no tag
namespace, no publication).

Local builds default to the per-module fallback (no `local-SNAPSHOT`). Maven Central is the
publish target; CI uses `signingKey`/`signingPassword` Gradle properties (GPG) and
`CENTRAL_TOKEN` for the Sonatype Central Portal upload.

## Commands

```bash
./gradlew build                           # compile + test all modules
./gradlew :nx-gs-adapter-core:test        # core tests only
./gradlew :nx-gs-adapter-core:shadowJar   # fat JAR for drop-in deployment
./gradlew publishToMavenLocal             # publish all modules to ~/.m2
```

## Testing

- **Naming**: `methodName_shouldExpectedBehavior` or `methodName_shouldExpectedBehavior_whenCondition`
- JUnit 5 + Mockito for unit tests
- WireMock for HTTP integration (the adapter is a `/connect` client)
- nx-gs-kafka itself is exercised end-to-end via Testcontainers in its own repo — adapter tests treat
  the Kafka facade as a unit-under-test only at the wiring level

## Comments

- **Comment only the non-obvious — *why*, not *what*.** A legitimate comment explains a non-obvious
  approach, edge case, workaround, constraint, invariant, reason for a choice, units, or
  null-semantics. Code that is clear from names and signatures gets no comment.
- **No comments that restate the code.** Junk like `// inject mapper`, `// loop over rows`,
  `// serialize to JSON`, or obvious headers over trivial blocks — delete them. If a comment line
  paraphrases the line below it, remove it.
- **Don't comment DTO/POJO fields** when name + type already say it. Javadoc on the public wire
  contract is required, but by substance (semantics, units, null-rules, invariants) — never a
  restatement of the field name.
- **Default to fewer comments.** When in doubt, leave it out — rename a variable or extract a method
  instead of explaining with a comment.

## Docs

- `docs/features/<feature-name>/spec.md` + `tech.md` — per-feature design, populated by the
  `/specl-take` skill
