# CLAUDE.md

Guidance for Claude Code (claude.ai/code) when working in this repository.

This file is a **map plus the non-obvious**: where things live, which invariants are not
negotiable, and which traps have already been paid for. It deliberately does **not** restate wire
contracts, DTO fields, or class structure — those live in the code and in `docs/specs/`
(index: [`docs/CLAUDE.md`](docs/CLAUDE.md)). A doc that mirrors code always drifts.

## Workflow

- Always confirm before creating or modifying files — never write code without user approval
- Keep `CLAUDE.md`, `README.md`, and `docs/` in sync with code changes
- Do not run `compile`/`build` after every change — only when explicitly asked or all changes are
  complete

## Project

`nx-gs-adapter` is the multimodule Gradle project hosting the runtime modules of the L2NX
game-server adapter. The adapter embeds into the JVM of a game-server core (L2J / Lucera / Essence
and forks), connects to the L2NX platform via outbound HTTPS, and synchronizes data via Kafka.

Design lives per feature in `docs/specs/NNN-<feature>.md` (or `docs/specs/NNN-<feature>/spec.md`
when a feature carries companion docs). `NNN` is a zero-padded sequential id; the index is
[`docs/CLAUDE.md`](docs/CLAUDE.md) and any spec change updates it in the same pass.

## Modules

| module                     | package root               | what it is                                                                 | primary specs                     |
| -------------------------- | -------------------------- | -------------------------------------------------------------------------- | --------------------------------- |
| `:nx-gs-adapter-api`       | `app.l2nx.gs.adapter.api`  | wire contracts (REST + Kafka DTOs) and the SPI tiers. Zero runtime deps.   | see `nx-gs-adapter-api/CLAUDE.md` |
| `:nx-gs-commons`           | `app.l2nx.gs.commons`      | shared utilities for adapter modules and tenant providers                  | —                                 |
| `:nx-gs-kafka`             | `app.l2nx.gs.kafka`        | lightweight Kafka client facade (producer/consumer, static headers)        | 008-messaging                     |
| `:nx-gs-adapter-core`      | `app.l2nx.gs.adapter.core` | runtime: config, `/connect`, heartbeat, module discovery, events, commands | 001, 002, 008, 009, 011           |
| `:nx-gs-db-sync-core`      | `app.l2nx.gs.db.sync`      | DB-sync module: CRC32 two-phase CDC engine over the Tier-2 schema SPI      | 003, 004, 005, 007, 012, 021      |
| `:nx-gs-runtime-sync-core` | `app.l2nx.gs.runtime.sync` | runtime-sync module: in-memory snapshot+diff over the Tier-2 state SPI     | 006-runtime-sync                  |
| `:nx-gs-gd-sync-core`      | `app.l2nx.gs.gd.sync`      | gd-sync module: static game-data catalog snapshots                         | 030-gamedata-sync                 |
| `:nx-gs-log`               | `app.l2nx.gs.log`          | internal logging facade — NOT published, bundled into the jars above       | —                                 |

Every published module shadow-includes `:nx-gs-log`. Library code never imports SLF4J directly:
the facade auto-detects it by reflection and falls back to console output.

SPI tiers: Tier-1 `AdapterModule` (a module), Tier-2 the per-domain provider SPIs the host
implements (`DbSchemaProvider`, `RuntimeStateProvider`, the gd catalog providers), Tier-3
`JdbcConnectionSource`. All of them are declared in `:nx-gs-adapter-api` so a host provider depends
on the contracts artifact alone.

**External Tier-1 consumer.** `nx-sac-agent-adapter` (repo `nx-sac`) implements `AdapterModule` from
outside this repository: it is the packet-capture agent of L2NX SAC, living in the same game JVM. It
uses the module lifecycle only — its Kafka producer, queue and thread are its own, and no adapter
runtime resource is shared with it. Keep Tier-1 SPI changes backwards compatible or coordinate with
`nx-sac/docs/specs/003-sac-agent.md`.

## Cross-cutting gotchas

Things that cost time to learn and are not visible from the code you are editing:

- **`ConnectContext.io()` / `CommandContext.io()` is mandatory for blocking IO.** JDBC / HTTP must
  hop onto the adapter IO pool — never the game thread, never the Kafka consumer thread. Sanctioned
  exception: the db-sync resync handlers resolve cascades synchronously because the ack reply needs
  the counts, and `ResyncRowsCommand.MAX_PKS` bounds the stall.
- **Capability façades survive reconnect.** `NxEvents` / `NxCommands` handed out via
  `ConnectContext` hold an `AtomicReference` swapped to the live publisher on every reconnect, so
  host modules cache the reference once at `start()` and never re-acquire it.
- **The host registers its game-thread executor via `NxAdapter.hostExecutor(Executor)` BEFORE
  `start()`** — that is what backs `ctx.host().sync(...)` hops in command handlers.
- **Events drop-policy defaults to `newest`** (drop the incoming envelope on overflow, preserving
  queue order). `oldest` is still selectable but over-counts `dropped-total` under multi-producer
  contention: the displaced envelope is counted on the eviction path even when concurrent enqueuers
  race for the same slot.
- **JDBC fetch-size is dialect-dependent and silently wrong if ignored.** MySQL Connector/J
  (`jdbc:mysql:`) honours only `Integer.MIN_VALUE` streaming for large result sets; MariaDB
  Connector/J 3.x rejects a negative `fetchSize` and needs `useCursorFetch=true` in the URL for a
  true server-side cursor; Postgres and others take the configured value as a cursor-batch hint.
  The engine auto-detects from the URL.
- **SQL identifiers are validated, not quoted.** Everything passed through `EntityMapping` /
  `PrimarySource` / `ChildSource` — plus `entityName()` and `schemaName()`, which are interpolated
  into the on-disk snapshot path — must match `^[A-Za-z_][A-Za-z0-9_]{0,63}$`. Schema-qualified or
  quoted names are rejected at engine start.
- **Two adapter JVMs must not share a snapshot dir.** db-sync takes a `FileChannel.tryLock` on
  `<persist.dir>/.lock`; losing it puts the module in `FAILED` rather than corrupting snapshots.
- **runtime-sync emits no tombstone on logout.** "Permanently gone" is db-sync's semantics; the
  runtime channel only carries volatile state.
- **`SNAPSHOT_COMPLETE` is a reconcile point.** A `count=0` marker deletes every row of that entity
  on the platform, so a provider with nothing to give returns `null` (burst aborted, nothing
  reconciled) — never an empty collection. See 030-gamedata-sync §2.

## Threading model

Adapter-owned threads (all daemon — never block JVM exit):

| Thread name              | Count        | Purpose                                               |
| ------------------------ | ------------ | ----------------------------------------------------- |
| `nx-adapter-connect`     | 1            | `POST /connect` + reconnect retries (±25% jitter)     |
| `nx-adapter-heartbeat`   | 1            | Periodic heartbeat POSTs                              |
| `nx-gs-kafka-shutdown`   | 1            | JVM shutdown hook                                     |
| `nx-gs-kafka-health`     | 1            | Persistent `AdminClient.describeCluster` health ticks |
| `nx-events-publisher`    | 1            | Bounded-queue fan-out for `NxEvents`                  |
| `nx-commands-consumer`   | 1            | Kafka poll + dispatch for `NxCommands`                |
| `nx-io-N`                | configurable | Adapter-owned IO pool (`ctx.io()` for JDBC/HTTP hops) |
| `nx-cdc-pool-<schema>-N` | configurable | Shared CDC engine pool (db-sync, all entities)        |
| `nx-runtime-sync-pool-N` | configurable | Shared runtime-sync engine pool (all entities)        |
| `nx-gd-sync-scheduler`   | 0-1          | gd-sync host-readiness polling + periodic resync      |
| Kafka producer I/O       | 1            | Internal to `KafkaProducer` (kafka-clients-managed)   |

Pool sizing keys: `l2nx.io.workers`, `l2nx.cdc-engine.workers`, `l2nx.runtime-sync.workers` (all
default `max(2, cores/2)`, or `max(2, min(entities, cores/2))` for the engines). Per-engine config
namespaces are independent on purpose — `l2nx.cdc-engine.*`, `l2nx.runtime-sync.*`,
`l2nx.gd-sync.*` — because the engines tick at different cadences.

## Constraints

- **Maximally tenant- and build-agnostic — model generic L2 game concepts, not one core's logic.**
  The contracts (Kafka / REST DTOs, SPI types, enums) describe _what_ a value means in generic
  Lineage 2 terms; they never encode _how_ a specific host / core decides it. Classification,
  detection cascades, and build-specific rules belong to the integration (host) code — the adapter
  ships only the shared vocabulary + its generic semantics. Example: `RaidBossKind` defines `RAID` /
  `EPIC` / `INSTANCE_BOSS` and what each means; the host decides which value a given boss gets —
  never bake division names, `instanceof` / engine-API detection (`getReflection()`, `isRaid()`), or
  other core-specific logic into adapter Javadoc / spec. Generalizes the proprietary-schema rule
  (Distribution & licensing) from table/column names to _logic_: we focus on L2 game logic, not a
  specific core's implementation.
- **Java 8 source + target** — host JVMs span Java 8 to 25+. No `var`, no `Stream.toList()`, no
  records, no `Map.of`, no text blocks, no switch expressions, no pattern matching. Stream API +
  lambda + Optional + `try-with-resources` are fine.
- **No Spring** — the adapter loads into a host JVM that may have its own classpath; Spring would
  clash.
- **Minimum dependencies** — only what is justified: `nx-gs-adapter-api` (contracts), `nx-gs-kafka`
  (Kafka facade), `gson` (JSON for `/connect`), `slf4j-api` (compileOnly, never imported directly),
  JDK `HttpURLConnection` for HTTP, JDK `java.util.Properties` for config.
- **Never block game-server threads** — connect / heartbeat / sync run on dedicated daemon threads.
  Any uncaught exception must be caught and logged, never propagated to the host JVM.
- **No reflection-heavy DI** — wiring is plain `new`. Constructor injection only.
- **No Lombok** — keep bytecode plain to avoid surprise across host classloaders.

## Distribution & licensing

Open-core. This repo and every artifact published from it is public under Apache 2.0 and published
to Maven Central. The vanilla sync modules (`nx-gs-db-l2j`, `nx-gs-dp-lucera`, …) — when they land —
ship from this repo on the same terms.

Per-client overrides (`nx-gs-db-l2j-<client>`, `nx-gs-dp-<core>-<client>`) live in private repos and
are shipped privately to that client only — never to Maven Central — to avoid leaking
client-proprietary DB schemas / datapack layouts. They extend a vanilla module via the
template-method pattern.

Implication: nothing committed to this repo may reference client-proprietary schemas, table names,
or column layouts.

## Versioning

Per-module independent versioning via slash-namespaced git tags: `api/`, `commons/`, `kafka/`,
`core/`, `db-sync/`, `runtime-sync/`, `gd-sync/` + `vX.Y.Z`; future `db-l2j/`, `dp-l2j/`, … CI parses
the tag, maps the prefix to its subproject, passes `-P<subproject>.version=X.Y.Z`, and publishes
ONLY that module — the others stay at the fallback literal in their own `build.gradle.kts`.
`nx-gs-log` is internal-only (no tag namespace, no publication).

**The release commit bumps the module's fallback literal to the version being tagged.** CI passes
`-P` only for the tagged module, so every other module's POM pins this one at whatever its
`build.gradle.kts` fallback says — a stale literal ships a dependency pin at a version that predates
the code. The publish workflow refuses to release when the two disagree.

Local builds default to the per-module fallback (no `local-SNAPSHOT`). Maven Central is the publish
target; CI uses `signingKey` / `signingPassword` Gradle properties (GPG) and `CENTRAL_TOKEN` for the
Sonatype Central Portal upload. Central propagation takes ~15-30 min — a dependent release must wait
for the artifact to be resolvable, not just for the tag to be pushed.

### Breaking wire changes go out in two releases, via `@Deprecated`

Renaming or removing anything on the wire (a DTO field, a getter, a whole DTO, an enum constant) is
**never** a single release. It is always:

1. **Additive release.** Add the new shape. Keep the old one alongside it, marked `@Deprecated`,
   with Javadoc naming the replacement AND the concrete event that gates removal ("removed once
   every schema provider emits `classes` — for bohpts, the morning game-server restart"). Consumers
   migrate to the new shape and keep a fallback to the old one.
2. **Removal release.** Delete the deprecated members once that gate has actually fired. This one is
   breaking and takes its own version bump.

The reason is deploy ordering, not politeness: the platform is always deployed **before** the schema
providers that feed it (nx-gameservers, then adapter-api to Maven Central, then the tenant fork). So
there is always a window in which a new consumer reads events emitted by an old producer. A one-shot
rename makes the new consumer blind to that traffic for the whole window — silently, since an
unknown JSON field just deserializes to `null`. Central propagation and game-server restart
schedules make the window hours-to-days wide, not seconds.

Add the migration note to the release's spec, with both gates spelled out — the deprecation gate and
the drop gate are usually different events and cannot be cleared in one pass. Reference:
`docs/specs/029-character-class-state-sync.md` (`subclasses` → `classes`).

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

## Naming conventions

- **Enum-like vocabulary values are `UPPER_SNAKE_CASE`.** Any open-string field standing for a
  closed vocabulary (`type`, `kind`, `category`, `element`, `operateType`, `skillType`, `targetType`,
  `trait`, drop-category, …) carries UPPER_SNAKE: map a core enum via `.name()`, or translate a
  bitmask / int into a canonical token. Never lowercase / camelCase / PascalCase. Single-token codes
  (`A1`, `TG`) are fine as-is.
  Free-form identifiers are NOT enum-like and stay verbatim: effect-handler names (`p_attack`),
  icon / resource names, localized text. Icon names are emitted verbatim — the platform lowercases
  them consumer-side (nx-gamedata `IconPaths.normalize`); do NOT normalize in the adapter.
- **Any field/column referencing an item template is `itemTemplateId` / `item_template_id`** — never
  `itemId`, `itemConsumeId`, `consumeItemId`. It is an FK to the item-template entity; the
  accompanying quantity is `itemTemplateCount`. A reference to a concrete item instance is `itemId`.
  Example: a skill level that consumes an item on cast carries `itemTemplateId` +
  `itemTemplateCount`.
- **Units live in field names, not comments** — `respawnSec`, `reuseDelayMs`, `chancePercent`.
  Non-physical counts / ids (`level`, `weight`, coordinates, stat values) stay unsuffixed.

## Comments

- **Comment only the non-obvious — _why_, not _what_.** A legitimate comment explains a non-obvious
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

- [`docs/CLAUDE.md`](docs/CLAUDE.md) — the spec index, auto-loaded each session. Source of truth for
  which specs exist.
- `docs/specs/NNN-<feature>.md` — one living spec per feature; iterations update the existing spec
  rather than adding a new number. A feature that grows companion docs becomes
  `docs/specs/NNN-<feature>/spec.md` + siblings. Populated by the `spec-authoring` skill.
- `nx-gs-adapter-api/CLAUDE.md` — the wire-contract map (packages, families, contracts worth calling
  out). Anything about DTO shape belongs there or in a spec, not in this file.
