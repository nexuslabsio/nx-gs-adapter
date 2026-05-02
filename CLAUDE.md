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
  `kafka.NxHeaders` — the wire-level Kafka header contract (`NX_SERVER_ID` constant
    + pure-JDK `encodeUuid` / `decodeUuid` helpers) shared between adapter producers
      and platform consumers.
- `:nx-gs-commons` — shared utilities for adapter modules and tenant providers:
  `concurrent.SafeRunnable` (exception-swallowing Runnable wrapper), `hash.Fnv1a64`
  (FNV-1a 64-bit hash), `Nulls` (sentinel-to-null), `jdbc.JdbcNulls` (null-aware
  `ResultSet` readers). Java 8, deps: `jspecify` only. Package root
  `app.l2nx.gs.commons`. `:nx-gs-log` shadow-included. Published to Maven Central.
- `:nx-gs-kafka` — lightweight Kafka client facade. Java 8, depends on `kafka-clients` + `gson`,
  `slf4j-api` compileOnly. Package root `app.l2nx.gs.kafka`. `:nx-gs-log` shadow-included.
  Producer supports connection-scoped static headers (`KafkaConfig.Builder.producerStaticHeader` /
  `NxProducer.create(props, gson, headers)`) — adapter-core stamps `Nx-Server-Id`
  (raw 16-byte UUID) on every record post-`/connect`.
- `:nx-gs-adapter-core` — runtime: config resolution, POST `/connect`, heartbeat, ServiceLoader-based
  module discovery, lifecycle. Depends on `:nx-gs-adapter-api` + `:nx-gs-kafka` +
  `:nx-gs-commons` + `gson`. Package root `app.l2nx.gs.adapter.core`. `:nx-gs-log` shadow-included.
- `:nx-gs-db-sync-core` — DB-sync `AdapterModule` shipped to Maven Central. Owns the
  CRC32 two-phase CDC engine (one daemon thread per entity, server-side CRC32 hashing,
  per-row snapshot swap on Kafka ack) and resolves the Tier-2 `DbSchemaProvider` SPI
  (defined in `nx-gs-adapter-api` so client providers depend only on the api artifact).
  Reads its per-entity Kafka topics from `ctx.syncTopics().db()` (db namespace of
  the namespaced `SyncTopics` bundle). Surfaces both `pool` (from
  `JdbcConnectionSource.stats()`) and `entities` (per-entity `EntityStats`) slots in
  the heartbeat. Engine config lives under `l2nx.cdc-engine.*` (file-first source
  chain). Depends on `:nx-gs-adapter-api` + `:nx-gs-kafka` + `:nx-gs-commons` +
  `fastutil-core` + `gson`. Package root `app.l2nx.gs.db.sync`. `:nx-gs-log`
  shadow-included.
- `:nx-gs-runtime-sync-core` — Runtime-sync `AdapterModule` shipped to Maven Central.
  Owns the in-memory snapshot+diff engine (one daemon thread per entity, FNV-1a
  64-bit hashing in Java, replay-on-failed-publish per the at-least-once contract)
  and resolves the Tier-2 `RuntimeStateProvider` SPI for in-memory game-server
  stores. Reads per-entity topics from `ctx.syncTopics().runtime()`. No tombstone
  on logout — `db-sync` owns "permanently gone" semantics. Engine config lives
  under `l2nx.runtime-sync.*` (per-module — independent of `l2nx.cdc-engine.*`
  because the two engines have different tick cadences). Depends on
  `:nx-gs-adapter-api` + `:nx-gs-kafka` + `:nx-gs-commons` + `fastutil-core` +
  `gson`. Package root `app.l2nx.gs.runtime.sync`. `:nx-gs-log` shadow-included.
- `:nx-gs-log` — internal logging facade (`app.l2nx.gs.log`). NOT published; classes are bundled
  into `:nx-gs-commons`, `:nx-gs-kafka`, `:nx-gs-adapter-core`, `:nx-gs-db-sync-core`,
  and `:nx-gs-runtime-sync-core` jars at build time. Auto-detects SLF4J via reflection,
  falls back to console output. Library code never imports SLF4J directly.

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

## Docs

- `docs/features/<feature-name>/spec.md` + `tech.md` — per-feature design, populated by the
  `/specl-take` skill
