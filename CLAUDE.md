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

Architecture reference: `E:/projects/nx-gs-adapter-architecture-v2.md` (working copy in repo root —
will be split into per-feature docs in `docs/features/` as features land).

## Modules

- `:nx-gs-adapter-api` — wire contracts (REST + Kafka DTOs) shared with the platform.
  Java 8 POJOs, zero runtime deps, package root `app.l2nx.gs.adapter.api`.
- `:nx-gs-kafka` — lightweight Kafka client facade. Java 8, depends on `kafka-clients` + `gson`,
  `slf4j-api` compileOnly. Package root `app.l2nx.gs.kafka`. `:nx-log` shadow-included.
- `:nx-gs-adapter-core` — runtime: config resolution, POST `/connect`, heartbeat, ServiceLoader-based
  module discovery, lifecycle. Depends on `:nx-gs-adapter-api` + `:nx-gs-kafka` + `gson`.
  Package root `app.l2nx.gs.adapter.core`. `:nx-log` shadow-included.
- `:nx-log` — internal logging facade (`app.l2nx.log`). NOT published; classes are bundled
  into `:nx-gs-kafka` and `:nx-gs-adapter-core` jars at build time. Auto-detects SLF4J via
  reflection, falls back to console output. Library code never imports SLF4J directly.

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

Open-core. This repo, plus `nx-gs-adapter-api` and `nx-gs-kafka`, are public under Apache 2.0 and
published to Maven Central. The vanilla sync modules (`nx-gs-adapter-db-l2j`,
`nx-gs-adapter-db-lucera`, `nx-gs-adapter-dp-l2j`, `nx-gs-adapter-dp-lucera`) — when they land
— ship from this repo on the same terms.

Per-client overrides (`nx-gs-adapter-db-l2j-<client>`, `nx-gs-adapter-dp-<core>-<client>`) live in
private repos and are shipped privately to that client only — never to Maven Central — to avoid
leaking client-proprietary DB schemas / datapack layouts. They extend a vanilla module via the
template-method pattern.

Implication: nothing committed to this repo, `nx-gs-adapter-api`, or `nx-gs-kafka` may reference
client-proprietary schemas, table names, or column layouts.

## Versioning

Per-module independent versioning via slash-namespaced git tags:

- `api/vX.Y.Z` → publish `nx-gs-adapter-api`
- `kafka/vX.Y.Z` → publish `nx-gs-kafka`
- `core/vX.Y.Z` → publish `nx-gs-adapter-core`
- future: `db-l2j/vX.Y.Z`, `dp-l2j/vX.Y.Z`, ...

Each module's `build.gradle.kts` declares
`version = findProperty("${project.name}.version") as String? ?: "<base>"`. CI parses the tag,
maps prefix → subproject (api → nx-gs-adapter-api, kafka → nx-gs-kafka, core → nx-gs-adapter-core),
passes `-P<subproject>.version=X.Y.Z`, and publishes ONLY that module. Other modules stay at
fallback. `nx-log` is internal-only (no tag namespace, no publication).

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
- Architecture reference (this repo's source of truth) — currently at the working-copy
  `nx-gs-adapter-architecture-v2.md` outside this repo; will be migrated to `docs/architecture.md`
