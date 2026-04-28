# nx-gs-adapter

L2NX game-server adapter — multimodule library that embeds into the JVM process of an L2J / Lucera /
Essence game server core and connects it to the L2NX platform.

## Project

- **Organization:** nexuslabsio
- **Group:** `app.l2nx`
- **Java:** 8 (source + target)
- **Build:** Gradle (Kotlin DSL), multimodule, per-module versioning

## Modules

| Module               | Artifact                      | Published            | Purpose                                                                            |
|----------------------|-------------------------------|----------------------|------------------------------------------------------------------------------------|
| `nx-gs-adapter-api`  | `app.l2nx:nx-gs-adapter-api`  | yes                  | Wire contracts (REST + Kafka DTOs) shared with the platform                        |
| `nx-gs-kafka`        | `app.l2nx:nx-gs-kafka`        | yes                  | Lightweight Kafka client facade for Java 8+ host JVMs                              |
| `nx-gs-adapter-core` | `app.l2nx:nx-gs-adapter-core` | yes                  | Adapter runtime — connect, heartbeat, lifecycle                                    |
| `nx-log`             | —                             | no (shadow-included) | Internal logging facade — bundled into `nx-gs-kafka` and `nx-gs-adapter-core` jars |

Future modules (per architecture v2):

- `nx-gs-adapter-db-l2j` — DB sync for vanilla L2J schema
- `nx-gs-adapter-db-lucera` — DB sync for vanilla Lucera schema
- `nx-gs-adapter-dp-l2j` — Datapack sync for vanilla L2J
- `nx-gs-adapter-dp-lucera` — Datapack sync for vanilla Lucera

Per-client overrides (private repos) extend vanilla via the template-method pattern (e.g.
`nx-gs-adapter-db-l2j-bohpts`).

## Distribution & licensing

Open-core model — Apache 2.0, published to Maven Central:

- `nx-gs-adapter-api` (contracts)
- `nx-gs-kafka` (Kafka facade)
- `nx-gs-adapter-core` (runtime)
- `nx-gs-adapter-db-l2j`, `nx-gs-adapter-db-lucera`, `nx-gs-adapter-dp-l2j`,
  `nx-gs-adapter-dp-lucera` (vanilla sync modules — community schemas, future)

Operators can audit every artifact that loads into their host JVM and drop in via Maven
Central without a private-repo handshake.

Per-client modules (`nx-gs-adapter-db-l2j-<client>`, `nx-gs-adapter-dp-<core>-<client>`)
that encode a client-proprietary DB schema or datapack layout are shipped privately to
that client only — never published to Maven Central — to avoid leaking their schema
internals. They extend the vanilla module via the template-method pattern.

## Versioning

Per-module independent versioning via slash-namespaced git tags:

- `api/v0.1.0` → release `nx-gs-adapter-api` 0.1.0
- `gs-kafka/v0.0.1` → release `nx-gs-kafka` 0.0.1
- `core/v0.1.0` → release `nx-gs-adapter-core` 0.1.0
- `db-l2j/v…`, `dp-l2j/v…`, `db-lucera/v…`, `dp-lucera/v…` (future)

Each module's `build.gradle.kts` declares
`version = findProperty("${project.name}.version") as String? ?: "<base>"`.
CI parses the tag prefix and passes `-P<module>.version=X.Y.Z` to that subproject only —
other modules stay at their fallback `<base>` version and are NOT republished by the same tag.

`nx-log` is internal-only (shadow-included into consuming jars) and has no tag namespace.

## Quick start

Drop the fat JAR into the game-server classpath and configure the adapter via a properties file.
The resolution chain (file-first, sysprop-fallback per key):

1. **`-Dl2nx.config-file=<path>`** — explicit path (absolute or relative to the JVM working
   directory). If set but unreadable, the adapter fails loud with `IllegalStateException` so
   operator typos do not silently fall through.
2. **`l2nx.properties` next to the JVM working directory** — implicit fallback when
   `-Dl2nx.config-file` is unset. Missing file is OK — the adapter then relies on JVM system
   properties.
3. **`-Dl2nx.<key>=<value>`** — per-key override consulted only when the file does not provide
   the key.

Environment variables are not supported.

A starter file with placeholder values lives at the repo root: [`l2nx.properties`](./l2nx.properties) —
copy it next to your game-server working directory (or to any path you point `-Dl2nx.config-file`
at) and fill in the real `gs-key` + `platform-url`.

Required: `l2nx.gs-key`, `l2nx.platform-url`. Optional: `l2nx.enabled` (default `false` — must be
`true` for the adapter to actually run; otherwise it transitions to `DISABLED` at startup).

```bash
# Option A: l2nx.properties in the JVM working directory (implicit fallback)
java -cp "l2j-server.jar:nx-gs-adapter-core-all.jar" org.l2j.server.GameServer
# where ./l2nx.properties (relative to where you launch java) contains:
#   l2nx.gs-key=nx_sk_...
#   l2nx.platform-url=https://acme.api.l2nx.app
#   l2nx.enabled=true

# Option B: explicit path (recommended for prod — keeps the secret outside the binary)
java -Dl2nx.config-file=/etc/l2nx/adapter.properties \
     -cp "l2j-server.jar:nx-gs-adapter-core-all.jar" org.l2j.server.GameServer
```

See [`docs/`](./docs/) for architectural detail.

## Commands

```bash
./gradlew build               # compile + test
./gradlew :nx-gs-adapter-core:shadowJar    # build the fat JAR
./gradlew publishToMavenLocal # publish all modules to ~/.m2
```
