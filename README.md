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
| `nx-gs-adapter-api`  | `app.l2nx:nx-gs-adapter-api`  | yes                  | Wire contracts (REST + Kafka DTOs) + Tier-1/Tier-3 SPI shared with the platform    |
| `nx-gs-kafka`        | `app.l2nx:nx-gs-kafka`        | yes                  | Lightweight Kafka client facade for Java 8+ host JVMs                              |
| `nx-gs-adapter-core` | `app.l2nx:nx-gs-adapter-core` | yes                  | Adapter runtime — connect, heartbeat, ServiceLoader-based module discovery         |
| `nx-gs-db-sync-core` | `app.l2nx:nx-gs-db-sync-core` | yes                  | DB-sync `AdapterModule` — Phase 1 SPI smoke-check; Phase 2 adds CRC32 CDC engine   |
| `nx-log`             | —                             | no (shadow-included) | Internal logging facade — bundled into `nx-gs-kafka` and `nx-gs-adapter-core` jars |

Future modules:

- `nx-gs-db-l2j` — DB sync for vanilla L2J schema
- `nx-gs-db-lucera` — DB sync for vanilla Lucera schema
- `nx-gs-dp-l2j` — Datapack sync for vanilla L2J
- `nx-gs-dp-lucera` — Datapack sync for vanilla Lucera

Per-client overrides (private repos) extend vanilla via the template-method pattern (e.g.
`nx-gs-db-bohpts`).

## Distribution & licensing

Open-core model — Apache 2.0, published to Maven Central:

- `nx-gs-adapter-api` (contracts + SPI)
- `nx-gs-kafka` (Kafka facade)
- `nx-gs-adapter-core` (runtime)
- `nx-gs-db-sync-core` (DB-sync engine + Tier-2 SPI host)
- `nx-gs-db-l2j`, `nx-gs-db-lucera`, `nx-gs-dp-l2j`, `nx-gs-dp-lucera` (vanilla sync
  modules — community schemas, future)

Operators can audit every artifact that loads into their host JVM and drop in via Maven
Central without a private-repo handshake.

Per-client modules (`nx-gs-db-l2j-<client>`, `nx-gs-dp-<core>-<client>`) that encode a
client-proprietary DB schema or datapack layout are shipped privately to that client
only — never published to Maven Central — to avoid leaking their schema internals.
They extend the vanilla module via the template-method pattern.

## Versioning

Per-module independent versioning via slash-namespaced git tags:

- `api/vX.Y.Z` → release `nx-gs-adapter-api`
- `kafka/vX.Y.Z` → release `nx-gs-kafka`
- `core/vX.Y.Z` → release `nx-gs-adapter-core`
- `db-sync/vX.Y.Z` → release `nx-gs-db-sync-core`
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

**DB credentials (only when DB-sync modules ship and host JVM doesn't register a
`JdbcConnectionSource` via SPI):** `l2nx.db.url`, `l2nx.db.username`, `l2nx.db.password` —
operator-local fallback for the bundled-Hikari pool. Creds NEVER travel through the
platform; they live only on the operator's machine. If the host (e.g. bohpts-core)
already exposes a `JdbcConnectionSource` via `META-INF/services`, leave the `l2nx.db.*`
keys unset — the adapter will reuse the host's existing pool.

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
