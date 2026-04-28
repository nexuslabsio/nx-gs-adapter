# JDBC Connection Source — tech

> Covers: spec.md
> Sibling: [`db-sync/spec.md`](../db-sync/spec.md) — first consumer

## Overview

`JdbcConnectionSource` is a Tier-3 SPI hosted in `nx-gs-adapter-api` (package
`app.l2nx.gs.adapter.api.spi`) — same artifact that publishes Tier-1 SPI types. Hosts
implementing the SPI depend on `nx-gs-adapter-api` only; they do not need
`nx-gs-db-sync-core`. Resolution at module-init time is two-tier — host SPI first, builtin
bundled-Hikari fallback second:

- **Path 1 [Phase 1]** — host impl discovered via
  `ServiceLoader.load(JdbcConnectionSource.class)`. Host returns connections from its own
  pool. Adapter agnostic to pool wrapper, version, sizing, credential strategy. The SPI
  body is two methods, so the host's adapter is often a one-line wrapper
  (`return DatabaseFactory.getInstance().getConnection();`).
- **Path 2 [Phase 3]** — builtin `BundledHikariConnectionSource` (NOT registered via
  `META-INF/services` — instantiated only when ServiceLoader returns zero impls AND
  `l2nx.db.*` config keys are present). Uses **shadowed Hikari 3.4.5** (relocated to
  `app.l2nx.shaded.hikari.*` at build time so it cannot collide with whatever pool the
  host JVM ships). Reads creds from `l2nx.properties` via `ConfigResolver`. Lives inside
  `nx-gs-db-sync-core` (consumer of the SPI), not in `nx-gs-adapter-api`.

DB credentials never travel through the platform — `ConnectResponse` carries no DB-creds
field at all.

## Structure

- `nx-gs-adapter-api/src/main/java/app/l2nx/gs/adapter/api/jdbc/` [planned] — Tier-3 SPI
  package
    - `JdbcConnectionSource.java` [planned] — the SPI interface (R1, R7, R8); imports
      `app.l2nx.gs.adapter.api.kafka.ops.PoolStats` for the optional `stats()` method
- `nx-gs-db-sync-core/src/main/java/app/l2nx/gs/db/sync/spi/` [planned] — consumer-side
  resolver + Path-2 fallback (Phase 3)
    - `JdbcConnectionSourceResolver.java` [planned, Phase 1] — internal helper running
      the R2 priority chain: ServiceLoader → (Phase 3) bundled-Hikari fallback →
      fail-loud
    - `BundledHikariConnectionSource.java` [planned, Phase 3] — Path-2 fallback impl
      (R6); uses shadowed Hikari, reads `l2nx.db.*` via ConfigResolver
- `nx-gs-db-sync-core/build.gradle.kts` [planned, Phase 3] — Hikari 3.4.5 added as
  `implementation` dep; shadowJar relocates `com.zaxxer.hikari.*` →
  `app.l2nx.shaded.hikari.*`
- `bohpts-core/` [planned, Phase 1, **lives in private bohpts-core repo, NOT this
  monorepo**]
    - depends on `app.l2nx:nx-gs-adapter-api:X.Y.Z` (Maven Central)
    - `<bohpts-package>/BohptsJdbcConnectionSource.java` [planned] — wraps
      `DatabaseFactory.getInstance().getConnection()`; package up to bohpts-core owner
      (see spec Open questions)
    - `src/main/resources/META-INF/services/app.l2nx.gs.adapter.api.spi.JdbcConnectionSource`
      [planned] — service descriptor

## Key components

- **`JdbcConnectionSource`** [planned] (R1, R7, R8) — the SPI interface. Two methods:
  ```java
  public interface JdbcConnectionSource {
      String name();
      Connection getConnection() throws SQLException;
      // R7 (Should): default Optional<PoolStats> stats() { return Optional.empty(); }
      // R8 (Could): default boolean isHealthy() { return true; }
  }
  ```
  Lives in `nx-gs-adapter-api` package `app.l2nx.gs.adapter.api.spi` — single api
  artifact carries every SPI tier (Tier-1 `AdapterModule`, Tier-3
  `JdbcConnectionSource`). Hosts implementing the SPI depend only on
  `nx-gs-adapter-api`.
- **`JdbcConnectionSourceResolver`** [planned] (R2) — implements the R2 priority chain:
  runs `ServiceLoader.load`, falls through to `BundledHikariConnectionSource` on zero
  impls when `l2nx.db.*` is set, fails loud otherwise. Caches the resolved instance.
  Throws `IllegalStateException` with an actionable message on the failure paths;
  caught one level up by `DbSyncModule` and translated into module state `FAILED` (no
  propagation to host JVM).
- **`BundledHikariConnectionSource`** [planned] (R6) — Path-2 fallback, hidden inside
  `nx-gs-db-sync-core` (NOT registered via `META-INF/services` — instantiated only by
  the resolver). Constructor reads `l2nx.db.url` / `l2nx.db.username` / `l2nx.db.password`
  / optional `l2nx.db.pool-size` via injected `ConfigResolver`. Builds shadowed
  `app.l2nx.shaded.hikari.HikariConfig` with `setReadOnly(true)`,
  `setMaximumPoolSize(N)`, `setConnectionTimeout(5000)`, `setValidationTimeout(5000)`,
  `setLeakDetectionThreshold(30000)`, `setPoolName("nx-adapter-db")`. Pool opened in
  constructor; closed in `close()` invoked from `DbSyncModule.onDisconnect`.
- **`PoolStats`** [planned] (R7) — value type for the optional stats path. Lives in
  `app.l2nx.gs.adapter.api.kafka.ops` (shared with `ModuleStatus.Stats.pool`). Fields:
  `int busy`, `int idle`, `Integer total` (nullable). Backward-compatible default
  `stats() = Optional.empty()` so existing impls don't break when the type ships.
- **`BohptsJdbcConnectionSource`** [planned] (R5) — bohpts reference impl, lives in
  bohpts-core source tree. Body of `getConnection()`:
  ```java
  return DatabaseFactory.getInstance().getConnection();
  ```
  Optional `stats()`:
  ```java
  return Optional.of(new PoolStats(
      DatabaseFactory.getInstance().getBusyConnectionCount(),
      DatabaseFactory.getInstance().getIdleConnectionCount()));
  ```

## Data flows

### 1. SPI resolution (one-shot at module init)

```
DbSyncModule.onConnect(ctx)
  → JdbcConnectionSourceResolver.resolve(configResolver)
  → ServiceLoader.load(JdbcConnectionSource.class)
  → providers.size():
       1 → use providers.get(0)              [Path 1 — host explicit registration]
       0:
         l2nx.db.url + username + password all present →
              new BundledHikariConnectionSource(configResolver)   [Path 2 — fallback]
         else →
              throw IllegalStateException(
                  "no JdbcConnectionSource SPI registered AND no l2nx.db.* config;
                   pick Path 1 (host SPI impl) or Path 2 (l2nx.db.* fallback) — see
                   jdbc-connection-source feature docs")
              → caught by DbSyncModule → state = FAILED
       >1 →
              throw IllegalStateException("multiple sources: [fqcn1, fqcn2]; ...")
              → caught by DbSyncModule → state = FAILED
  → DbSyncModule caches the source on `this.connectionSource`
  → log INFO: "JdbcConnectionSource: <source.name()>" (e.g. "bohpts-hikari" or
    "nx-adapter-bundled-hikari")
```

### 2. Per-borrow lifecycle (every Phase 1 / Phase 2 query)

```
engine code (e.g. Phase1Hasher.hash)
  → try (Connection c = source.getConnection()) {
        c.setReadOnly(true);                          // R3: per-borrow enforcement
        try (PreparedStatement ps = c.prepareStatement("SELECT ...");
             ResultSet rs = ps.executeQuery()) {
            // read
        }
    }                                                 // R4: try-with-resources close
  → connection returned to host's pool
```

### 3. Heartbeat enrichment (R7, optional)

```
HeartbeatService tick
  → if module enabled: stats = source.stats().orElse(null)
  → enabledModules.add({name: "db-sync", state: ..., poolStats: stats})
```

If the host's `JdbcConnectionSource` doesn't override `stats()`, the heartbeat reports
`null` for `poolStats` — heartbeat never fails on missing stats.

## Integration points

- **`nx-gs-adapter-api`** (R1) — hosts the Tier-3 SPI interface. Single api artifact
  for Tier-1 + Tier-3 SPI types; consumers (hosts, db-sync-core) all depend on this
  one artifact for SPI contracts.
- **`nx-gs-db-sync-core`** (R2, R6) — consumer of the Tier-3 SPI; hosts the
  `JdbcConnectionSourceResolver` (Phase 1) and `BundledHikariConnectionSource`
  (Phase 3). Does NOT define the SPI interface itself.
- **`bohpts-core`** (R5) — private repo, hosts `BohptsJdbcConnectionSource` and the
  service descriptor. Depends on `app.l2nx:nx-gs-adapter-api:X.Y.Z`. NOT a separately
  published artifact.
- **`db-sync` feature** — first consumer. R2 of db-sync invokes
  `JdbcConnectionSourceLoader.load()` at `onConnect`; per-borrow `setReadOnly(true)` is
  R3 of db-sync (cross-referenced) plus R3 here (the SPI-level contract that consumers
  MUST satisfy).
- **Bohpts `l2e.gameserver.database.DatabaseFactory`** (R5) — the singleton bohpts uses
  internally; bohpts' `JdbcConnectionSource` impl wraps it. We do NOT touch
  `DatabaseFactory` itself — only call its public `getConnection()` /
  `getBusyConnectionCount()` / `getIdleConnectionCount()` methods.
- **`nx-gs-adapter-api`** — no change required. The SPI is internal to host↔adapter (not
  platform-facing), so it stays out of the wire-contracts module.

## Decisions

- **DB credentials never travel through the platform.** The wire contract carries no
  DB-creds field. Path 1 reuses in-process host creds; Path 2 reads operator-local
  `l2nx.properties`. Rationale:
  security-conscious operators (especially self-hosted ones who run their own MySQL
  beyond our visibility) do not entrust their DB password to a third-party platform UI,
  even if encrypted at rest. Removing the path entirely is cleaner than offering it
  with caveats — operators who DO want platform-driven creds (rare) can reintroduce a
  dedicated key-delivery channel later if a real demand surfaces.
- **Two-path resolution: host SPI wins over fallback.** Explicit host registration
  expresses operator intent more clearly than "creds happen to be in
  `l2nx.properties`". When both are set up, ignoring the local creds is the
  least-surprising tie-break — operator who wrote the SPI did so deliberately. Resolver
  emits an INFO log identifying the chosen path so it's never ambiguous in practice.
- **SPI lives in `nx-gs-adapter-api`, alongside Tier-1 SPI types.** Single api
  artifact carries every SPI tier (Tier-1 `AdapterModule`, Tier-3
  `JdbcConnectionSource`). Future DB-reading modules (`nx-gs-metrics-db`, etc.) depend
  only on `nx-gs-adapter-api`, not on `nx-gs-db-sync-core`. Hosts implementing the SPI
  also depend only on `nx-gs-adapter-api` — no transitive db-sync engine code pulled
  into the host JVM.
- **Bundled Hikari 3.4.5 (shadowed) for Path 2 fallback.** Hikari 3.4.5 is the last
  Java-8-compatible release (bytecode major version 52); Hikari 4.x+ require Java 11
  and would refuse to load on Java 8 hosts (which are still common in the L2J
  ecosystem). The `com.zaxxer.hikari.*` packages are relocated to
  `app.l2nx.shaded.hikari.*` via the ShadowJar plugin at build time; this is the same
  pattern already used for `:nx-log`. Result: zero classpath conflicts with whatever
  HikariCP / DBCP2 / etc. version the host JVM ships, at the cost of ~150 KB extra in
  `nx-gs-db-sync-core.jar`. The trade-off is justified — without bundling, Path 2 isn't
  achievable for closed-source clients without source-access surgery.
- **Minimal interface (`name()` + `getConnection()`), not `javax.sql.DataSource`.**
  `DataSource` has 7+ methods (`getConnection(user, pass)`, log writer, login timeout,
  parent logger, `unwrap`, `isWrapperFor`); forcing host impls to fill all of them is
  hostile for the simple "give me a Connection" use case. The two-method SPI is also
  natural for the bohpts case where `DatabaseFactory.getConnection()` is already the
  one-line implementation. If a consumer ever needs the full `DataSource` surface, that
  consumer can wrap an `JdbcConnectionSource` into a tiny `DataSource` adapter locally.
- **Per-borrow `setReadOnly(true)`, not pool-level.** Pool config is host-owned; we
  cannot impose pool-level read-only without instantiating our own pool (defeats the
  whole point). Per-borrow is portable across all pool implementations — every
  `Connection` impl supports `setReadOnly(boolean)` per the JDBC spec. Performance impact
  is negligible (one method call per borrow). Treating this as a CONSUMER obligation (R3)
  rather than something the SPI does internally keeps the SPI dumb and avoids
  decorator-stacking.
- **Single-impl assumption with fail-loud on multi-impl.** Mirrors the Tier-2
  `DbSchemaProvider` discovery rule. Operator deployments naturally have one provider
  (host JVM ships its own; vanilla modules + bohpts modules don't co-exist in the same
  classpath). Resolution strategies for multi-impl scenarios (config selector / shadow
  exclusion / activator JAR — same options as Tier-2) deferred to vanilla-extraction
  time, decided in the same slice as Tier-2's resolution.
- **Provider receives no parameters at discovery / construction.** Providers that share
  the host pool reach their pool via the host's static singleton (e.g.
  `DatabaseFactory.getInstance()`). Providers running Scenario Y (separate read-only
  pool) are responsible for sourcing their creds — typically from a host-side bootstrap
  hook that stashes `ConnectResponse.database` into a static somewhere accessible.
  Forcing an `init(ConnectContext)`-style lifecycle on every provider would burden the
  Scenario-X majority for the benefit of the Scenario-Y minority. Revisit if a real
  Scenario-Y consumer arrives — see spec Open question.
- **`stats()` is a default method, not a required method.** Adding `stats()` later would
  break every existing impl if it weren't a default. We pre-declare a `default` returning
  `Optional.empty()` so impls upgrade voluntarily. Same for `isHealthy()` (R8).
- **Adapter never closes the source.** Host owns pool lifecycle. Module `onDisconnect`
  drops the cached source reference and that's it; the source's own static singleton
  (e.g. bohpts' `DatabaseFactory`) keeps running for the host's own use.

## Extension points

- **New host-side pool wrapper** — implement `JdbcConnectionSource`, drop a
  `META-INF/services/app.l2nx.gs.adapter.api.spi.JdbcConnectionSource` resource pointing to
  it, ensure exactly one descriptor on classpath. No code change in
  `nx-gs-db-sync-core`. Works for HikariCP, DBCP2, c3p0, Tomcat JDBC, custom singletons,
  even raw `DriverManager` for tests.
- **Pool stats** — host overrides the `stats()` default if its pool exposes busy / idle
  counts (Hikari `getHikariPoolMXBean()`, DBCP2 `getNumActive()`, etc.). Default
  `Optional.empty()` is safe for pools that don't expose stats.
- **Health probing** — host overrides `isHealthy()` (R8) when it has a richer view of
  pool health than "next `getConnection` succeeds". Useful for circuits where the pool
  knows it's down (e.g. a circuit-breaker around the underlying DataSource).
- **Future Tier-3 expansions** — if subsequent slices need additional SPI methods
  (transactional borrow, async / `CompletionStage` API, named connections for write
  scenarios when DML eventually lands), extend `JdbcConnectionSource` with `default`
  methods to preserve backward compat with existing host impls.
