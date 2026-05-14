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

- `nx-gs-adapter-api/src/main/java/app/l2nx/gs/adapter/api/spi/` — Tier-3 SPI
  alongside Tier-1 SPI types (single api package for every SPI tier)
    - `JdbcConnectionSource.java` — the SPI interface (R1, R7, R8); imports
      `app.l2nx.gs.adapter.api.kafka.ops.PoolStats` for the optional `stats()` method
- `nx-gs-db-sync-core/src/main/java/app/l2nx/gs/db/sync/`
    - `DbSyncModule.java` — Phase 1 owns the R2 resolution chain inline in
      `onConnect()` (ServiceLoader load + 0/>1 → fail-loud); a separate
      `JdbcConnectionSourceResolver` class is not needed for Phase 1 (logic is ~20
      lines and lives next to its only consumer). Refactor candidate when Path 2 /
      Phase 3 lands.
    - `BundledHikariConnectionSource.java` [planned, Phase 3] — Path-2 fallback impl
      (R6); uses shadowed Hikari, reads `l2nx.db.*` via ConfigResolver
- `nx-gs-db-sync-core/build.gradle.kts` [planned, Phase 3] — Hikari 3.4.5 added as
  `implementation` dep; shadowJar relocates `com.zaxxer.hikari.*` →
  `app.l2nx.shaded.hikari.*`
- `bohpts-core/` (**private repo, NOT this monorepo**)
    - depends on `app.l2nx:nx-gs-adapter-core:0.3.1` + `nx-gs-db-sync-core:0.1.0`
      (Maven Central)
    - `core/src/main/java/l2e/gameserver/l2nx/BohptsJdbcConnectionSource.java` —
      wraps `DatabaseFactory.getInstance().getConnection()`, sets `readOnly=true`
      per-borrow as defense-in-depth; package follows bohpts convention
      (`l2e.gameserver.l2nx` houses all l2nx-related plumbing)
    - `core/src/main/resources/META-INF/services/app.l2nx.gs.adapter.api.spi.JdbcConnectionSource`
      — service descriptor pointing to `BohptsJdbcConnectionSource`

## Key components

- **`JdbcConnectionSource`** (R1, R7, R8) — the SPI interface. Two methods:
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
- **R2 resolution** (R2) — Phase 1 lives inline in `DbSyncModule.onConnect()`:
  runs `ServiceLoader.load(JdbcConnectionSource.class)` with TCCL save/restore,
  applies the 0 / 1 / >1 rule, logs an actionable ERROR and transitions the module
  to `FAILED` on the failure paths. A separate `JdbcConnectionSourceResolver`
  class will be extracted in Phase 3 when the bundled-Hikari fallback joins the
  chain (`ServiceLoader → l2nx.db.* fallback → fail-loud`).
- **`BundledHikariConnectionSource`** [planned] (R6) — Path-2 fallback, hidden inside
  `nx-gs-db-sync-core` (NOT registered via `META-INF/services` — instantiated only by
  the resolver). Constructor reads `l2nx.db.url` / `l2nx.db.username` / `l2nx.db.password`
  / optional `l2nx.db.pool-size` via injected `ConfigResolver`. Builds shadowed
  `app.l2nx.shaded.hikari.HikariConfig` with `setReadOnly(true)`,
  `setMaximumPoolSize(N)`, `setConnectionTimeout(5000)`, `setValidationTimeout(5000)`,
  `setLeakDetectionThreshold(30000)`, `setPoolName("nx-adapter-db")`. Pool opened in
  constructor; closed in `close()` invoked from `DbSyncModule.onDisconnect`.
- **`PoolStats`** (R7) — value type for the optional stats path. Lives in
  `app.l2nx.gs.adapter.api.kafka.ops` (shared with `ModuleStatus.Stats.pool`). Fields:
  `Integer active`, `Integer idle`, `Integer total`, `Integer waiting` — all nullable so
  providers expose only the subset their pool API gives them. **api/0.6.0 (in source,
  tag pending)**: field `busy` (api/0.5.0) renamed to `active` (HikariCP / Tomcat JDBC
  / DBCP2 convention); new `waiting` slot added for diagnosing pool backpressure
  (HikariCP's `getThreadsAwaitingConnection`, equivalent in other pools). Backward-
  compatible default `stats() = Optional.empty()` so existing impls don't break when
  the type ships.
- **`BohptsJdbcConnectionSource`** (R5) — bohpts reference impl, lives in
  bohpts-core source tree under `l2e.gameserver.l2nx`. Body of `getConnection()`
  borrows from `DatabaseFactory.getInstance()` and flips `setReadOnly(true)` on
  the borrowed Connection as defense-in-depth (closes the connection and
  rethrows on `SQLException`). HikariCP resets the dirty `readOnly` flag back
  to the pool default on connection return, so the flag does NOT leak to the
  host's own subsequent borrowers. This is belt-and-suspenders on top of the
  SQL-level `START TRANSACTION ... READ ONLY` the CDC engine issues (R3 +
  cdc-engine R11) — explicit signal to the host that the adapter does not
  modify host data through this source. `stats()` overrides the default with
  `PoolStats.builder()
  .active(DatabaseFactory.getBusyConnectionCount())
  .idle(DatabaseFactory.getIdleConnectionCount()).build()` — `total` and `waiting`
  left null because `DatabaseFactory` does not expose
  `HikariPoolMXBean.getTotalConnections()` / `getThreadsAwaitingConnection()` directly
  (per `PoolStats` contract: every field is nullable).

## Data flows

### 1. SPI resolution (one-shot at module init)

Phase 1 (current):

```
DbSyncModule.onConnect(ctx)
  → ServiceLoader.load(JdbcConnectionSource.class) with TCCL save/restore
  → providers.size():
       1 → cache as `this.source`; smoke-check (isValid(5) — adapter no longer
           calls setReadOnly on the borrowed connection; see R3) →
           ACTIVE on success / DEGRADED on failure (source kept so stats() still surfaces)
       0 → log ERROR ("no JdbcConnectionSource SPI registered — register one via
           META-INF/services/... — see jdbc-connection-source feature docs"); state = FAILED
       >1 → log ERROR listing impl FQCNs; state = FAILED
  → on success log INFO: "JdbcConnectionSource resolved: <source.name()>"
    (e.g. "bohpts-hikari")
```

Phase 3 will add the bundled-Hikari fallback:

```
       0 AND l2nx.db.url + username + password all present →
              new BundledHikariConnectionSource(configResolver)   [Path 2 — fallback]
       0 AND no l2nx.db.* → fail-loud (current Phase 1 behavior)
```

### 2. Per-borrow lifecycle (every Phase 1 / Phase 2 query)

```
engine code (EntitySyncTask cycle)
  → try (Connection c = source.getConnection()) {
        // Adapter does NOT call c.setReadOnly(true) — would leak the flag back
        // to other consumers of the host pool. Read-only is SQL-level via
        // START TRANSACTION ... READ ONLY in ConsistentSnapshotTxn (cdc-engine R11).
        try (Statement guard = c.createStatement()) {
            guard.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY");
        }
        try (PreparedStatement ps = c.prepareStatement("SELECT ...")) {
            ps.setFetchSize(cfg.fetchSize);            // cursor mode (cdc-engine R9)
            try (ResultSet rs = ps.executeQuery()) {
                // read
            }
        }
        c.commit();
    }                                                 // R4: try-with-resources close
  → connection returned to host's pool
  → host pool MUST reset readOnly / autoCommit / isolation on return (R3)
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

- **`nx-gs-adapter-api`** (R1) — hosts the Tier-3 SPI interface (released in
  `0.5.0`). Single api artifact for Tier-1 + Tier-3 SPI types; consumers
  (hosts, db-sync-core) all depend on this one artifact for SPI contracts.
- **`nx-gs-db-sync-core`** (R2, R6) — consumer of the Tier-3 SPI; Phase 1 inlines
  resolution in `DbSyncModule.onConnect`; `BundledHikariConnectionSource` arrives
  in Phase 3. Does NOT define the SPI interface itself.
- **`bohpts-core`** (R5) — private repo, hosts `BohptsJdbcConnectionSource` and
  the service descriptor. Depends on `app.l2nx:nx-gs-adapter-core:0.3.1` +
  `nx-gs-db-sync-core:0.1.0`. NOT a separately published artifact.
- **`db-sync` feature** — first consumer. R2 of db-sync invokes
  `JdbcConnectionSourceLoader.load()` at `onConnect`. The adapter does NOT call
  `setReadOnly` per borrow — the host-pool contract (R3 here) keeps the pool's
  state-reset responsibility on the host, and the CDC engine enforces read-only
  at the SQL level (`START TRANSACTION ... READ ONLY`).
- **Bohpts `l2e.gameserver.database.DatabaseFactory`** (R5) — the singleton bohpts
  uses internally; bohpts' `JdbcConnectionSource` impl wraps it. We do NOT touch
  `DatabaseFactory` itself — only call its public `getConnection()` /
  `getBusyConnectionCount()` (→ `PoolStats.active`) / `getIdleConnectionCount()`
  (→ `PoolStats.idle`) methods.
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
  pattern already used for `:nx-gs-log`. Result: zero classpath conflicts with whatever
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
- **Read-only enforced at SQL level by the engine; provider-level `setReadOnly`
  is an optional defense-in-depth signal.** The CDC engine (adapter side) does
  NOT call `Connection.setReadOnly(true)` on borrowed connections. Read-only is
  enforced via `START TRANSACTION ... READ ONLY` inside every Phase 1 / Phase 2
  transaction (see [`cdc-engine` R11](../cdc-engine/spec.md)) — SQL-level
  enforcement is scoped to the transaction only, no leakage. **Providers MAY
  call `Connection.setReadOnly(true)` themselves** as an explicit "we don't
  modify your data" signal to the host; the bohpts reference impl does
  exactly that (R5). HikariCP and other mainstream pools reset the dirty
  `readOnly` flag back to the pool default on return, so the flag does not
  leak to subsequent host borrowers. The host-pool contract (R3) requires the
  pool to reset connection state on return,
  but the adapter does not depend on it for read-only correctness.
- **Host pool must size at least `entityCount` connections.** Each per-entity
  CDC tick borrows independently; entities scheduled to tick in the same window
  contend for connections. A pool sized for the host game-server's own
  concurrency alone (e.g. 2-4 connections for a small server) can deadlock the
  engine when every entity ticks together. Documented as a hard contract on
  Path-1 providers (R3) rather than enforced at the adapter level — sizing the
  host's pool is the host's decision; the adapter only states the minimum.
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
- **Pool stats** — host overrides the `stats()` default if its pool exposes any of
  `active` / `idle` / `total` / `waiting` counts (Hikari `getHikariPoolMXBean`, DBCP2
  `getNumActive` / `getNumIdle`, Tomcat JDBC `getActive` / `getIdle`, etc.). Default
  `Optional.empty()` is safe for pools that don't expose stats; partial coverage is
  fine — every `PoolStats` field is nullable.
- **Health probing** — host overrides `isHealthy()` (R8) when it has a richer view of
  pool health than "next `getConnection` succeeds". Useful for circuits where the pool
  knows it's down (e.g. a circuit-breaker around the underlying DataSource).
- **Future Tier-3 expansions** — if subsequent slices need additional SPI methods
  (transactional borrow, async / `CompletionStage` API, named connections for write
  scenarios when DML eventually lands), extend `JdbcConnectionSource` with `default`
  methods to preserve backward compat with existing host impls.
