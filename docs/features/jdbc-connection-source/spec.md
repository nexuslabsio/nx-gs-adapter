# JDBC Connection Source

## Problem

DB-reading adapter modules (`db-sync` first, future `metrics`/etc.) need JDBC `Connection`s
to query the host game-server's MySQL instance. The naive design — bundle HikariCP with the
adapter and open a second pool from `ConnectResponse.database` creds — has three real
problems on host JVMs we don't control:

1. **Classpath conflicts** — most L2J / Lucera / Essence cores already include HikariCP
   (often a different major version) or Apache DBCP2 in their classpath. Two HikariCP JARs
   = `LinkageError` / wrong-version-loaded surprises. HikariCP 4.x+ requires Java 11; the
   adapter targets Java 8 hosts. Forcing a specific Hikari version violates the "min deps"
   architectural principle.
2. **Wasted resources** — opening a second pool means doubling TCP connections to MySQL,
   doubling auth handshakes, doubling validation overhead, for state the host already has
   open.
3. **Encapsulation breaches** — accessing the host's pool via reflection (e.g. peeking at
   bohpts' `DatabaseFactory.dataSource` private field) couples the adapter to one specific
   implementation and breaks the moment the host refactors. Direct `import com.zaxxer.hikari.*`
   in adapter code couples it to Hikari forever.

This slice introduces a Tier-3 SPI (`JdbcConnectionSource`) where the host JVM optionally
exposes a factory for `Connection`s, plus a builtin **bundled-Hikari fallback** when no
SPI impl is registered. Two deployment paths cover both source-access scenarios; **DB
credentials never travel through the platform** in either case (operator-side only):

- **Path 1 — host-registered SPI** (preferred when source-access is available): host
  implements `JdbcConnectionSource`, returns connections from its existing game-core
  pool (e.g. bohpts wraps `DatabaseFactory.getInstance().getConnection()` in a 5-line
  class). Adapter discovers via ServiceLoader and borrows. **Zero credential
  duplication** — host's pool already knows them; nothing in `l2nx.properties`,
  nothing on the platform.
- **Path 2 — bundled Hikari fallback**: when no `JdbcConnectionSource` impl is on
  classpath, the adapter creates its own shadowed-Hikari pool from creds in
  `l2nx.properties` (`l2nx.db.url` / `l2nx.db.username` / `l2nx.db.password`).
  Operator-local secret stays on the operator's machine. For clients without source
  access to the host JVM (closed-source distributions, third-party resellers, etc.).

The `ConnectResponse.database` field is intentionally absent from the wire contract — the
platform never carries DB creds. Operators with source access pick Path 1; everyone else
gets Path 2 by default.

Audience: host-JVM authors who choose to implement `JdbcConnectionSource` once per
game-server distribution (bohpts, vanilla L2J / Lucera when those land); operators
deploying closed-source distributions who configure `l2nx.db.*` locally; db-sync (and
future DB-reading modules) consume the resolved `JdbcConnectionSource` transparently.

## Requirements

**Must:**

- [done] R1. `nx-gs-adapter-api` MUST expose a Tier-3 SPI `JdbcConnectionSource` in
  package `app.l2nx.gs.adapter.api.spi` (alongside Tier-1 SPI types — single api
  artifact for every SPI tier; no separate `nx-gs-jdbc-connection-source-api`
  artifact):
    - `String name()` — provider identifier for logging (e.g. `"bohpts-hikari"`,
      `"l2j-vanilla-dbcp2"`); not a selection key in MVP, informational only
    - `Connection getConnection() throws SQLException` — borrow a connection. Caller
      closes via `try-with-resources` to return it to the host's pool.

- [wip] R2. `nx-gs-db-sync-core` MUST resolve a `JdbcConnectionSource` instance during
  `DbSyncModule.onConnect(...)` via the following priority chain — host SPI wins over
  fallback, fallback wins over failure:
    1. **[Phase 1]** `ServiceLoader.load(JdbcConnectionSource.class)` finds **exactly
       1** impl → use it (Path 1 — host explicitly registered their pool).
    2. **[Phase 3]** ServiceLoader finds **0** impls AND `l2nx.db.url` +
       `l2nx.db.username` + `l2nx.db.password` are all present in the config chain →
       instantiate `BundledHikariConnectionSource` from those creds (Path 2 —
       fallback). Optional `l2nx.db.pool-size` (default 4); see R6.
    3. **[Phase 1]** ServiceLoader finds **0** impls (Phase 3 will add the Path 2
       fallback before this step) → log actionable ERROR ("no JdbcConnectionSource
       SPI registered; register one via `META-INF/services` — see
       jdbc-connection-source feature docs"), db-sync `FAILED`. Host JVM keeps
       running.
    4. **[Phase 1]** ServiceLoader finds **>1** impls → log actionable ERROR listing
       conflicting impl class names, db-sync `FAILED`. Multi-impl resolution (config
       selector / shadow exclusion / activator JAR) deferred to second-customer time,
       same as Tier-2.

  Note: when a host SPI impl is registered, `l2nx.db.*` keys (if present) are ignored.
  The host's explicit registration is a stronger signal than fallback config — easier
  to reason about than "config wins, SPI wins" tie-breaking.

- [done] R3. **Host-pool contract.** The pool exposed via the SPI is host-owned;
  the adapter does NOT impose connection-level configuration. Specifically:
    - The adapter does NOT call `Connection.setReadOnly(true)` on borrowed
      connections. CDC engine enforces read-only at the SQL level via
      `START TRANSACTION ... READ ONLY` (see
      [`cdc-engine` R11](../cdc-engine/spec.md)). Providers MAY call
      `Connection.setReadOnly(true)` themselves as defense-in-depth (explicit
      "adapter doesn't modify host data" signal) — the bohpts reference
      `BohptsJdbcConnectionSource` does so. This is safe with mainstream
      pools (HikariCP, DBCP2) that reset the dirty `readOnly` flag on
      connection return; verify your pool's reset semantics before adopting
      the pattern. Providers MUST NOT assume the adapter itself will enforce
      read-only at the connection level.
    - The host pool MUST reset connection-level state (`readOnly`,
      `autoCommit`, isolation level, schema, etc.) on connection return.
      Adapter-side code mutates `autoCommit` while wrapping queries in
      `START TRANSACTION ... READ ONLY`; the pool is responsible for
      restoring defaults so subsequent borrowers see a clean connection.
    - The host pool MUST size at least `entityCount` connections. Each
      per-entity tick borrows independently from the pool (one borrow per
      tick per entity, returned at end-of-tick); a pool sized for the
      host-game-server's own concurrency alone can deadlock the CDC engine
      when every entity is scheduled to tick in the same window.
    - A `GRANT SELECT`-only MySQL user is still recommended as
      defence-in-depth.

- [done] R4. Consumers MUST close the borrowed `Connection` via `try-with-resources` (or
  equivalent finally-close) at the end of every query. Provider impls do NOT need to
  track per-borrow lifecycle — the host's pool reclaims connections on close.

- [done] R5. **[Phase 1]** Bohpts reference impl (Path 1) — `bohpts-core` (private
  repo; `E:/bohpts/code/bohpts-core`) hosts a `BohptsJdbcConnectionSource` class
  implementing `JdbcConnectionSource`, returning
  `DatabaseFactory.getInstance().getConnection()`, plus a
  `META-INF/services/app.l2nx.gs.adapter.api.spi.JdbcConnectionSource` resource
  pointing to it. Bohpts-core declares `implementation 'app.l2nx:nx-gs-adapter-api:X.Y.Z'`
  (Maven Central) — that artifact carries the Tier-3 SPI interface. The package for
  `BohptsJdbcConnectionSource` inside bohpts-core is operator-chosen — see Open
  questions.
    - SC1. The reference impl body is a single line:
      `return DatabaseFactory.getInstance().getConnection();`. If the body grows beyond
      that, something is wrong with the SPI contract.

- [todo] R6. **[Phase 3]** `BundledHikariConnectionSource` (Path 2 fallback) MUST:
    - Be a builtin class inside `nx-gs-db-sync-core`, NOT registered via
      `META-INF/services` (instantiated only by R2's resolution chain, not discovered
      via ServiceLoader).
    - Use a **shadowed Hikari 3.4.5** runtime — `com.zaxxer.hikari.*` packages are
      relocated to `app.l2nx.shaded.hikari.*` via shadowJar at build time, so the
      bundled pool cannot collide with whatever HikariCP version (or Apache DBCP2 /
      c3p0 / etc.) the host JVM ships. Hikari 3.4.5 is the last Java-8-compatible
      release; bytecode major version 52.
    - Read creds via `ConfigResolver` (same chain as `gs-key`/`platform-url`): file-first
      `l2nx.db.url`, `l2nx.db.username`, `l2nx.db.password`. Optional
      `l2nx.db.pool-size` (default 4).
    - Configure the Hikari pool with `setReadOnly(true)`, `setMaximumPoolSize(N)`,
      `setConnectionTimeout(5000)`, `setValidationTimeout(5000)`,
      `setLeakDetectionThreshold(30000)`, `setPoolName("nx-adapter-db")`.
    - Open the pool synchronously on `DbSyncModule.onConnect`; close it on
      `DbSyncModule.onDisconnect`.
    - DB credentials NEVER travel through the platform — the wire contract carries no
      DB creds field at all.

**Should:**

- [wip] R7. **[Phase 1]** `JdbcConnectionSource` SHOULD expose a default
  `Optional<PoolStats> stats()` method (default impl returns `Optional.empty()`) so
  `db-sync`'s heartbeat enrichment (R12 in db-sync spec) can carry pool active / idle /
  total / waiting counts when the host's pool exposes them. Bohpts impl reads
  `DatabaseFactory.getBusyConnectionCount` (mapped to `PoolStats.active`) /
  `getIdleConnectionCount` (mapped to `PoolStats.idle`); `total` and `waiting` left null
  because bohpts `DatabaseFactory` does not expose those. Vanilla impls without any
  stats just return `Optional.empty()`. Default method preserves backward compat —
  existing impls don't break when the method is added. `PoolStats` lives in
  `app.l2nx.gs.adapter.api.kafka.ops` (shared with `ModuleStatus.Stats.pool`).
  **api/0.6.0 wire-shape change**: field `busy` renamed to `active` (HikariCP / Tomcat
  JDBC / DBCP2 convention); new `waiting` field added for pool-backpressure
  diagnostics. All `PoolStats` fields are nullable (`Integer`, not `int`) so providers
  expose only the counters they have.

**Could:**

- [done] R8. `JdbcConnectionSource` COULD expose a default `boolean isHealthy()` method
  (default `true`) so consumers can skip a tick when the pool is known to be down,
  avoiding spew of `SQLException` on broken connections. Useful when the host has a more
  detailed view of pool health than "next getConnection succeeds or fails".

**Non-goals:**

- **DB credentials on the platform** — `ConnectResponse.database` is dropped from the
  wire contract. Operators do NOT enter DB creds in the platform UI; the platform never
  receives, stores, or transmits DB passwords. Path 1 reuses host's existing in-process
  creds (no transmission); Path 2 reads operator-local `l2nx.properties`. Defense-in-depth
  with a `GRANT SELECT`-only MySQL user is still recommended.
- **JDBC driver bundling** — host provides the driver (game core needs it anyway). MariaDB
  / MySQL drivers do NOT ship in the adapter artifacts.
- **Pool implementation choice in Path 1** — host's pool is whatever the host chose
  (HikariCP, DBCP2, custom singleton). Adapter is agnostic.
- **Pool sizing / timeouts in Path 1** — host owns those. Adapter does not impose.
- **Multi-impl resolution** — single-impl assumption per R2; >1 → fail-loud. Resolution
  strategies (config selector / shadow exclusion / activator JAR) deferred to
  second-customer time.
- **Per-borrow validation hooks** — `try-with-resources` close is the only contract.
  Connection liveness probes are the host's pool's responsibility.
- **Connection retry / backoff at the SPI layer** — consumers (db-sync engine) handle
  their own retry policies; the SPI just throws `SQLException` and lets the consumer
  decide.
- **`DataSource` interface** — `javax.sql.DataSource` has 7+ methods (`getConnection()`,
  `getConnection(String, String)`, `getLogWriter`, `setLogWriter`, `getLoginTimeout`,
  `setLoginTimeout`, `getParentLogger`, `unwrap`, `isWrapperFor`). Forcing host impls to
  fill all of them is hostile for the simple "give me a Connection" use case. We choose
  a minimal SPI instead.
- **Forcing operators to duplicate creds** — operators with source access pick Path 1
  and avoid duplication entirely. Operators without source access necessarily duplicate
  in `l2nx.properties` — that's irreducible when no SPI hook is available.

## Open questions

- [resolved: SPI interface lives in `nx-gs-adapter-api` package
  `app.l2nx.gs.adapter.api.spi` — single api artifact carries every SPI tier
  (Tier-1 `AdapterModule`, Tier-3 `JdbcConnectionSource`). Future DB-reading modules
  depend only on `nx-gs-adapter-api`, not on `nx-gs-db-sync-core`.]
- [resolved: Provider takes no parameter at construction. With `ConnectResponse.database`
  removed from the wire, there's no platform-delivered creds for the SPI to forward.
  Path-1 providers fish from host static state (`DatabaseFactory.getInstance()`); Path-2
  fallback reads `l2nx.db.*` from `ConfigResolver` directly inside
  `BundledHikariConnectionSource`. No `init(ctx)` lifecycle needed.]
- [NEEDS CLARIFICATION: Java package for `BohptsJdbcConnectionSource` inside bohpts-core
  source tree. Candidates: `l2e.gameserver.nx.db` (matches bohpts-core convention),
  `app.l2nx.gs.db.bohpts` (uses L2NX namespace inside the bohpts JAR), `com.bohpts.gs.l2nx`.
  Up to bohpts-core owner; no impact on the SPI contract. Same Open question as
  `BohptsDbSchemaProvider` in db-sync — keep both packages aligned.]
- [assumed: Bohpts MVP picks **Path 1** (host SPI, shared pool via `DatabaseFactory`) —
  bohpts dev has source access, no creds duplication wins. Path 2 fallback exists for
  closed-source distributions of bohpts to third parties.]
- [assumed: Single-impl on classpath in MVP — bohpts-core ships exactly one
  `JdbcConnectionSource` descriptor; vanilla L2J / Lucera modules will ship their own
  when they land but never alongside bohpts-core in the same deployment.]
- [assumed: Default `stats()` returns `Optional.empty()` if the host's pool doesn't
  expose stats — adapter heartbeat enrichment treats this as "stats unavailable",
  doesn't fail the heartbeat.]

## Links

- Sibling feature (Tier-1 SPI): [`docs/features/adapter-bootstrap/spec.md`](../adapter-bootstrap/spec.md)
- Sibling feature (Tier-2 SPI + first consumer):
  [`docs/features/db-sync/spec.md`](../db-sync/spec.md) — db-sync borrows connections
  through this SPI
- Bohpts reference for `DatabaseFactory`:
  `E:/bohpts/code/bohpts-core/core/src/main/java/l2e/gameserver/database/DatabaseFactory.java`
