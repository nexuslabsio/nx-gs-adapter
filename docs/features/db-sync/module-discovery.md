# DB Sync — Tier-2 schema-provider discovery

> Sibling: [spec.md](./spec.md), [tech.md](./tech.md)
> Audience: vanilla schema authors (`nx-gs-db-l2j`, `nx-gs-db-lucera` when those
> land), client overrides (bohpts, future per-client schema variants).
>
> **For general SPI mechanics** (service-descriptor format, `ServiceLoader`
> internals, "Why ServiceLoader", common mistakes when authoring any tier),
> see [`adapter-modules/module-discovery.md`](../adapter-modules/module-discovery.md).
> This doc focuses on Tier-2 specifics: `DbSchemaProvider` discovery, the vanilla
> → client template-method override pattern, and operator-classpath scenarios.

## Tier-2 picture (this feature's slice)

Tier 1 (`AdapterModule`) is delivered by `adapter-modules`; `nx-gs-db-sync-core` is one
of its consumers. Internally, `DbSyncModule` adds a second SPI tier
(`DbSchemaProvider`) that schema variants per game-server fork plug into:

```
   ┌──────────────────────────────────────┐
   │   nx-gs-db-sync-core                 │
   │                                      │
   │   class DbSyncModule                 │
   │     implements AdapterModule         │ ◄─── consumed by adapter-modules (Tier 1)
   │                                      │
   │   public interface DbSchemaProvider  │ ◄─── this doc (Tier 2 SPI, defined in
   │     String schemaName();             │      nx-gs-adapter-api so providers
   │     List<EntityMapping<?>> mappings()│      depend only on api)
   │   }                                  │
   └──────────────┬───────────────────────┘
                  ▲
                  │ implements (MVP path — direct)
                  │
   ┌──────────────┴────────────┐
   │     bohpts-core repo      │  ◄── private; ships as the host
   │     (private)             │      game-server JAR. No separate
   │                           │      nx-gs-db-bohpts artifact.
   │  class BohptsDbSchema-    │
   │  Provider implements      │
   │    DbSchemaProvider       │
   └───────────────────────────┘

   ┌─── Future, after a 2nd customer arrives ──────────────────┐
   │                                                            │
   │   ┌───────────────────────────┐                            │
   │   │     nx-gs-db-l2j          │                            │
   │   │     (vanilla, future)     │                            │
   │   │                           │                            │
   │   │  class L2jSchemaProvider  │ implements                 │
   │   │    DbSchemaProvider       │                            │
   │   └──────────────┬────────────┘                            │
   │                  ▲                                         │
   │                  │ extends (template method)               │
   │                  │                                         │
   │   ┌──────────────┴────────────┐                            │
   │   │  bohpts-core (refactored) │                            │
   │   │  BohptsDbSchemaProvider   │                            │
   │   │    extends                │                            │
   │   │    L2jSchemaProvider      │                            │
   │   └───────────────────────────┘                            │
   │                                                            │
   └────────────────────────────────────────────────────────────┘
```

**Tier 2** is **internal to the DB-sync stack** — `DbSchemaProvider` impls describe
schemas (table names, PK columns, hashed columns, mapRow lambdas) for specific
game-server forks. Discovered by `nx-gs-db-sync-core` once when `DbSyncModule.start()`
runs.

---

## Tier 2 — `DbSchemaProvider` discovery

### Interface (in `nx-gs-adapter-api`)

```java
package app.l2nx.gs.adapter.api.spi;

public interface DbSchemaProvider {
    String schemaName();                 // "l2j", "bohpts", "lucera"

    List<EntityMapping<?>> mappings();   // entities this provider knows about
}
```

`EntityMapping<T>` declares one `PrimarySource<?>` + zero-or-more
`ChildSource<?>` per entity (see [`db-sync` R5](./spec.md)). Every
identifier (`tableName`, `pkColumn`, `fkColumn`, every `hashedColumns`
entry) MUST match `^[A-Za-z_][A-Za-z0-9_]{0,63}$` — schema-qualified or
quoted names are rejected at engine start (see
[`cdc-engine` R19](../cdc-engine/spec.md)).

### Lifecycle

```
[DbSyncModule.start()]
       │
       ▼
[ServiceLoader.load(DbSchemaProvider.class)]   ◄─── Tier-2 discovery point
       │
       │   Same JDK machinery as Tier 1; descriptor file is:
       │     META-INF/services/app.l2nx.gs.adapter.api.spi.DbSchemaProvider
       ▼
   ┌────────────────────────┐
   │  providers.size()      │
   ├────────────────────────┤
   │   0 → state = DISABLED │  WARN: "no DbSchemaProvider on classpath; nothing to sync"
   │   1 → use it           │  ◄── dominant case
   │  >1 → state = FAILED   │  ERROR: "multiple providers found: [fqcn1, fqcn2]"
   └────────────────────────┘
       │
       ▼ (size == 1)
[CdcEngine constructed with the provider]
       │
       ▼
[CdcEngine validates every SQL identifier against the bare-identifier
 regex (^[A-Za-z_][A-Za-z0-9_]{0,63}$). Violation → STATE_FAILED.]
       │
       ▼
[shared pool nx-cdc-pool-<schema>-N is built (size = l2nx.cdc-engine.workers);
 for each EntityMapping in provider.mappings():
       pool.scheduleWithFixedDelay(SafeRunnable.wrap(task),
                                    0, cfg.tickInterval, ...)]
```

### Service descriptor in `bohpts-core` JAR (MVP)

```
META-INF/
└── services/
    └── app.l2nx.gs.adapter.api.spi.DbSchemaProvider
```

Content (single fully-qualified class name — package up to bohpts-core owner per spec
Open question; example below uses `l2e.gameserver.nx.db`):

```
l2e.gameserver.nx.db.BohptsDbSchemaProvider
```

(See [`adapter-modules/module-discovery.md`](../adapter-modules/module-discovery.md)
for general descriptor-format rules and common mistakes when authoring SPI impls.)

---

## MVP path — direct `DbSchemaProvider` impl (bohpts-core)

In MVP there is no vanilla L2J module. Bohpts implements `DbSchemaProvider` directly.
The class lives in `bohpts-core` source tree (private repo,
`E:/bohpts/code/bohpts-core`) alongside the existing JPA entities; bohpts-core's
existing Gradle build produces the JAR that already contains the schema-provider class

+ service descriptor.

```java
package l2e.gameserver.nx.db;   // example — package decision in spec Open question

import app.l2nx.gs.adapter.api.spi.DbSchemaProvider;
import app.l2nx.gs.adapter.api.spi.EntityMapping;

import java.util.Collections;
import java.util.List;

public class BohptsDbSchemaProvider implements DbSchemaProvider {

    @Override
    public String schemaName() {
        return "bohpts";
    }

    @Override
    public List<EntityMapping<?>> mappings() {
        return Collections.singletonList(new ClanMapping());
    }
}
```

`ClanMapping` declares a `PrimarySource<ClanRow>` over `clan_data`
(`clan_id` PK, hashedColumns = `clan_name`, `clan_level`, `leader_id`,
`ally_id`) and assembles `ClanDbDto` via `mapEntity`. Operational tuning
(tickInterval, rowsPerWindow, queryTimeout, fetchSize, workers) is NOT on
the mapping — it lives in `l2nx.properties` under `l2nx.cdc-engine.*`
(see [`db-sync` spec](./spec.md) for the full key table).

bohpts-core `build.gradle.kts` adds (from Maven Central):

```kotlin
dependencies {
    implementation("app.l2nx:nx-gs-adapter-api:0.7.0")
    // Provider authors depend only on api; nx-gs-db-sync-core is the host's
    // classpath responsibility (engine runtime).
}
```

## Future path — vanilla → client override (template method)

Activated when a second non-bohpts customer arrives and common L2J vanilla code is
extracted into `nx-gs-db-l2j`. Clients override `protected` hooks instead of writing
whole new providers.

```java
// nx-gs-db-l2j (future, vanilla — published to Maven Central)
package app.l2nx.gs.db.l2j;

public class L2jSchemaProvider implements DbSchemaProvider {

    @Override
    public String schemaName() {
        return "l2j";
    }

    // Template-method hooks — clients override these surgically.
    protected String clanTable() {
        return "clan_data";
    }

    protected String clanPkColumn() {
        return "clan_id";
    }

    protected List<String> clanHashedColumns() {
        return Arrays.asList("clan_name", "clan_level", "leader_id", "ally_id");
    }

    @Override
    public List<EntityMapping<?>> mappings() {
        return Arrays.asList(
                new ClanMapping(clanTable(), clanPkColumn(), clanHashedColumns())
        );
    }
}
```

```java
// bohpts-core (refactored once vanilla L2J ships)
package l2e.gameserver.nx.db;

import app.l2nx.gs.db.l2j.L2jSchemaProvider;

public class BohptsDbSchemaProvider extends L2jSchemaProvider {

    @Override
    public String schemaName() {
        return "bohpts";
    }

    // Bohpts overrides go here. Currently no column overrides needed —
    // bohpts uses the same `clan_data` table and 4 plain cols as vanilla
    // would. Customizations land here as new bohpts requirements appear.
}
```

---

## Operator classpath at runtime

### Scenario MVP — bohpts client operator

The bohpts game-server JAR (built from `bohpts-core` repo) is itself the
`DbSchemaProvider` carrier. No separate `nx-gs-db-bohpts` JAR is on the classpath.

```
host classpath
├── bohpts-core-X.Y.Z.jar              ← provides DbSchemaProvider (bohpts impl + descriptor)
├── nx-gs-adapter-core-X.Y.Z.jar       ← Tier-1 ServiceLoader
├── nx-gs-adapter-api-X.Y.Z.jar
├── nx-gs-kafka-X.Y.Z.jar
├── nx-gs-db-sync-core-X.Y.Z.jar       ← Tier-2 ServiceLoader; provides AdapterModule
├── HikariCP, mariadb-jdbc, gson, slf4j, ...
```

Discovery:

- Tier 1 → `[DbSyncModule]` (one impl, from `nx-gs-db-sync-core`)
- Tier 2 → `[BohptsDbSchemaProvider]` (one impl, from `bohpts-core`) → engine starts ✓

### Scenario Future — vanilla L2J operator (no client customizations)

Activated once `nx-gs-db-l2j` is extracted and published. A non-bohpts L2J operator
would deploy:

```
host classpath
├── l2j-server.jar
├── nx-gs-adapter-core-X.Y.Z.jar       ← Tier-1 ServiceLoader
├── nx-gs-adapter-api-X.Y.Z.jar
├── nx-gs-kafka-X.Y.Z.jar
├── nx-gs-db-sync-core-X.Y.Z.jar       ← Tier-2 ServiceLoader; provides AdapterModule
├── nx-gs-db-l2j-X.Y.Z.jar             ← provides DbSchemaProvider (vanilla)
├── HikariCP, mariadb-jdbc, gson, slf4j, ...
```

Discovery:

- Tier 1 → `[DbSyncModule]` ✓
- Tier 2 → `[L2jSchemaProvider]` ✓

### Scenario Future — bohpts (refactored to extend vanilla)

Once `nx-gs-db-l2j` ships, bohpts-core's `BohptsDbSchemaProvider` is refactored to
`extends L2jSchemaProvider`. bohpts-core's Gradle build adds
`implementation("app.l2nx:nx-gs-db-l2j:X.Y.Z")` for the inheritance.

```
host classpath
├── bohpts-core-X.Y.Z.jar              ← provides DbSchemaProvider (bohpts override)
├── nx-gs-adapter-core-X.Y.Z.jar
├── nx-gs-adapter-api-X.Y.Z.jar
├── nx-gs-kafka-X.Y.Z.jar
├── nx-gs-db-sync-core-X.Y.Z.jar
├── nx-gs-db-l2j-X.Y.Z.jar             ← transitively brought by bohpts-core; classes used
│                                         for inheritance, BUT its descriptor would also
│                                         register the vanilla provider with ServiceLoader
├── HikariCP, mariadb-jdbc, gson, slf4j, ...
```

Discovery (open issue, **resolved at vanilla-extraction time**):

- Tier 1 → `[DbSyncModule]` ✓
- Tier 2 → **conflict if both descriptors are active.** `bohpts-core` ships a
  descriptor pointing to `BohptsDbSchemaProvider`. The transitively-pulled
  `nx-gs-db-l2j` ALSO ships a descriptor pointing to `L2jSchemaProvider`. Pure SPI
  sees both.

Three resolution strategies — **decision deferred to vanilla-extraction time** (see
[`spec.md`](./spec.md) Open questions); MVP has no conflict because vanilla doesn't
exist:

#### Strategy (a) — config selector

Operator sets `l2nx.db-sync.schema=bohpts` in `l2nx.properties`. Engine compares
`schemaName()` of every discovered provider; picks the matching one.

- Pro: no build-time gymnastics required of client modules
- Con: extra config knob; operator can mis-spell schemaName and silently fall back to
  vanilla

#### Strategy (b) — shadow-jar service-descriptor exclusion

Client modules use the shadow plugin to exclude vanilla's descriptor and ship their
own:

```kotlin
// nx-gs-db-bohpts/build.gradle.kts
shadowJar {
    exclude("META-INF/services/app.l2nx.gs.adapter.api.spi.DbSchemaProvider")
    // own descriptor lives in src/main/resources/META-INF/services/...
}
```

- Pro: single descriptor on classpath at runtime; no config knob
- Con: every client module adopts shadow + remembers the exclusion glob

#### Strategy (c) — vanilla activator JAR pattern

Vanilla `nx-gs-db-l2j` JAR ships **classes only**, no service descriptor. A separate
tiny `nx-gs-db-l2j-default` JAR carries the descriptor pointing to
`L2jSchemaProvider`. Operators pick:

- Vanilla deployment → `nx-gs-db-l2j` + `nx-gs-db-l2j-default`
- Client deployment → `nx-gs-db-bohpts` (which pulls `nx-gs-db-l2j` for classes only)

- Pro: no build-time gymnastics in client modules; no config knob
- Con: extra published artifact per vanilla module
