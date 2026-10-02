# DB Sync — Tier-2 schema-provider discovery

> Sibling: [spec.md](./spec.md) (see its Technical design section)
> Audience: vanilla schema authors (`nx-gs-db-l2j`, `nx-gs-db-lucera` when those
> land), client overrides (the reference integration, future per-client schema variants).
>
> **For general SPI mechanics** (service-descriptor format, `ServiceLoader`
> internals, "Why ServiceLoader", common mistakes when authoring any tier),
> see [`adapter-modules/module-discovery.md`](../002-adapter-modules/module-discovery.md).
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
   │     host's own repo       │  ◄── private; ships as the host
   │     (private)             │      game-server JAR. No separate
   │                           │      per-client artifact.
   │  class HostDbSchema-      │
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
   │   │  host repo (refactored)   │                            │
   │   │  HostDbSchemaProvider     │                            │
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
    String schemaName();                 // "l2j", "lucera", "my-server"

    List<EntityMapping<?>> mappings();   // entities this provider knows about
}
```

`EntityMapping<T>` declares one `PrimarySource<?>` + zero-or-more
`ChildSource<?>` per entity (see [`db-sync` R5](./spec.md)). Every
identifier (`tableName`, `pkColumn`, `fkColumn`, every `hashedColumns`
entry) MUST match `^[A-Za-z_][A-Za-z0-9_]{0,63}$` — schema-qualified or
quoted names are rejected at engine start (see
[`cdc-engine` R19](../005-cdc-engine/spec.md)).

### Lifecycle

```
[DbSyncModule.start()]
       │
       ▼
[ServiceLoader.load(DbSchemaProvider.class)]   ◄─── Tier-2 discovery point
       │
       │   Same JDK machinery as Tier 1; descriptor file is:
       │     META-INF/services/app.l2nx.gs.adapter.api.spi.provider.DbSchemaProvider
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

### Service descriptor in the host JAR (MVP)

```
META-INF/
└── services/
    └── app.l2nx.gs.adapter.api.spi.provider.DbSchemaProvider
```

Content (single fully-qualified class name — package up to the host owner per spec
Open question; example below uses `com.example.host.nx.db`):

```
com.example.host.nx.db.HostDbSchemaProvider
```

(See [`adapter-modules/module-discovery.md`](../002-adapter-modules/module-discovery.md)
for general descriptor-format rules and common mistakes when authoring SPI impls.)

---

## MVP path — direct `DbSchemaProvider` impl (host-owned)

In MVP there is no vanilla L2J module. The host implements `DbSchemaProvider` directly.
The class lives in the host's own source tree (private repo)
alongside its existing entities; the host's
existing Gradle build produces the JAR that already contains the schema-provider class

+ service descriptor.

```java
package com.example.host.nx.db;   // example — package decision in spec Open question

import app.l2nx.gs.adapter.api.spi.provider.DbSchemaProvider;
import app.l2nx.gs.adapter.api.spi.model.EntityMapping;

import java.util.Collections;
import java.util.List;

public class HostDbSchemaProvider implements DbSchemaProvider {

    @Override
    public String schemaName() {
        return "my-server";
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

The host's `build.gradle.kts` adds (from Maven Central):

```kotlin
dependencies {
    implementation("app.l2nx:nx-gs-adapter-api:0.7.0")
    // Provider authors depend only on api; nx-gs-db-sync-core is the host's
    // classpath responsibility (engine runtime).
}
```

## Future path — vanilla → client override (template method)

Activated when a second customer with an L2J-family schema arrives and common L2J vanilla code is
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
// host repo (refactored once vanilla L2J ships)
package com.example.host.nx.db;

import app.l2nx.gs.db.l2j.L2jSchemaProvider;

public class HostDbSchemaProvider extends L2jSchemaProvider {

    @Override
    public String schemaName() {
        return "my-server";
    }

    // Host overrides go here. Currently no column overrides needed —
    // the host uses the same `clan_data` table and 4 plain cols as vanilla
    // would. Customizations land here as new host requirements appear.
}
```

---

## Operator classpath at runtime

### Scenario MVP — client operator

The host's game-server JAR is itself the
`DbSchemaProvider` carrier. No separate per-client JAR is on the classpath.

```
host classpath
├── host-X.Y.Z.jar                     ← provides DbSchemaProvider (host impl + descriptor)
├── nx-gs-adapter-core-X.Y.Z.jar       ← Tier-1 ServiceLoader
├── nx-gs-adapter-api-X.Y.Z.jar
├── nx-gs-kafka-X.Y.Z.jar
├── nx-gs-db-sync-core-X.Y.Z.jar       ← Tier-2 ServiceLoader; provides AdapterModule
├── HikariCP, mariadb-jdbc, gson, slf4j, ...
```

Discovery:

- Tier 1 → `[DbSyncModule]` (one impl, from `nx-gs-db-sync-core`)
- Tier 2 → `[HostDbSchemaProvider]` (one impl, from the host JAR) → engine starts ✓

### Scenario Future — vanilla L2J operator (no client customizations)

Activated once `nx-gs-db-l2j` is extracted and published. Another L2J operator
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

### Scenario Future — host (refactored to extend vanilla)

Once `nx-gs-db-l2j` ships, the host's `HostDbSchemaProvider` is refactored to
`extends L2jSchemaProvider`. The host's Gradle build adds
`implementation("app.l2nx:nx-gs-db-l2j:X.Y.Z")` for the inheritance.

```
host classpath
├── host-X.Y.Z.jar                     ← provides DbSchemaProvider (host override)
├── nx-gs-adapter-core-X.Y.Z.jar
├── nx-gs-adapter-api-X.Y.Z.jar
├── nx-gs-kafka-X.Y.Z.jar
├── nx-gs-db-sync-core-X.Y.Z.jar
├── nx-gs-db-l2j-X.Y.Z.jar             ← transitively brought by the host; classes used
│                                         for inheritance, BUT its descriptor would also
│                                         register the vanilla provider with ServiceLoader
├── HikariCP, mariadb-jdbc, gson, slf4j, ...
```

Discovery (open issue, **resolved at vanilla-extraction time**):

- Tier 1 → `[DbSyncModule]` ✓
- Tier 2 → **conflict if both descriptors are active.** The host ships a
  descriptor pointing to `HostDbSchemaProvider`. The transitively-pulled
  `nx-gs-db-l2j` ALSO ships a descriptor pointing to `L2jSchemaProvider`. Pure SPI
  sees both.

Three resolution strategies — **decision deferred to vanilla-extraction time** (see
[`spec.md`](./spec.md) Open questions); MVP has no conflict because vanilla doesn't
exist:

#### Strategy (a) — config selector

Operator sets `l2nx.db-sync.schema=my-server` in `l2nx.properties`. Engine compares
`schemaName()` of every discovered provider; picks the matching one.

- Pro: no build-time gymnastics required of client modules
- Con: extra config knob; operator can mis-spell schemaName and silently fall back to
  vanilla

#### Strategy (b) — shadow-jar service-descriptor exclusion

Client modules use the shadow plugin to exclude vanilla's descriptor and ship their
own:

```kotlin
// nx-gs-db-<client>/build.gradle.kts
shadowJar {
    exclude("META-INF/services/app.l2nx.gs.adapter.api.spi.provider.DbSchemaProvider")
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
- Client deployment → `nx-gs-db-<client>` (which pulls `nx-gs-db-l2j` for classes only)

- Pro: no build-time gymnastics in client modules; no config knob
- Con: extra published artifact per vanilla module
