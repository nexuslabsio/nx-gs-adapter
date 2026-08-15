# nx-gs-adapter-api

Subproject of the `nx-gs-adapter` monorepo. See [`../CLAUDE.md`](../CLAUDE.md) for repo-wide
conventions (per-module versioning, Maven Central publishing, the two-release rule for breaking wire
changes, license).

This file is a **map plus the invariants**. It names packages and the contracts that are easy to get
wrong; it deliberately does not enumerate DTO fields — the classes are the source of truth and the
design rationale lives in `docs/specs/` (index: [`../docs/CLAUDE.md`](../docs/CLAUDE.md)). A prose
copy of the wire shape drifts silently; this one already had.

## Purpose

Contracts-only artifact: pure Java interfaces and POJOs defining the wire shape exchanged by the
L2NX game-server adapter and its consumers. Published as `app.l2nx:nx-gs-adapter-api`. Consumed by
`:nx-gs-adapter-core` and the sync modules on the adapter side, by `nx-tenants` (`/connect` handler)
and `nx-gameservers` / `nx-gamedata` on the platform side, and by host forks implementing the Tier-2
provider SPIs.

## Package map

Root package `app.l2nx.gs.adapter.api`.

| package                       | what lives there                                                                                                                  | design     |
| ----------------------------- | --------------------------------------------------------------------------------------------------------------------------------- | ---------- |
| `rest`                        | `/connect` request/response, Kafka credentials, `SyncTopics`, `MessagingTopics`                                                   | 001        |
| `kafka`                       | `NxHeaders` — the wire-level header contract                                                                                      | 008        |
| `kafka.sync.db.<entity>`      | CDC per-entity DTOs                                                                                                               | 003, 005   |
| `kafka.sync.gd.<entity>`      | static game-data catalog DTOs, payload of `GameDataSyncEvent<T>`                                                                  | 030        |
| `kafka.sync.runtime.<entity>` | volatile runtime-state DTOs                                                                                                       | 006        |
| `kafka.events.<family>`       | outbound discrete-fact / snapshot event DTOs, grouped by family                                                                   | per family |
| `kafka.commands.<group>`      | inbound command DTOs; the package root holds `NxCommand`, `CommandResult`, `CommandStatus` (+ nested `Tier`) and `CommandProblem` | 009        |
| `kafka.ops`                   | heartbeat and telemetry payloads (`ModuleStatus`, `EntityStats`, …)                                                               | 001        |
| `spi`                         | the SPI tiers + capability interfaces (see below)                                                                                 | 002        |

Current entity / family / group names (the directory listing is authoritative — check it before
assuming):

- **db sync**: `alliance`, `announcement`, `ban`, `character`, `clan`, `item`, `rating`
- **gd sync**: `armorsettemplate`, `classtemplate`, `gearscore`, `instancetemplate`, `itemtemplate`,
  `npctemplate`, `recipetemplate`, `skill`, `soulcrystaltemplate`
- **runtime sync**: `character`
- **events**: `account`, `castle`, `character`, `chat`, `gameevents`, `leveldata`, `mail`,
  `olympiad`, `premiumpurchase`, `privatestore`, `privatetrade`, `raid`, `schedule`, `serveronline`,
  `sync`
- **commands**: `announcement`, `ban`, `character`, `gd`, `item`, `mail`, `privatestore`, `sync`,
  `telegram`

The command group is a code-organization bucket only — the commands topic stays single.

## SPI tiers

- **Tier-1** `AdapterModule` — a module plugged into adapter-core via `ServiceLoader`.
- **Tier-2** — the per-domain provider SPIs a host implements: `DbSchemaProvider` (+ `EntityMapping`,
  `PrimarySource`, `ChildSource`, `ParentRef`), `RuntimeStateProvider` (+ `RuntimeEntityMapping`,
  `RuntimeRow`), and the gd catalog providers (`ItemTemplateProvider`, `SkillProvider`, …, plus the
  singleton `GearScoreRulesetProvider` and the optional `GameDataReadinessProvider` — absent means
  "always ready", see spec 030).
- **Tier-3** `JdbcConnectionSource` — how a host hands the adapter a pooled connection.
- **Capabilities** handed to modules through `ConnectContext` / `CommandContext`: `NxEvents`,
  `NxCommands` (+ `CommandHandler`, `HostExecutor`), `NxSync`, `NxGameData`. Each ships a `NoOp*`
  fallback so a context wired without that runtime never returns null.

They all live here so a host provider depends on the contracts artifact alone.

## Contracts worth calling out

- **Timestamps are UTC `Instant` only.** Every timestamp field in every wire DTO (`kafka.sync.*`,
  `kafka.events.*`, `kafka.commands.*`, `kafka.ops.*`, `rest.*`) MUST be `java.time.Instant` — never
  `OffsetDateTime`, `ZonedDateTime`, `LocalDateTime`, `LocalDate`, `LocalTime`, `java.util.Date`,
  `java.util.Calendar`, or `java.sql.{Date,Time,Timestamp}`. The platform operates strictly on UTC;
  any zoned/local type risks a host-timezone leak. Schema providers read source columns through
  `JdbcNulls.nullableInstantFromEpochMillis(...)` / `instantFromEpochMillisOrSentinel(...)` (in
  `:nx-gs-commons`), not `rs.getTimestamp(...).toLocalDateTime(...)`. Enforced at build time by
  `WireTimestampConformanceTest` (reflective classpath scan — the build fails on any violation).
- **SQL identifiers are validated, not quoted.** Anything passed via `EntityMapping` /
  `PrimarySource` / `ChildSource` (tableName / pkColumn / fkColumn / hashedColumns) MUST match
  `^[A-Za-z_][A-Za-z0-9_]{0,63}$`. Schema-qualified names, quoted identifiers, and anything outside
  that pattern are rejected at engine start — no runtime escaping is performed.
- **`SyncEvent` `DELETED` carries `payload=null`, and that is not a Kafka tombstone.** Topics use
  bounded retention, not log compaction, so consumers MUST handle the `DELETED` op explicitly rather
  than relying on the null-value tombstone protocol.
- **`CharacterRuntimeDto` must keep exactly ONE constructor.** It binds through implicit
  constructor-parameter names; adding an overload (even a back-compat one) makes creator detection
  ambiguous and consumers silently stop deserializing the whole channel.
- **Presence is encoded by omission.** `CharacterRuntimeDto.online`: `null`/omitted = ONLINE (byte
  budget), explicit `false` = one-shot OFFLINE tombstone, explicit `true` is allowed but redundant.
  Consumers MUST read omitted/`null` as `true`.
- **`CharacterDbDto.classes` is the full roster, and it does not say which class is active.** Exactly
  one entry is `MAIN`; the class being played is `CharacterDbDto.classId`. Per-class `exp` / `sp` are
  deliberate UNHASHED ride-alongs — read in `mapRow` but absent from `hashedColumns()`, because they
  tick on every kill — so they never trigger a sync event and are only as fresh as the source's last
  full store. The runtime channel carries the live values.
- **Partition keys are per event TYPE, not per family.** A family can mix keyed and round-robin
  types (`raid`: `RaidKillEvent` keys on `bossNpcId`, `BossRespawnSnapshotEvent` uses `null`). The
  authoritative mapping is one `register(...)` line per type in adapter-core's `EventTypeRegistry` —
  read it there; the same file also holds the `Nx-Message-Type` value used for polymorphic dispatch.
- **A family's topic is not implied by its Java package.** `events.leveldata.LevelExpTableSnapshotEvent`
  rides the `character` topic — a table synced once per boot did not justify its own
  topic/consumer/group.
- **`SNAPSHOT_COMPLETE count=0` deletes a catalog.** A gd provider with nothing to give returns
  `null` (burst aborted, nothing reconciled), never an empty collection. See spec 030 §2.
- **`ConnectContext.io()` / `CommandContext.io()` (binary-breaking for external implementers).**
  `CommandContext.io()` is abstract — any external implementer (test doubles, alternate adapters)
  MUST implement it. `ConnectContext` gained an `io` field and now has a 10-arg canonical constructor
  with the 9-arg one preserved for positional callers. Blocking IO (JDBC, HTTP) MUST hop onto these
  executors. Sanctioned exception: the db-sync resync handlers resolve cascades synchronously because
  the ack needs the counts, bounded by `ResyncRowsCommand.MAX_PKS`.

## Conventions

- **Enum-like open-string fields are `UPPER_SNAKE_CASE`**; free-form identifiers (effect-handler
  names, icon paths, localized text) stay verbatim. Icon names are emitted as-is — the platform
  lowercases them consumer-side.
- **Item-template references are `itemTemplateId`** (+ `itemTemplateCount`); a concrete item instance
  is `itemId`.
- **Units live in the field name** — `respawnSec`, `reuseDelayMs`, `chancePercent`. Non-physical
  counts / ids stay unsuffixed.
- **Boolean fields carry no `is` prefix.**
- **Well-known open vocabularies ship as `WellKnown*` constant classes** next to their DTO
  (`WellKnownServices`, `WellKnownDeathMetadata`, `WellKnownBossStatuses`, …). Open by design: hosts
  MAY publish keys outside the canonical set, so consumers must tolerate unknown values.

## Constraints

- **Java 8 source + target** — no `var`, no `Stream.toList()`, no records, no `Map.of`, no text
  blocks, no switch expressions, no pattern matching. Stream API + lambda + Optional are fine.
- **Zero runtime dependencies** — pure JDK. No Spring, no Lombok, no Jackson, no Gson; JSON binding
  is the consumer's business. One exception: `org.jspecify:jspecify` for `@Nullable` / `@NonNull` —
  `RetentionPolicy.CLASS`, so no runtime cost. Wired as `api(libs.jspecify)` so consumers get the
  annotations transitively and can run their own nullness checking against the wire types.
- **POJOs, not records** (records are Java 14+): final fields, private constructor, static
  `builder()`, `equals`/`hashCode`. Every multi-field DTO ships a hand-written `Builder`.
- **Public API → Javadoc mandatory**, by substance (semantics, units, null-rules, invariants), never
  a restatement of the field name.
- **No framework annotations** — `@Component`, `@JsonProperty`, `@NotBlank` and friends are forbidden;
  the artifact is consumed by both Spring and non-Spring sides.
- **Constructor parameter names are preserved** (`-parameters` javac flag) so parameter-name-binding
  deserializers build the POJOs without annotations.

## Versioning

Slash-namespaced tag `api/vX.Y.Z` releases this module independently. Fallback when no
`-Pnx-gs-adapter-api.version=...` is passed: the literal in this module's `build.gradle.kts`. Release
flow and the two-release deprecation rule live in [`../CLAUDE.md`](../CLAUDE.md).

## Testing

- **Naming**: `methodName_shouldExpectedBehavior` or `methodName_shouldExpectedBehavior_whenCondition`
- JUnit 5, no Mockito (pure DTOs — nothing to mock)
- Test what builders / `equals` / `hashCode` actually guarantee, not framework code
