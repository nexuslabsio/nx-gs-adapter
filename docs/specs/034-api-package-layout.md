# Package layout

> Owner: @n1rmata
>
> Consumers that change imports with this slice: the reference host integration, nx-gameservers, nx-gamedata,
> nx-social, nx-tenants, nx-alerts, nx-wiki.

## Problem

Several packages grew into flat piles mixing different kinds of files. The worst is `api.spi`: 37
files in one directory — host provider ports, mapping value types, capability facades, no-op
fallbacks and an exception side by side. Event and command families keep `WellKnown*` vocabularies,
enums and helper value objects next to the events and commands themselves, so a reader cannot tell
the slice's contract from its supporting types. The same smell exists inside core and db-sync.

## Rule

- A package is a slice. Two or more files of the same kind inside a slice move into a sub-package:
  data models (value objects, enums, `WellKnown*` vocabularies) → `model/`; host-implemented ports →
  `provider/`; a self-contained mechanism (several classes implementing one thing) → a sub-package
  named after it. A single file of a kind stays in the slice root.
- The slice root keeps the orchestration core: the slice's main types and singles below the
  threshold.
- Wire-contract slices are read by role: a command group's `{X}Command` / `{X}Result` pairs and an
  event family's events are the slice content and stay in the root; an entity DTO package
  (`kafka.sync.db.*`, `kafka.sync.gd.skill`, `kafka.sync.gd.npctemplate`) is one aggregate of one
  kind and stays flat.
- The rule applies on first touch: adding the second file of a kind opens the sub-package.

## Requirements

- [done] R1. `api.spi` splits as follows; the root keeps `AdapterModule`, `CommandContext`,
  `ConnectContext`, `DirectExecutor`, `HostExecutorTimeoutException`:
  - `spi.provider` — `ArmorSetTemplateProvider`, `ClassTemplateProvider`, `InstanceTemplateProvider`,
    `ItemTemplateProvider`, `NpcTemplateProvider`, `RecipeTemplateProvider`, `SkillProvider`,
    `SoulCrystalTemplateProvider`, `ExperienceLevelProvider`, `GearScoreRulesetProvider`, `GameDataReadinessProvider`,
    `DbSchemaProvider`, `RuntimeStateProvider`, `JdbcConnectionSource`.
  - `spi.model` — `EntityMapping`, `PrimarySource`, `ChildSource`, `ParentRef`,
    `RuntimeEntityMapping`, `RuntimeRow`.
  - `spi.capability` — `NxCommands`, `NxEvents`, `NxSync`, `NxGameData`, `NxGameDataTrigger`,
    `NxSyncTrigger`, `NxSyncResyncHandler`, `CommandHandler`, `HostExecutor`, `DeferredReply`
    (new, [`commands`](009-commands/spec.md) R27).
  - The package-private `NoOp*` fallbacks stay in the root next to `ConnectContext`, their only
    user — a `spi.noop` package would force them public.

- [done] R2. Supporting types of wire families move into `model/`:

  | Slice                          | Into `model/`                                                                                                               |
  | ------------------------------ | --------------------------------------------------------------------------------------------------------------------------- |
  | `kafka.commands.privatestore`  | `BoughtLine`, `BuyLine`, `DroppedLine`, `SellLine`                                                                        |
  | `kafka.commands.ban`           | `WellKnownBanTargetTypes`, `WellKnownBanTypes`                                                                              |
  | `kafka.commands.mail`          | `ItemDeliveryError`, `MailItem`                                                                                             |
  | `kafka.commands.captcha` (new) | `CaptchaRoundResult`, `WellKnownCaptchaOutcomes`                                                                            |
  | `kafka.events.privatestore`    | `Offer`, `TradeLine`, `PrivateStoreSide`, `WellKnownPrivateStoreMetadata`                                                   |
  | `kafka.events.premiumpurchase` | `Payment`, `PurchaseItem`, `PurchaseService`, `WellKnownServices`                                                           |
  | `kafka.events.olympiad`        | `OlympiadGameType`, `OlympiadMatchReason`, `OlympiadMatchResult`                                                            |
  | `kafka.events.character`       | `WellKnownDeathMetadata`, `WellKnownFarmModes`, `WellKnownKillerTypes`, `WellKnownPresenceMetadata`                         |
  | `kafka.events.serveronline`    | `WellKnownServerOnlineBuckets`, `WellKnownServerStartMetadata`                                                              |
  | `kafka.ops`                    | `ChangesSummary`, `CommandsStats`, `EntityState`, `EntityStats`, `EventsStats`, `ModuleStates`, `ModuleStatus`, `PoolStats` |
  | `kafka.sync.runtime.character` | `Activity`, `WellKnownActivities`, `WellKnownActivityMetadata`, `WellKnownAiStatuses`                                       |
  | `kafka.sync.gd.npctemplate`    | `WellKnownNpcTypes`                                                                                                         |
  | `kafka.sync.gd.gearscore`      | `GearScoreRule`, `GearScoreRuleGroup`, `GearScoreScalingStep`, `WellKnownGearScoreEnchantProfiles`                          |

  Package-private helpers (`MailLists`, `PrivateStoreLists`) stay with the commands that use them;
  `BuyLine` carries its own attribute-map copy instead of reaching back into the root helper.

- [done] R3. Internal modules (not public API, no consumer impact):
  - core `connect` → `connect.flow` (`ConnectFlow`, `GameServerConnectFlow`, `HostConnectFlow`,
    `LoginServerConnectFlow`, and the package-private `TypedHttpConnect` they share),
    `connect.backoff` (`BackoffSchedule`, `DefaultBackoffSchedule`),
    `connect.model` (`ErrorEnvelope`, `TypedConnectOutcome`).
  - core `kafka.gson` (`AdapterGson`, `LocalizedTextTypeAdapter`). The command / event type
    registries stay in `commands` / `events`: they are package-private collaborators of the
    consumer and publisher there.
  - db-sync `engine.jdbc` (`JdbcDialect`, `SqlIdent`, `StatementRegistry`); `SnapshotStore` stays in
    `engine` — `persist`, `phase` and `window` all depend on it.
  - Tests of moved classes follow them into the mirrored package.

- [done] R4. The moves are a pure relocation: no class is renamed, no member changes, and package
  names are not part of the wire (routing and type headers use simple names — verified against
  `CommandTypeRegistry` / `EventTypeRegistry` before release). Moves land as their own commit, every
  file showing as a rename, before any content change.

- [done] R5. `nx-gs-adapter-api/CLAUDE.md` carries the new package map; the flat-`spi` statement is
  replaced by this rule.

## Compatibility

Source- and binary-breaking for every consumer that imports a moved type; wire-compatible (nothing on
the wire references a package). Because nothing changes on the wire, the two-release `@Deprecated`
path (root `CLAUDE.md` → "Versioning") does not apply — there is no producer/consumer deploy window,
only a compile break. Released in one step as `api/v0.89.0`, with every module that compiles
against the api re-released in the same pass (`core/v0.38.1`, `db-sync`, `runtime-sync`, `gd-sync`),
since their published jars reference the old packages. Each consumer bumps and fixes imports in the
same pass; an un-bumped consumer keeps working on the old jars until it next upgrades.

## Non-goals

- Renaming types or changing any contract.
- Splitting the entity DTO aggregates (`sync.gd.skill`, `sync.gd.npctemplate`, `sync.db.*`).
