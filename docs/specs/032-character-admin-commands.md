# Commands — Character admin (access level, kick)

> Owner: @n1rmata
>
> Host counterpart: bohpts-core `l2e.gameserver.l2nx.commands.character.*` (host specs are local
> to that repo and not published).

## Problem

Two everyday moderation actions still require a GM client: changing a character's access level
(`//changelvl`) and kicking a character (`//kick`). The platform has the character roster and the
commands rail but no down-channel verb for either, so operators log into the game to do them.

This slice adds two commands on the existing `NxCommands` rail (see
[`commands`](009-commands/spec.md)):

- `SetCharacterAccessLevelCommand` — set a character's access level to an absolute value.
- `KickCharacterCommand` — disconnect an online character, either back to the login screen or by
  closing the client.

Both carry `staffNotes`, an internal note the platform surfaces on the command audit; the host never
shows it to the player.

## Requirements

> Sibling features carry the wire + dispatch plumbing:
>
> - [`commands`](009-commands/spec.md) — Kafka commands topic + consumer + dispatch table + reply
>   path. UNCHANGED by this slice.
> - [`db-sync`](003-db-sync/spec.md) — `CharacterDbDto.accessLevel` is the up-channel mirror of the
>   value this slice writes. UNCHANGED.

### Access level

- [done] R1. `nx-gs-adapter-api.kafka.commands.character.SetCharacterAccessLevelCommand` MUST ship
  as `NxCommand<SetCharacterAccessLevelResult>`. Final Java-8 POJO + builder; constructor enforces
  non-null `charId` / `accessLevel` (wire-path Gson bypasses the constructor → the handler
  re-validates and emits `VALIDATION_FAILED`). Fields:
  - `Long charId` — REQUIRED. Target character's primary key.
  - `String accessLevel` — REQUIRED. Opaque, build-agnostic: the same vocabulary as
    `CharacterDbDto.accessLevel` — numeric text on int-based builds (`"0"`, `"7"`), a role name on
    string-role builds. The host converts to its own model; the platform never interprets it.
  - `@Nullable String staffNotes` — internal staff note (see R5).

  Idempotent: writes an absolute value, so re-delivery converges on the same state.

- [done] R2. `SetCharacterAccessLevelResult` MUST ship as the success payload:
  - `Long charId` — echo.
  - `String accessLevel` — the level as the host stored it, in the same vocabulary (an int build
    echoes the canonical numeric text).
  - `@Nullable String previousAccessLevel` — the level before the write, `null` when the host
    could not read it.
  - `boolean wasOnline` — `true` when the change was applied to a live session (takes full effect
    on next login, as with the in-game command); `false` when written to the offline row.

### Kick

- [done] R3. `nx-gs-adapter-api.kafka.commands.character.KickCharacterCommand` MUST ship as
  `NxCommand<KickCharacterResult>`. Constructor enforces non-null `charId`. Fields:
  - `Long charId` — REQUIRED.
  - `boolean closeClient` — REQUIRED. `true` closes the game client; `false` drops the player to
    the login screen.
  - `@Nullable String staffNotes` — see R5.

  NOT idempotent by nature (a second delivery after the player relogged would kick them again) —
  hosts dedupe on the correlation id.

- [done] R4. `KickCharacterResult` MUST ship as the success payload:
  - `Long charId` — echo.
  - `boolean offlineTrader` — `true` when the target was an offline trader: the store was ended
    and `closeClient` had no effect because there was no client to close.

### Staff notes

- [done] R5. Both commands carry `@Nullable String staffNotes`. It is a note for staff, not for
  the player: the host MUST NOT display it in-game and SHOULD log it on the audit line for the
  action. The platform records the whole command payload on `gs_command_audits.params`, which is
  where the admin UI reads it back. New mutating admin commands follow the same convention
  (recorded in [`009-commands/guide.md`](009-commands/guide.md)); retrofitting the existing
  commands is a separate slice.

### Host (bohpts-core)

- [done] R6. `SetCharacterAccessLevelHandler`, registered through the dedup decorator:
  - `VALIDATION_FAILED` when `charId` is missing / out of int range, when `accessLevel` is not
    an integer, when it is negative (a ban goes through `BanCommand`, never through the access
    level), or when the level is not registered in the host's access-level registry.
  - `FORBIDDEN` when the level is above the host's platform-grantable ceiling (bohpts: `5`, the
    top support tier; GM / admin tiers stay an in-game decision). The ceiling binds only this
    command — the in-game `//changelvl` keeps its full range.
  - Online target: on the game thread, `Player.setAccessLevel(level)` +
    `cancelAccessLevelExpiration()` + the same player message the in-game command sends;
    persisted by the regular character store. `wasOnline = true`.
  - Offline target: on the io executor, read the current row (`NOT_FOUND` when absent), write
    `characters.accesslevel`, drop `accessLvlExpires` (the timed-grant marker), refresh the
    host's in-memory access-level cache; a login racing the write yields `INVALID_STATE` so the
    caller retries against the online path. DB failure → `UNAVAILABLE`.
  - On success, `requestResync("character", [charId])` so `CharacterDbDto.accessLevel` catches up
    without waiting for the next CDC cycle.

- [done] R7. `KickCharacterHandler`, registered through the dedup decorator:
  - `VALIDATION_FAILED` when `charId` is missing / out of int range.
  - Target not in the world: `NOT_FOUND` when no such character exists, `INVALID_STATE` when it
    exists but is offline — the platform asked to disconnect a session that is not there.
  - Target in the world: on the game thread, `closeClient ? player.kick() : player.logout()`.
    In this build `kick()` closes the connection with the packet that makes the client exit and
    `logout()` with the one that returns it to the login screen. An offline trader is ended by
    the same call; `offlineTrader = true` in the result.
  - No resync nudge: the logout path stores the character with `online = 0`, which the CDC cycle
    picks up.

## Compatibility

Purely additive. The four `kafka.commands.character.*` types are new in `nx-gs-adapter-api`
(released as `api/v0.88.0`). No existing wire shape changes; `nx-gs-adapter-core` needs no change
(commands route by `Nx-Message-Type`). A host on an older api jar simply does not register the
handlers.

## Non-goals

- **Timed access grants.** The host has a timed-grant mechanism (`accessLvlExpires` +
  `TimedAccessManager`); this command always sets a permanent level and clears any pending expiry,
  exactly like the in-game command. A timed variant is a later iteration of this spec if needed.
- **A reason shown to the player.** The kick is silent; `staffNotes` is staff-only.
- **Platform side.** REST endpoints, audit details and admin UI for both commands live in
  nx-gameservers / nx-admin-web and are their own slice.
- **Multi-server fan-out.** One command targets one server (`Nx-Target-Server-Id`).

## Links

- Command catalog entries: [`009-commands/catalog.md`](009-commands/catalog.md) → "Character
  commands".
- Sibling pattern (character-scoped online/offline handler):
  [`023-platform-sync-fixes-2026-06.md`](023-platform-sync-fixes-2026-06.md) → `UpsertCharacterLockCommand`.
- Ban / moderation contract: [`024-ban-commands.md`](024-ban-commands.md).
