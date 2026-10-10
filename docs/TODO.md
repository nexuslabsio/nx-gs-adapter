# TODO

Living backlog of deferred work. A spec's "deferred" item gets an entry here in the same pass; when
it is picked up, it moves to "Done / moved into a spec".

## Open

### 1. Formalize captcha consequences in `SendCaptchaResult`

- **Want:** a typed, host-agnostic description of what the host did after a captcha check (kick,
  jail, account ban, escalation stage) instead of the free-form `metadata` map.
- **To decide:** whether a dedicated model is needed at all — bans and jails created by a failed
  check already reach the platform through the synced `BanDbDto` rows, so a reference to the ban
  (`banId`) may be enough; kick has no ban vocabulary yet.
- **Why:** consumers (nx-sac-sentinel dossier, admin UI) parse host-defined keys today, and the key
  set is explicitly unstable.
- **Related:** [`009-commands/captcha.md`](specs/009-commands/captcha.md) R4,
  [`009-commands/ban.md`](specs/009-commands/ban.md).

### 2. `HostExecutor.sync` timeout leaves the task queued

- **Want:** a `sync` timeout that means the task will not run: a task the game pool has not started
  yet is skipped, one already running is reported as such instead of as a timeout.
- **To decide:** the shape of the "already started" signal (return normally after waiting it out, or
  a distinct exception), and whether `UNAVAILABLE` replies keep `error.cause=host-executor-timeout`.
- **Why:** today the caller gets `UNAVAILABLE` and the queued task may still mutate game state later
  (kick, ban, mail, item delivery). Each non-idempotent handler would need its own claim flag;
  a host handler sidestepped it by switching to `host().async`.
- **Related:** [`009-commands`](specs/009-commands/spec.md).

### 3. `skill_type` as an enum `SkillType` in `nx-gs-adapter-api`

- **Want:** the gd-sync `Skill.skillType` (and the `gd_skills.skill_type` column it feeds) typed by an api enum
  `SkillType` mirroring the engine's `l2e.gameserver.model.skills.SkillType` (62 values on prod, `BUFF`, `DEBUFF`,
  `PHYSICAL_DAMAGE`, ...), instead of a free string.
- **To decide:** the null policy (14k skills have no type: `NOTDONE` / blank in the datapack), how an unknown engine
  value reaches consumers (accept-then-emit: consumers must tolerate a new constant before the host emits it), and
  where nx-gamedata's `type` filter starts using it.
- **Why:** the skill-type vocabulary is already a contract - nx-gamedata filters by it (`GET
/gamedata/v1/skill-templates?type=BUFF`) and nx-social validates buff watches against it - but nothing pins its values.
  Not to be confused with `SkillEffectCategory` (status-bar group of a hanging effect), which is a different axis.
- **Related:** `nx-social/docs/specs/054-buff-item-watch.md` §2, `nx-gamedata/docs/specs/023-player-template-search.md`.

## Done / moved into a spec

- **Deprecated boss-kind and division shim, raw-seconds activity keys** - removed in `api/v0.93.1`
  (`RaidBossKind`, `RaidKillEvent.bossKind`, `BossRespawnEntry.kind`, `WellKnownBossDivisions`,
  `WellKnownBossMetadata`, `WellKnownActivityMetadata.ELAPSED_SECONDS` / `SECONDS_REMAINING` /
  `SECONDS_TO_NEXT_TIER`). See [`014-events-raid`](specs/014-events-raid.md) R4,
  [`017-character-runtime-activity`](specs/017-character-runtime-activity.md) R4.
