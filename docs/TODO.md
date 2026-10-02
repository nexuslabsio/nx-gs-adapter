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
  bohpts `SendCaptchaHandler` sidestepped it by switching to `host().async`.
- **Related:** [`009-commands`](specs/009-commands/spec.md).

## Done / moved into a spec

—
