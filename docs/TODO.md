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
- **Related:** [`033-captcha-command.md`](specs/033-captcha-command.md) R4,
  [`024-ban-commands.md`](specs/024-ban-commands.md).

## Done / moved into a spec

—
