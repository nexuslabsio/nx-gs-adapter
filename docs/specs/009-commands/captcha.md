# Commands — Captcha check

> Owner: @n1rmata
>
> Counterparts:
>
> - nx-gameservers `docs/specs/076-character-captcha.md` — REST entry points, fast window, audit.
> - nx-sac-sentinel `docs/specs/012-captcha-measure.md` — the automatic caller and reply consumer.
> - bohpts-core `l2e.gameserver.l2nx.commands.captcha.SendCaptchaHandler` over the host captcha
>   service `l2e.gameserver.handler.captcha.CaptchaService` (host specs are local to that repo).

## Problem

The anti-cheat (L2NX SAC) needs to put a human-verification check in front of a suspected bot and
learn what the player actually did: which answers they clicked, how fast, and how the check ended.
The host already has an in-game captcha that a GM starts by hand; nothing can start it from the
platform, and its outcome never leaves the game server.

The answer arrives minutes after the request — a check runs several rounds with a per-round
deadline. The commands rail replies once, synchronously, when the handler returns (see
[`commands`](spec.md) R6/R11), so it cannot carry a result that does not exist yet.
This slice adds the **deferred reply** to the rail (`commands` R27) and the first command that
uses it.

## Requirements

> Sibling features carry the plumbing:
>
> - [`commands`](spec.md) — topic, dispatch, reply path; R27 (deferred reply) is added
>   by this slice.
> - [`ban-commands`](ban.md) — the ban vocabulary (`WellKnownBanTypes`) a host reuses in
>   the result `metadata`.

### Wire

- [done] R1. `nx-gs-adapter-api.kafka.commands.captcha.SendCaptchaCommand` MUST ship as
  `NxCommand<SendCaptchaResult>`. Final Java-8 POJO + builder; constructor enforces non-null
  `characterId` (the handler re-validates, Gson bypasses the constructor). Fields:
  - `Long characterId` — REQUIRED. Target character (platform canon `characterId`; the legacy `charId` of older commands is not reused).
  - `@Nullable String issuedBy` — who asked for the check: a staff login, or a service label such
    as `sac-sentinel`. Echoed in the result so a consumer can attribute checks it did not start.
  - `@Nullable String staffNotes` — staff-only note, never shown to the player (same convention as
    [`character-admin-commands`](character-admin.md) R5).

  Not idempotent, and needs no dedup: a second delivery while a check is open is rejected as
  `INVALID_STATE` (one open check per character), and the rail is at-most-once anyway.

- [done] R2. `SendCaptchaResult` MUST ship as the success payload. It is produced once, when the
  check ends — never as an intermediate progress update:
  - `Long characterId`, `@Nullable String issuedBy` — echo.
  - `String outcome` — open string, UPPER_SNAKE, canonical values in
    `kafka.commands.captcha.model.WellKnownCaptchaOutcomes`:
    - `PASSED` — answered enough rounds correctly;
    - `FAILED_WRONG` — ran out of allowed wrong answers;
    - `FAILED_TIMEOUT` — a round or the whole check timed out;
    - `LOGOUT` — the player left the game (or lost the connection) while the check was open;
    - `ABORTED` — the host could not run the check to a verdict (no picture could be drawn, the
      server is shutting down, the check was released by the host for its own reasons).

    A consumer treats an unknown value as `ABORTED`.

  - `Instant startedAt`, `Instant finishedAt` — host clock, UTC.
  - `long durationMs` — `finishedAt - startedAt`, same host wall clock; a clock step during the
    check skews it.
  - `List<CaptchaRoundResult> rounds` — every picture the player was shown, in order; empty when
    the check ended before the first picture.
  - `Map<String, String> metadata` — host-defined consequences and context; never null, may be
    empty. The key set is **not stable** (see R4).

- [done] R3. `kafka.commands.captcha.model.CaptchaRoundResult` — one picture:
  - `int index` — 1-based position in the check.
  - `String questionType` — host vocabulary, UPPER_SNAKE (bohpts: `MAX_AREA`, `MIN_AREA`,
    `ODD_COLOR`, `SHAPE_COUNT`, `MISSING_KIND`). The platform stores it verbatim.
  - `@Nullable Integer pickedSlot` — 0-based button the player clicked; `null` when the round
    timed out without an answer.
  - `boolean correct` — `false` for a timed-out round.
  - `@Nullable Long answerTimeMs` — from the moment the picture was sent to the click, measured on
    the host, so it excludes Kafka and platform latency; `null` when the round timed out.

- [done] R4. `metadata` carries what the host did about the result. Until the format is agreed
  (see [`TODO.md`](../../TODO.md) §1) no `WellKnown*` class is shipped and the keys are host-defined.
  Hosts SHOULD align ban-like consequences with the platform ban vocabulary: `ban.type` (a value
  from `WellKnownBanTypes`), `ban.expiresAt` (ISO-8601 instant); a disconnect is `kick=true`.
  Host-internal escalation state uses unprefixed keys (bohpts: `stage`, `mode`). A host that only
  logs failures (bohpts `mode=LOG_ONLY`) reports the stage the failure would have reached, with no
  `ban.*` / `kick` keys.

### Rail behaviour

- [done] R5. The handler uses the deferred reply (`commands` R27): it takes `ctx.deferReply()`
  before starting the check, posts the start to the game thread without waiting
  (`ctx.host().async`), returns the deferred marker, and every answer — the outcome or a refusal to
  start — goes through the handle, first one wins. Taking the handle first matters: the start can
  end the check it is creating. Validation that needs no host state (R6 `VALIDATION_FAILED`) may
  return directly before the handle is taken.

- [done] R6. Immediate errors (statuses are `commands` R23):
  - `VALIDATION_FAILED` — `characterId` missing / out of the host's id range.
  - `NOT_FOUND` — no such character, or the character is not in the world.
  - `INVALID_STATE` — a check is already open for the character, or the server itself is playing
    the character (a sanctioned auto-play feature: a check would test the server, not the player).
    `problem.extensions["reason"]` names which: `ALREADY_ACTIVE`, `SERVER_PLAYS_CHARACTER`.
  - `RATE_LIMITED` — the host's per-character cooldown between checks has not elapsed.
  - `UNAVAILABLE` — the check is disabled on this server, the concurrent-check limit is reached, or
    no picture could be produced.

  These arrive right after the command, either as the handler's return or through the handle
  (R5); the consumer cannot tell the two apart. Host-specific causes beyond these go into
  `problem.extensions`, never into new statuses.

- [done] R7. Upper bound: a check ends well inside `l2nx.commands.deferred-reply-max-ms` (R27). A
  host whose check can outlive the bound MUST raise the bound in its config, otherwise the adapter
  closes the reply with `INTERNAL_ERROR` and the real outcome is lost.

## Compatibility

Additive on the wire: new command, new result, new model types. `SendCaptchaCommand` is a new simple
name — unique across the catalog. A host on an older api jar does not register the handler and the
platform gets `UNSUPPORTED_COMMAND`. Ships in `api/v0.89.0` + `core/v0.38.0` together with R27 and the
api package layout change ([`api-package-layout`](../034-api-package-layout.md)).

## Non-goals

- **Progress updates.** One reply per command; a consumer that needs live progress is out of scope.
- **Platform-decided consequences.** What happens to a player who fails is the host's policy,
  reported in `metadata`. The platform applies its own measures through the existing ban / kick
  commands.
- **Surviving a host restart.** Open checks live in host memory; a restart loses them together with
  their deferred replies (`commands` R27), and the caller sees no reply.

## Links

- Command catalog entry: [`009-commands/catalog.md`](catalog.md) → "Captcha commands".
- Deferred reply mechanics: [`009-commands/spec.md`](spec.md) R27,
  [`009-commands/guide.md`](guide.md) → "Deferred replies".
