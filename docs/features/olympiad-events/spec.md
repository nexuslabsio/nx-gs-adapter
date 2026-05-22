# Olympiad match-result events

Per-character history of every Olympiad 1v1 match outcome — wins, losses,
draws, and all no-fight edge cases (default, mid-fight disconnect, timeout).

## Goal

Expose two consumer-side capabilities on the platform side:

1. **Per-character battle history within an Olympiad cycle** — query "show all
   matches char X played in cycle N" and reconstruct the sequence of outcomes,
   opponents, points changes, and damage.
2. **Complete points-change audit** — every Olympiad point movement is
   observable on the wire, including no-fight cases where the host awards or
   deducts points without a real battle (technical defaults, mid-fight
   disconnects, timeout penalties).

Snapshot fields are NOT carried when the platform's CDC streams already cover
them (character / clan names, hero status) — consumers join by `charId` /
`clanId` on the corresponding CDC stream.

## Scope

**In scope (v1):**

- 1v1 matches: `OlympiadGameClassed` + `OlympiadGameNonClassed` (both extend
  the common `OlympiadGameNormal` skeleton — one hook covers both).
- All `validateWinner` branches that result in either points change OR a
  finished match observation (BOTH_OFFLINE draw with zero delta still emits).

**Out of scope (deferred):**

- `OlympiadGameTeams` (3v3) — different participant shape; non-breaking add
  as a separate event subtype when needed.
- Cycle-end events (hero crowning, points reset) — derivable from the
  `olympiadCycle` field on subsequent match events; bulk-reset event would
  be N-events at cycle boundary, not warranted.
- Noblesse passes / reward grants (`OlympiadLogger.logNoblessePasses`) —
  separate concern (reward distribution, not match history).
- Match-started events — a started-but-never-finished match produces no
  points change (early-return branch), no signal value for history.

## Wire contract

**Family:** `events.olympiad` → `<tenant>.gs.events.olympiad`
**Event type (single):** `OlympiadMatchResultEvent`
**Partition key:** `charId` (8-byte big-endian) — all of one character's
Olympiad history lands on one partition in occurrence order.

Per-participant model — every `validateWinner` invocation that progresses past
the early-return guards emits **two** events (one per participant), each
written from that participant's self-perspective. Shared `matchId` lets
consumers pair the two sides on demand.

### Event payload

```
eventId: UUID                  REQUIRED — UUIDv7, occurredAt anchor
matchId: UUID                  REQUIRED — shared by both per-participant events
olympiadCycle: int             REQUIRED — Olympiad.getCurrentCycle()
gameType: OlympiadGameType     REQUIRED — CLASSED | NONCLASSED

// Self (partition key)
charId: long                   REQUIRED
classId: int                   REQUIRED — base class at match time
clanId: @Nullable Long         snapshot — clan affiliation can change mid-cycle

// Opponent
opponentCharId: long           REQUIRED
opponentClassId: int           REQUIRED
opponentClanId: @Nullable Long snapshot

// Outcome
result: OlympiadMatchResult    REQUIRED — WIN | LOSS | DRAW
reason: OlympiadMatchReason    REQUIRED — see enum

// Points (single source of truth — delta = after - before)
pointsBefore: int              REQUIRED
pointsAfter: int               REQUIRED

// Combat (zeros / null when no fight occurred)
damageDealt: int               REQUIRED
opponentDamageDealt: int       REQUIRED
fightStartedAt: @Nullable Instant   null = no fight occurred
fightDurationSec: long         0 = no fight
```

### Enums

**`OlympiadGameType`**: `CLASSED, NONCLASSED` — Teams added non-breaking
when 3v3 support lands.

**`OlympiadMatchResult`**: `WIN, LOSS, DRAW`.

**`OlympiadMatchReason`** (self-perspective):

- `NORMAL` — fight occurred and concluded by HP / death / damage tiebreak.
- `OPPONENT_DEFAULTED` — opponent did not show (pre-fight technical loss).
- `SELF_DEFAULTED` — I did not show.
- `BOTH_DEFAULTED` — both no-show, DRAW.
- `OPPONENT_DISCONNECTED` — opponent dropped mid-fight.
- `SELF_DISCONNECTED` — I dropped mid-fight.
- `BOTH_DISCONNECTED` — both dropped mid-fight, DRAW.
- `BOTH_OFFLINE` — both offline at match end without explicit crash flag,
  DRAW with zero points delta.
- `TIMEOUT` — both alive at time-up, DRAW with –1/divider penalty each.

## Branch mapping (bohpts `OlympiadGameNormal.validateWinner`)

| Branch condition                      | P1 `(result, reason)`              | P2 `(result, reason)`          | Emits event?                        |
|---------------------------------------|------------------------------------|--------------------------------|-------------------------------------|
| `_aborted`                            | —                                  | —                              | No (early return)                   |
| `_startTime == 0` AND any crash       | —                                  | —                              | No (early return, no points change) |
| `p1.defaulted && !p2.defaulted`       | `(LOSS, SELF_DEFAULTED)`           | `(WIN, OPPONENT_DEFAULTED)`    | Yes                                 |
| `p2.defaulted && !p1.defaulted`       | `(WIN, OPPONENT_DEFAULTED)`        | `(LOSS, SELF_DEFAULTED)`       | Yes                                 |
| `p1.defaulted && p2.defaulted`        | `(DRAW, BOTH_DEFAULTED)`           | `(DRAW, BOTH_DEFAULTED)`       | Yes                                 |
| `p2crash && !p1crash && _startTime>0` | `(WIN, OPPONENT_DISCONNECTED)`     | `(LOSS, SELF_DISCONNECTED)`    | Yes                                 |
| `p1crash && !p2crash && _startTime>0` | `(LOSS, SELF_DISCONNECTED)`        | `(WIN, OPPONENT_DISCONNECTED)` | Yes                                 |
| `p1crash && p2crash && _startTime>0`  | `(DRAW, BOTH_DISCONNECTED)`        | same                           | Yes                                 |
| Both offline at end (no crash flag)   | `(DRAW, BOTH_OFFLINE)` (delta=0)   | same                           | Yes                                 |
| Normal WIN by HP / damage tiebreak    | `(WIN, NORMAL)` / `(LOSS, NORMAL)` | mirror                         | Yes                                 |
| Both alive at timeout                 | `(DRAW, TIMEOUT)`                  | same                           | Yes                                 |

`pointsBefore` is captured at method entry (existing locals `playerOnePoints`
/ `playerTwoPoints`); `pointsAfter` is read from `_playerX.getStats().getInteger(POINTS)`
at the end of the method (after `addPoints` / `removePoints` ran). Damage
fields use existing `_damageP1` / `_damageP2`. `matchId` is minted once via
UUIDv7 at validation start and shared by both per-participant events.
`fightStartedAt` derives from `_startTime` (epoch ms → Instant, null when
`_startTime == 0`).

## Host-side wiring

**`l2e.gameserver.l2nx.events.olympiad.OlympiadMatchResultPublisher`** —
game-loop-safe facade following the
`PrivateTradeFinishedPublisher` / `MailSentPublisher` pattern: static
`volatile @Nullable NxEvents events`, `bind(handle)` / `bind(null)`,
error-swallowing publish method that emits both per-participant events from
one call.

`BohptsEventsModule` wires `bind` on `onConnect`, `bind(null)` on
`onDisconnect`.

`OlympiadGameNormal.validateWinner(stadium)` end-of-method dispatcher:
captures (result, reason) into local variables per branch, single
`OlympiadMatchResultPublisher.publish(...)` call before the existing
`stadium.broadcastPacket(result)`.

## Adapter-core registration

`EventTypeRegistry`: one `register(...)` line —
`OlympiadMatchResultEvent.class → "olympiad"`, partition extractor
`LongBytes.bigEndian(event.getCharId())`. Family auto-flows through
heartbeat / disabled-families reporting.

## Idempotency / ordering

- `eventId` UUIDv7 — platform-side dedupe key (at-least-once delivery).
- Per-char partition + UUIDv7 ordering ⇒ stable per-char timeline on
  consumer.
- The two per-participant events of one match land on **different**
  partitions (different charIds) — by design; consumer pairs by `matchId` if
  needed.

## Testing

- API: JUnit per-DTO tests matching the existing pattern (builder roundtrip,
  defensive copy where applicable, equals discrimination on each field,
  toString sanity).
- Core: `EventTypeRegistryTest` additions for the new family + partition
  key extractor.
- Host: no unit tests for the publisher (consistent with the existing
  mail / privatetrade / privatestore publishers — host wiring is exercised
  via end-to-end runs).
