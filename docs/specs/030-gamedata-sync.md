# Game-data sync (gd-sync) — static catalog snapshots + host-readiness gate

> Owner: @n1rmata

Living spec for the `:nx-gs-gd-sync-core` module. Covers what the module publishes and how, plus
the host-readiness contract added in gd-sync `0.11.0` / api `0.83.0`.

Cross-repo: `bohpts-core` (Tier-2 providers + readiness signal), `nx-gamedata` (platform consumer of
the `gd` stream).

## Problem

Static game data — item / npc / skill / recipe / armor-set / soul-crystal / class / instance
templates plus the gear-score ruleset — lives in the host's datapack and is parsed into host memory
at boot. The platform needs it as a catalog (wiki, market, gear-score tables) and cannot read the
datapack itself: file layout is per-client proprietary, and the parsed form (post-override,
post-custom) is the only truthful one.

So the adapter publishes the host's already-parsed in-memory catalogs as full snapshots on the `gd`
sync stream, and re-publishes on demand when the host reloads its datapack.

The second half of this spec fixes a boot-ordering flaw in that design: the adapter connects and
pulls a snapshot **before** the host has loaded those catalogs, which produced eight false `ERROR`
lines on every game-server boot (312 over 2026-08-01…15) and — via the gear-score singleton path —
briefly reconcile-deleted the platform's gear-score ruleset on every boot.

## 1. Module shape

`GameDataSyncModule` is a Tier-1 `AdapterModule`, discovered by adapter-core via `ServiceLoader`.
Data-driven: a static registry of `EntityDescriptor`s pairs each gd entity's Tier-2 provider SPI with
its snapshot accessor and primary-key extractor, so adding an entity is one registry line.

### 1.1 Entity registry

| entity                | Tier-2 SPI                    | pk                                 |
| --------------------- | ----------------------------- | ---------------------------------- |
| `itemtemplate`        | `ItemTemplateProvider`        | `template.id`                      |
| `npctemplate`         | `NpcTemplateProvider`         | `template.id`                      |
| `skill`               | `SkillProvider`               | `skill.id`                         |
| `recipetemplate`      | `RecipeTemplateProvider`      | `template.id`                      |
| `armorsettemplate`    | `ArmorSetTemplateProvider`    | `template.id`                      |
| `soulcrystaltemplate` | `SoulCrystalTemplateProvider` | `template.id`                      |
| `classtemplate`       | `ClassTemplateProvider`       | `clazz.ordinal()` (`-1` when null) |
| `instance`            | `InstanceTemplateProvider`    | `template.id`                      |
| `gearscore`           | `GearScoreRulesetProvider`    | constant `0` (singleton)           |

Each present provider becomes an independent `EntitySync` with its own burst, `syncId` and heartbeat
`EntityStats`. A missing provider is skipped; two impls of one SPI put the module in `FAILED` (the
module never guesses which one is authoritative). No provider at all → `DISABLED`.

`gearscore` is a **singleton entity**: its SPI returns `Optional<GearScoreRuleset>`, adapted to a
0-or-1-element collection so it reuses the same burst engine.

### 1.2 Burst protocol

Per entity, keyed by `serverId` (raw 16-byte UUID, so the whole burst lands on one partition in
order):

1. one `GameDataSyncEvent{op=UPSERT, syncId, pk, payload}` record per template,
2. one terminal `GameDataSyncEvent{op=SNAPSHOT_COMPLETE, syncId, count}` marker.

`syncId` is UUIDv7 (time-ordered — the platform derives burst time from the id). Every record carries
`Nx-Message-Type=GameDataSyncEvent`. Per-entity topics come from `ctx.getSyncTopics().getGd()`; an
absent topic skips that entity with a `WARN`.

The platform treats `SNAPSHOT_COMPLETE` as the reconcile point: rows of that entity not seen under
the terminal `syncId` are deleted. That makes the marker the dangerous part of the protocol — see §2.

### 1.3 Triggers

| trigger                        | source                                             |
| ------------------------------ | -------------------------------------------------- |
| initial burst on `start()`     | adapter connect lifecycle                          |
| `NxGameData.publishSnapshot()` | host, e.g. after an in-game datapack reload        |
| `GdResyncCommand`              | platform, remote RPC over the commands topic       |
| periodic resync                | `l2nx.gd-sync.resync-interval-hours` (default off) |

All four funnel into `runAllSnapshots`, which coalesces concurrent triggers into a single in-flight
pass: every caller arms `rerunRequested` **before** contending for the running flag, so no trigger is
lost and no two passes run at once. All of them run on `ctx.io()` — never on the connect or game
thread.

### 1.4 Config

`GameDataSyncConfig`, file-first source chain (`l2nx.properties`, then system properties), read once
at module start:

- `l2nx.gd-sync.resync-interval-hours` — `0`/absent disables the scheduler; any positive value is
  clamped up to a minimum of `1` hour.

Readiness timings (§3.6) are deliberately **not** configurable — see the decision note there.

### 1.5 Status

`currentStatus()` reports module state plus one `EntityStats` per active entity: `HEALTHY` once a
burst completed, `DEGRADED` otherwise; `lastSyncEpochMs` stays `0` until the first complete burst, so
the platform can tell "never synced" from "was synced, now degraded".

## 2. Snapshot contract: `null` ≠ empty

The provider contract distinguishes two returns, and the distinction is load-bearing:

- **empty collection** — "this catalog is legitimately empty". Legal. Emits `SNAPSHOT_COMPLETE`
  with `count=0`, whose reconcile pass **deletes every row of that entity** on the platform.
- **`null`** — "no snapshot to give". The publisher aborts the burst: no marker is emitted, nothing
  is reconciled, the previous platform state survives untouched.

Coercing `null` into an empty collection would therefore wipe the whole catalog. This is why the
publisher refuses, and why refusing is correct. What was missing until §3 is that `null` had only one
reading — "the provider broke its contract" — while in practice it also meant "the host has not
loaded its catalogs yet", which is a normal, expected state on every boot.

## 3. Host readiness

### 3.1 Symptom

Every game-server boot logged eight `ERROR` lines, one per entity:

```
ERROR a.l.g.g.s.GameDataSnapshotPublisher - [L2NX] gd-sync provider for entity 'itemtemplate'
      returned null snapshot (contract violation) — burst aborted, no SNAPSHOT_COMPLETE emitted
```

~39s later a second burst published all eight successfully. No data loss — but 16 lines/day/server of
permanently false `ERROR`, which trains operators to ignore `ERROR` from this component and will mask
the case where the retry does _not_ recover. Reported independently by an operator reading a boot
log: real attention spent on a non-event.

### 3.2 Root cause

The adapter starts — and gd-sync fires its initial burst — during host boot, **before** the host has
parsed its item / npc / skill / spawn catalogs. Pulling a snapshot then would force-load those
parsers off the host boot thread, out of order (e.g. spawn parsing ahead of zone / territory init),
risking a half-initialized load or a poisoned singleton. So the host's Tier-2 providers deliberately
return `null` until boot finishes — exactly as documented in `BohptsGameDataModule`.

In other words the adapter logged `ERROR` for behaviour it itself required. `null` was carrying two
distinct states with no way to express the first:

- "provider is not ready yet" — expected, transient, recovers on the next burst,
- "provider is ready and returned null anyway" — a genuine contract violation.

### 3.3 The gear-score hole (not in the original report)

`gearscore` was reported as "the only entity that succeeds on the first burst". It does not succeed —
it publishes an **empty** snapshot. Its SPI returns `Optional<GearScoreRuleset>`, which physically
cannot express "not ready" separately from "gear score is disabled": the host returns
`Optional.empty()` while unready, the descriptor maps it to an empty collection, and the publisher
emits a perfectly legal `SNAPSHOT_COMPLETE count=0`.

The platform then reconcile-deletes `gd_gearscore_rulesets` for that server and re-creates it ~39s
later when the real burst lands. That is precisely the failure the `null` guard exists to prevent,
leaking through the singleton path — a recurring data hole on every boot, not just log noise. It is
the reason this fix is a readiness contract rather than a log-severity change.

### 3.4 Design — `GameDataReadinessProvider`

New optional Tier-2 SPI in `nx-gs-adapter-api`:

```java
package app.l2nx.gs.adapter.api.spi;

public interface GameDataReadinessProvider {
    boolean ready();
}
```

- Resolved by gd-sync via `ServiceLoader` alongside the entity descriptors.
- **No implementation registered → the host is always ready.** Every existing host keeps today's
  behaviour; the change is purely additive on the wire and on the classpath.
- Two implementations → module `FAILED`, consistent with how duplicate entity SPIs are handled.
- Javadoc contract: `ready()` MUST be cheap, non-blocking, and callable from any thread (the adapter
  polls it from its own scheduler thread); it returns `false` while the host's game-data catalogs are
  still loading.

Module-level, not per-entity: gd catalogs come from one datapack load, and a per-entity gate would
mean nine SPI changes and nine host overrides for a signal that flips once.

### 3.5 Pass-level gate

`runAllSnapshots` checks readiness **once per pass, before the first `snapshot()` call**. Not ready →
the whole pass is skipped, providers are not touched at all, and the fallback poll (§3.6) is
(re-)armed — which also covers a host that goes unready again for a datapack reload after a
successful pass. Ready → the pass cancels the poll before publishing, so whichever path opens the
gate first makes the other redundant; this is what stops a boot from publishing the whole catalog
twice.

The same guard also drops a pass whose module is no longer `ACTIVE`, or whose `ConnectContext` is not
the current one: an `io()` task queued before a reconnect must not publish against the serverId and
topic map of the connection that replaced it.

Skipping at pass level (rather than per entity) is what closes §3.3: `gearscore` never gets the
chance to emit `count=0`, because its provider is not consulted while the host is unready. It also
preserves the boot-ordering guarantee the host wants — an unready pass triggers zero parser
force-loads.

Consequences for the other triggers:

- `start()` on a ready host — initial burst exactly as before.
- `start()` on an unready host — one `INFO` (`deferring initial snapshot`) and the poller (§3.6)
  is armed. No `ERROR`, no `WARN`.
- `GdResyncCommand` on an unready host — replies `CommandResult.unavailable("game data not ready")`
  instead of acking with `acceptedEntities` and then publishing nothing. The platform learns the
  reason instead of waiting for a snapshot that was never scheduled.
- Host-driven `publishSnapshot()` on an unready host — no-op pass, same as above.

### 3.6 Readiness poller

The module keeps a single lazily-created daemon scheduler (`nx-gd-sync-scheduler`) that now serves
both the periodic resync and readiness polling — no second thread is introduced.

- Armed whenever a pass finds the host unready — at `start()`, and again later if the host goes
  unready after having been ready. Arming is idempotent: one poll handle at a time.
- Fixed **5s** re-check. The check is one boolean call; a backoff would be extra state for no gain.
- Ready → dispatch a snapshot pass; that pass cancels the poll (§3.5), so the poller does not
  disarm itself.
- Scheduler lifecycle is guarded by one lock, and a shutdown latches: `start()` runs on the adapter's
  connect thread while `stop()` runs on the host's, so a late scheduling attempt must not resurrect a
  daemon that would outlive the connection.
- Still unready after **15 min** → exactly one `ERROR` (`host game data still not ready after 15m —
gd catalogs will not sync until the host reports ready`), then keep polling silently. A host that
  finishes booting on minute 20 still publishes.

The host's own `publishSnapshot()` call at end-of-boot stays the fast path; the poller is the
fallback that makes gd-sync self-sufficient against a host that never calls it.

**Decision — the timings are constants, not config keys.** Deliberate: fewer knobs on a module whose
only existing knob is the resync interval. Accepted cost — a host whose boot exceeds 15 min gets one
false `ERROR` per boot (down from eight). If such a host appears, promote both values to
`l2nx.gd-sync.*` keys then.

### 3.7 `null` reclassification (defence in depth)

The gate removes `null` for a host that implements the readiness SPI. `null` can still arrive from a
host that does not — including `bohpts-core` itself, in the window between the gd-sync release and
the host wiring its provider. So the publisher's classification changes:

- Per entity, per connection, the module tracks when `null` was first observed.
- Within 15 min of that first `null` → `WARN` ("provider not ready yet, burst deferred").
- After it → one `ERROR`, then silence for that entity.
- Any successful burst resets the tracker.

Net effect: the boot noise disappears at the gd-sync release, **before** any host change, while a
provider that is genuinely broken still surfaces as `ERROR`.

The classification is a small pure state object (input: entity, now, first-null timestamp → output:
severity), so tests assert the decision rather than scraping log output.

### 3.8 Host side (`bohpts-core`)

- New `BohptsGameDataReadinessProvider` returning `BohptsHostReady.isReady()`, plus its
  `META-INF/services` entry.
- The nine existing providers are unchanged: their `null` / `Optional.empty()` guards stay as
  defence in depth and keep the host compatible with an older adapter.
- `BohptsGameDataModule.markReady()` keeps calling `NxGameData.publishSnapshot()` — now the fast path
  rather than the only path.

After the gate is live, `Optional.empty()` from the gear-score provider means only what it should:
gear score is disabled on this server, and deleting the ruleset row is the correct outcome.

## 4. Requirements

**Must:**

- [done] R1. gd-sync MUST NOT pull any provider snapshot while the host reports itself not ready.
- [done] R2. A host with no `GameDataReadinessProvider` on the classpath MUST behave exactly as
  before this change (always ready).
- [done] R3. The `gearscore` singleton MUST NOT emit `SNAPSHOT_COMPLETE count=0` as a result of host
  unreadiness. (`count=0` when gear score is genuinely disabled stays legal.)
- [done] R4. A boot of a readiness-aware host MUST produce zero `ERROR` and zero `WARN` from gd-sync.
- [done] R5. gd-sync MUST publish the first full snapshot on its own once the host becomes ready,
  without depending on the host calling `publishSnapshot()`.
- [done] R6. A host that is still not ready 15 min after connect MUST produce exactly one `ERROR`,
  and MUST still publish if it becomes ready later.
- [done] R7. `GdResyncCommand` received while the host is not ready MUST be answered `UNAVAILABLE`,
  not acked as accepted.

**Should:**

- [done] R8. A `null` snapshot from a host without the readiness SPI SHOULD be reported as `WARN` for
  the first 15 min after the first occurrence, and escalate to a single `ERROR` afterwards.

**Non-goals:**

- Making the readiness timings operator-configurable (§3.6).
- Per-entity readiness granularity.
- Any change to the `gd` wire shape, the burst protocol, or `ModuleStatus` / heartbeat payloads.

## 5. Tests (`nx-gs-gd-sync-core`)

- `runAllSnapshots` performs no provider call and publishes nothing while readiness is `false`
  — asserted per entity **and** for the `gearscore` singleton (no `count=0` marker).
- A ready host bursts exactly as today (regression against the pre-change behaviour).
- No readiness provider registered → always ready.
- Two readiness providers → `STATE_FAILED`.
- The poller publishes once readiness flips, then stops — driven synchronously, no sleeps.
- A boot publishes the catalog **once**: with the poll armed, the host's own publish disarms it, and
  a following poll tick adds nothing.
- A host that goes unready after a successful pass re-arms the poll and publishes nothing.
- Deadline escalation fires exactly once and does not stop the poller.
- `GdResyncCommand` while unready → `UNAVAILABLE`.
- `null`-severity decision table (`WARN` inside the window, single `ERROR` past it, reset on a
  successful burst), asserted per entity: one entity escalating must not escalate another.

Readiness providers are injected explicitly in tests rather than discovered — `ServiceLoader` cannot
express "none registered" or "two registered" on a classpath shared by the whole test module.

## 6. Rollout

Ordering is forced by Maven Central propagation (~15-30 min) and the host build:

1. `api/v0.83.0` — additive: new `GameDataReadinessProvider` interface only.
2. `gd-sync/v0.11.0` — gate + poller + `null` reclassification. **The boot noise is gone at this
   step**, via §3.7, before the host knows anything about readiness.
3. `bohpts-core` — bump api + gd-sync deps, register `BohptsGameDataReadinessProvider`. Takes effect
   at the next game-server restart; from then on the unready window produces no log lines at all and
   the gear-score ruleset stops being deleted-and-recreated on boot.

No step is breaking, and steps 2 and 3 are independently useful — an interrupted rollout leaves a
working system.

## 7. Links

- Issue: [nexuslabsio/nx-gs-adapter#8](https://github.com/nexuslabsio/nx-gs-adapter/issues/8)
- `docs/specs/023-platform-sync-fixes-2026-06.md` — Fix ③, the gear-score deploy gap that first
  shipped the `gearscore` entity (`gd-sync/v0.10.0`); a version-skew incident, not a design record.
- Host side: `bohpts-core` `l2e.gameserver.l2nx.data.BohptsGameDataModule` (boot-ordering gate and
  the `null` protocol it relies on).
- Platform consumer: `nx-gamedata` (gd stream ingest, reconcile-on-`SNAPSHOT_COMPLETE`).
