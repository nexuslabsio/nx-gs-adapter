# EtcItemType consumable signals + private-store commands (wire summary)

> Owner: @n1rmata

## Problem

Three nx-gameservers-driven features landed on the same `nx-gs-adapter-api` release, all touching
private-store / item-classification wire shape:

- **Exact market-category signals** — `nx-gameservers` docs/specs/051-market-category-signals.md
  needed a few `EtcItemType` values the host was silently dropping to `null`, plus a new
  `consumable` predicate on the item-template catalog wire DTO, so the market category bar can stop
  approximating.
- **Private-store start/stop commands** — `nx-gameservers` docs/specs/052-private-store-start-commands.md
  needed an outbound command-RPC family to seat a character (online or offline) into a private
  store, or stop one, from the miniapp/admin panel.
- **package-sell offer flag** — `nx-gameservers` docs/specs/054-package-sell-offer-flag.md needed a
  per-offer boolean on the private-store snapshot wire so the platform can tell a `PACKAGE_SELL`
  component from a regular `SELL` lot.

This is the **adapter-side thin summary** — `nx-gameservers` owns the taxonomy / business rules for
all three; this doc is the wire-contract record and cross-link anchor for browsing this repo's spec
index. No new design decisions are made here.

## 1. `EtcItemType` — 3 new constants (`domain.item.EtcItemType`)

`SPELLBOOK`, `MONEY`, `ENSOUL_STONE` added to the shared, build-agnostic etc-item-type vocabulary.
`NONE` already existed on this enum but the host mapping (`bohpts-core`
`BohptsItemTemplateProvider.toEtcItemType()`) fell through its `default -> null` branch for it and
for these three — 4 host `EtcItemType` values were silently dropped. The host-side fix (out of scope
for this repo — see `nx-gameservers` spec 051 §3.1) adds the 4 missing `case`s; `default -> null`
remains for genuinely unmapped future values. Additive, backward-compatible — no existing constant
changes meaning.

## 2. `ItemTemplate.consumable` (`kafka.sync.gd.itemtemplate.ItemTemplate`)

New `@Nullable Boolean consumable` field on the gd-sync item-template wire DTO — the host's own
"is this a consumable" predicate (`EtcItem.isConsumable()` on this build), a more precise signal
than deriving "supply" from a curated `etcItemType` list. `null` = the producer didn't report it
(no signal, not "false"). Field + getter + builder + `toBuilder`/`equals`/`hashCode` added per the
module's hand-written-POJO pattern (see `ItemDbDto` / `ItemAugmentationDbDto` for the template this
follows — [`026-item-augmentation-sync.md`](026-item-augmentation-sync.md)). Additive.

## 3. `commands.privatestore` — start/stop command family

New package `kafka.commands.privatestore`, mirroring the existing `commands.<group>.*` convention
(see [`009-commands/spec.md`](009-commands/spec.md) for the rail infrastructure — unchanged):

- `StartPrivateStoreSellCommand implements NxCommand<StartPrivateStoreResult>` —
  `{ int charId, @Nullable String title, List<SellLine> lines }` (REQUIRED `charId` + non-empty
  `lines`; constructor-enforced for programmatic construction, wire-path re-validated by the handler
  → `VALIDATION_FAILED`).
- `StartPrivateStorePackageSellCommand` — identical shape, `packaged=true` semantics on the host
  side (all-or-nothing bundle).
- `SellLine { int itemId /* inventory instance object-id, NOT item-template id */, long count,
long priceAdena }` — **`priceAdena` is per-unit**; the host charges `count * priceAdena` for the
  whole stack. `count > 0` and `priceAdena >= 0` constructor-enforced.
- `StopPrivateStoreCommand implements NxCommand<StopPrivateStoreResult>` — `{ int charId }`.
- `StartPrivateStoreResult { String storeType, int acceptedCount, List<DroppedLine> dropped }` —
  `storeType` is an open host-defined token (not a closed adapter enum); `dropped` is non-null,
  empty when every line was accepted (partial acceptance is a normal, non-error outcome).
- `DroppedLine { int itemId, String reason }` — `reason` is host free-form diagnostic text, not a
  closed vocabulary on this contract (the concrete reason tokens a given host emits, e.g.
  `NOT_TRADEABLE` / `EQUIPPED` / `BAD_COUNT`, are a host-side (`bohpts-core`) convention documented
  in `nx-gameservers` spec 052 §6 — not enforced by this adapter-api type).
- `StopPrivateStoreResult { String previousStoreType }`.

Reserved for a future iteration, not shipped in this release: `StartPrivateStoreBuyCommand`
(private-store BUY / skupka — see `nx-gameservers` spec 052 §8).

Business rules (gates, online/offline positioning, item-lock semantics, own-scoped ownership
checks) live entirely in `nx-gameservers` / `bohpts-core` — this contract only names the fields.

## 4. `events.privatestore.Offer.packaged`

New `@Nullable Boolean packaged` field on `Offer` (the per-offer line of
`PrivateStoreSnapshotEvent`'s order book — see the `events.privatestore` family in the repo-root
[`CLAUDE.md`](../../CLAUDE.md)). `null` = legacy producer / not reported, treated as `false`
(regular `SELL`) by consumers. `nx-gs-commons` `PrivateStoreOfferHasher` mixes `packaged` into the
order-book hash (`PrivateStoreOfferHasher.hash`) — a `SELL`↔`PACKAGE_SELL` re-seat with otherwise
identical items/prices must still change the hash so the snapshot daemon re-publishes; without it
the change-detection would treat the two states as identical and never emit. `nx-gs-commons`
`OfferRow` carries the same field for the pre-hash intermediate representation.

## Compatibility

All four changes are additive — new enum constants, new nullable fields, new command/result/vocab
types. No existing wire shape changes meaning; an older consumer ignores unknown fields, an older
producer simply never sets them. Ships in the same `api/vX.Y.Z` release as the other three.

## Links

- `nx-gameservers/docs/specs/051-market-category-signals.md` — taxonomy owner for `EtcItemType` +
  `consumable`, deploy ordering across `nx-gs-adapter` → `bohpts-core` → `nx-gamedata` →
  `nx-gameservers`.
- `nx-gameservers/docs/specs/052-private-store-start-commands.md` — full gate inventory, platform
  wiring (permissions, controller, audit), host handler design for the command family in §3.
- `nx-gameservers/docs/specs/054-package-sell-offer-flag.md` — read-side filtering and package
  identity (`trader_id`-keyed) built on top of `Offer.packaged`.
- `nx-gamedata/docs/specs/020-item-template-consumable-column.md` — thin spec for the
  `gd_item_templates.consumable` column (adapter-owned `EXCLUDED` semantics), cross-linking back to
  `nx-gameservers` spec 051.
