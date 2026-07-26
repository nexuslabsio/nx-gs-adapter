# Character inventory capacity on the runtime wire (wire summary)

> Owner: @n1rmata

## Problem

`nx-gameservers` docs/specs/061-character-inventory-capacity.md needs a character's inventory
capacity — occupied/max regular slots, occupied/max quest slots, current/max carried weight — so a
Telegram Mini App can render an inventory screen with the same numbers the player sees in game.

Neither channel could carry it before:

- **db-sync** cannot: both caps are stat-derived (slot cap = config base per race / access level plus
  an inventory-limit stat plus a per-character purchased expansion, clamped by a server maximum;
  weight cap = a weight-limit stat over a CON-derived base and bonus multipliers). None of that is
  reconstructible from the persisted character row a schema provider reads.
- **runtime-sync** is the right channel — it already carries the stat-derived vitals for the same
  reason.

This is the **adapter-side thin summary**; `nx-gameservers` owns the read API and the UI contract.
No new design decisions are made here.

## `CharacterRuntimeDto` — 6 new optional fields (`kafka.sync.runtime.character`)

| field                                               | semantics                                                                                                                                     |
| --------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `curInventorySlots` / `maxInventorySlots`           | occupied / max regular inventory slots. One slot per item stack; equipped items count; **quest items do not**                                 |
| `curQuestInventorySlots` / `maxQuestInventorySlots` | occupied / max quest inventory slots — a separate container with its own cap                                                                  |
| `curWeight` / `maxWeight`                           | current / max carried weight, summed over the whole inventory (regular + equipped + quest), minus any build-specific weight-penalty reduction |

All `@Nullable Integer`. `null` = the producer did not report it (no signal, not zero), and `null` on
offline tombstones like every other volatile field — platform consumers keep the last-known values.

The regular/quest slot split is not a platform invention: L2 gates the two containers against
separate caps, and the game client shows quest items in their own tab. Modelling them as one pair
would lose the distinction a consumer needs. Quest items carry no distinct `ItemLocation` — they sit
in `INVENTORY` — so the quest tab is reconstructed from the item template's `questItem` flag (already
on the gd-sync wire) plus this quest-slot pair.

Field + getter + builder + `toBuilder` / `equals` / `hashCode` / `toString` added per the module's
POJO convention. The six parameters are appended at the END of the canonical constructor, which grows
16 -> 22 args.

**No back-compat constructor overload.** The first cut kept the old 16-arg constructor delegating with
`null`, which looks harmless and is not: these DTOs carry no binder annotations and bind through
implicit constructor-parameter names (the module compiles with `-parameters`), which only resolves
while exactly ONE non-default constructor is visible. With two, creator detection goes ambiguous and
a consumer throws `InvalidDefinitionException` on every record — and `nx-gameservers`
`SyncEventConsumer` catches parse failures per record, WARNs and acks, so the entire
runtime-character channel (vitals, coordinates, activity, exp, presence, capacity) would have frozen
silently. Reproduced against Jackson 3 before removing it; guarded by
`CharacterRuntimeDtoTest.class_shouldExposeExactlyOneConstructor` and, consumer-side, by
`nx-gameservers` `CharacterRuntimeWireBindingTest`. Grow this wire by appending constructor
parameters, never by overloading. Positional call sites of the 16-arg form must add six `null`s
(none exist outside this repo's tests — hosts use `builder()`).

Host side (out of scope for this repo — see `nx-gameservers` spec 061): the provider reads the
engine's own capacity accessors on the runtime snapshot thread (all O(1) field reads / cheap stat
calls) and adds the six values to the runtime hash, so a pickup or a cap change propagates on the
next tick.

## Release

Tag `api/vX.Y.Z` → `nx-gs-adapter-api`. Purely additive; consumers on the previous version keep
working and simply see the fields absent.
