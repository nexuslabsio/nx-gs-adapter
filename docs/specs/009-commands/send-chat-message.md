# Commands — SendChatMessage

> Owner: @n1rmata

Outbound half of the `chat` family (`SendChatMessageCommand`, platform → host). The inbound fact
`ChatMessageEvent` and the problem statement live in [`025-chat-events.md`](../025-chat-events.md);
requirements keep their original numbers (R8–R15) so existing references stay valid. The whisper /
party / alliance / item-link slice continues as R22–R27 (R16–R21 live in the events spec).

**Counterpart spec (cross-repo):** `nx-gameservers/docs/specs/073-chat.md` — announcement-scheduler
cutover and the platform send path.

## Problem

The reverse direction of the chat feature. Writing into game chat from outside used to be
impossible except for one narrow path — `AnnounceNowCommand`, which broadcasts to the announcement
channel and nothing else. The mini app (Telegram, VK and Discord surfaces) needs to post into clan chat as the player's own character
(including while that character is offline), and announcements need to reach a player's private chat
under an arbitrary display name ("System", "Дед Мороз").

## Requirements

- [done] R8. `nx-gs-adapter-api.kafka.commands.chat.SendChatMessageCommand` MUST ship as the single
  generic "put this text into game chat" command, implementing `NxCommand<SendChatMessageResult>`.
  Per-command wire contract lives in [`009-commands/catalog.md`](catalog.md); the
  design decisions behind its shape are here.

  **Sender is two independent things, and the contract keeps them apart.** `senderCharacterId` is who
  speaks legally — it drives the host's gates, the packet's `objectId` and the platform's attribution.
  `senderDisplayName` is what the client renders. A mini-app message carries both (character plus its
  own name with a suffix); a "Дед Мороз" announcement carries only the second. Collapsing them into a
  single field cannot express either the persona or the audit trail.

  **The display name is composed platform-side, in full, for nameless senders.** (With a
  `senderCharacterId` the host builds the name instead, R23.) The host writes the string as given —
  `CreatureSay` serializes the sender name with `writeS`, so an arbitrary string renders without any
  client change. Keeping composition on the platform means the suffix format changes without an
  adapter or game-core release. This relies on the standing rule that the adapter trusts the platform.

  **`audience` is an axis of its own**, orthogonal to `channel`: `CHARACTER` | `CLAN` | `ALLIANCE` | `PARTY` | `ALL_ONLINE`.
  A whisper to one player and a whisper fanned out to everyone online are the same frame with
  different recipient lists. Without the axis, DM announcements would need a second command carrying a
  copy of every field.

  **`messageId` (UUIDv7) is supplied by the platform and becomes the `eventId` of the echo event**
  (R9). Re-issuing the command after a reply timeout therefore converges on one stored row instead of
  two. The host is expected to keep a bounded window of seen ids; without that window the field exists
  but the property does not.

  **Origin travels on the command, not by inference.** `source` is required and echoed into the
  event metadata: the host has no way to tell which platform surface issued a message, and
  without the marker the stored corpus cannot separate platform traffic from what players typed
  in-game — which is the distinction every abuse query starts from.

  **Accepted channels are a whitelist that grows per slice.** The first slice accepted `CLAN` and `ANNOUNCEMENT`; the current whitelist is `CLAN`, `ALLIANCE`,
  `PARTY`, `WHISPER`, `ANNOUNCEMENT` (R22). Anything outside the current whitelist is answered `VALIDATION_FAILED`. `CRITICAL_ANNOUNCEMENT` is
  deliberately absent — see R11.

- [done] R9. The host handler MUST publish a `ChatMessageEvent` for every message it sends, reusing
  `messageId` as `eventId` and marking origin in `metadata`. Otherwise platform-originated messages
  never reach the chat table, the RMT corpus, or the live stream other clan members read, and chat
  ends up with two sources of truth.

- [done] R10. The host handler MUST run the same gates as the native chat handler for the target
  channel — for `CLAN` that is `isChatBanned` + `Config.BAN_CHAT_CHANNELS` and the academy level floor;
  the other channels have their own gates (R22). Skipping them turns the command into a chat-ban bypass.
  A shadow-banned speaker (and text matching the host's broadcast filter) is treated as in game: not
  refused — that would reveal the ban — but answered `OK` with nothing delivered, and the echo carries
  `metadata.shadowed=true` so the platform stores it and shows it to the speaker only. An offline speaker
  is resolved through the clan table rather than a live `Player`, and the clan broadcast reaches the
  online members.

## Absorbing `AnnounceNowCommand`

Announcements are not a separate subsystem: `Broadcast.announceToOnlinePlayers` builds
`new CreatureSay(0, 10, "", text)` and sends it to every online player — the same packet class the
chat handlers use. In command terms that is exactly `senderCharacterId: null`,
`senderDisplayName: ""`, `channel: ANNOUNCEMENT`, `audience: ALL_ONLINE`. So `SendChatMessageCommand`
supersedes `AnnounceNowCommand` rather than living beside it.

- [done] R11. The `critical` flag MUST NOT be carried over. It is visually near-worthless on the
  host client, the front-end already always sends `critical: false`, and its only channel
  (`CreatureSay` type 18) is the one where the clickable-link token `[=url=]` renders literally.
  Dropping it removes the trap along with the flag. If a real need appears later, it comes back as its
  own change.

- [done] R12. Text handling (`\n` split into physical lines, wrapping bare `http(s)://` URLs in the
  host's clickable-link token, trimming trailing punctuation out of the wrapper) MUST move into the
  new handler, and `AnnounceNowHandler` MUST be rewritten as a thin delegate over it. Then the two
  paths agree by construction rather than by reviewer attention.

**Rollout is constrained by the host's release cadence: the platform deploys at any time, the game
core's jar applies only at the morning restart.** Announcements must not break in the window where the
platform is new and the host is old.

`CommandStatus.UNSUPPORTED_COMMAND` already covers it — the dispatcher replies with it when no handler
is registered for the `Nx-Message-Type`. That is an explicit, fast, per-server negative.

> Considered and rejected: feature detection through `CommandsStats.registeredTypes`, which rides the
> heartbeat topic. No platform service consumes heartbeats at all, so this would mean standing up a
> consumer for a whole family to read one flag.

- [done] R13. **Phase 1 (expand).** The platform's announcement scheduler sends the new command and,
  on an `UNSUPPORTED_COMMAND` reply, immediately re-sends the legacy `AnnounceNowCommand`. The
  fallback is counted by a metric. `AnnounceNowHandler` stays registered host-side.
- [done] R14. **Phase 2 (contract).** Once the fallback stops firing on every **live** server — the
  trigger is that observation, not a date — the fallback, `AnnounceNowHandler`, `AnnounceNowCommand`
  and `AnnounceResult` are all removed. Naming the trigger is what makes the compatibility layer a
  phase instead of a permanent straddle. Met once every live server answers the new command with `OK`;
  a non-production build that never received the handler is excluded and stays on the legacy path
  until its branch converges with the release line.
- [done] R15. Removing the command from the api module MUST NOT strand the platform's command audit.
  `Command.ANNOUNCE_NOW` on the platform side is not only a dispatch type: it is also the discriminator
  persisted on every historical audit row, so deleting it breaks reading them. The platform migrates
  those rows onto `SEND_CHAT_MESSAGE` in the same release — see `nx-gameservers/docs/specs/073-chat.md`
  §5.4. Nothing is required of the adapter beyond dropping the classes in `api/v0.87.0`; hosts pinned
  to `0.86.0` keep compiling.

## Whisper, party, alliance and item links

- [done] R22. The channel whitelist MUST grow to `CLAN`, `ALLIANCE`, `PARTY`, `WHISPER`,
  `ANNOUNCEMENT`, and `ChatAudiences` MUST gain:
  - `ALLIANCE` — `audienceId` = allianceId;
  - `PARTY` — `audienceId` = `null`; the host resolves the party from the sender, who therefore MUST
    be online and in a party, else `INVALID_STATE`;
  - `CHARACTER` (whisper) is now allowed for an **offline** addressee. The host runs the same gates as
    the in-game whisper — sender chat ban, min level 40, the addressee's block list (read from the DB
    when offline), message refusal mode — sends a packet only if the addressee is online, and
    publishes the echo with `targetCharId` set either way (events R20).

- [done] R23. **Sender identity.** `senderDisplayName` is nullable when `senderCharacterId` is
  set. The host builds the name itself, including the `*` postfix for an offline speaker (events
  R16), and ignores a value sent alongside a sender; the platform sends none. For nameless
  announcements (`senderCharacterId == null`) it stays required (empty string = nameless line).

- [done] R24. **Whisper addressee.** The command gains `@Nullable String targetCharacterName`. For
  audience `CHARACTER` exactly one of `audienceId` (character id) / `targetCharacterName` MUST be
  given; the platform sends the name typed in the UI, and the host resolves it by name, stripping a
  trailing `*`. Both or neither is `VALIDATION_FAILED`; an unknown name is `NOT_FOUND`.

- [done] R25. **`text` carries game-native item tokens**, built by the platform (plain text, LF
  breaks and bare URLs keep the R12 micro-format; item links are the one addition). The host parses
  tokens like the native `Say2` handler (by `ID=`), verifies ownership — online: the live inventory on
  the game thread; offline: the DB, location `INVENTORY` / `PAPERDOLL` only — and rebuilds a canonical
  token. A foreign or missing item is `VALIDATION_FAILED`. Anything outside a valid token (stray
  `\b`, control characters) is stripped, and the rest is filtered with the game chat whitelist
  (ASCII 32-126, Cyrillic U+0400-04FF, Latin-1 U+00C0-00FF).

- [done] R26. **`source` for the mini app is `MINIAPP`** (the legacy value `TMA` existed; the mini
  app now runs on Telegram, VK and Discord, so the old name misleads). `source` stays an open
  string; stored `TMA` rows are not rewritten.

- [done] R27. The echo (R9) for the new channels carries `senderDisplayName`,
  `recipientCharacterIds` (`PARTY`) and `items` (R25) as defined in the events spec, and is published
  only after delivery (events R19); for an offline addressee it is published although no packet went
  out, and for a shadowed speaker (R10) it is published with `shadowed=true` although nothing went
  out. Host-internal, not wire: a bounded item snapshot cache (objectId -> item info, TTL 6h, re-link
  refreshes) lets in-game players open links sent by offline characters.

- [done] R29. **`text` is limited to 300 characters on the host.** A platform MAY enforce a lower
  limit of its own; it must never exceed the host's, otherwise a message accepted by the platform fails
  on the host with `VALIDATION_FAILED`.

- [done] R30. **`ANNOUNCEMENT` has no speaker, so `senderDisplayName` may be `null`.** For channel
  `ANNOUNCEMENT` the host accepts `senderDisplayName = null` (a nameless line); the "required without
  `senderCharacterId`" check of R23 is by presence, and a blank `""` used to pass it only because it
  was non-null. Absence is `null`, not an empty string. The platform keeps sending its current value
  until hosts with this change are restarted (platform-side follow-up), so both forms must be accepted
  and rendered as the same nameless line.

## Topic & wire summary

| Item              | Value                                                   |
| ----------------- | ------------------------------------------------------- |
| Command topic     | `<tenant>.gs.commands` (shared, routed by message type) |
| `Nx-Message-Type` | `SendChatMessageCommand`                                |
| Idempotency       | `messageId` (UUIDv7), at-most-once delivery             |

## Compatibility

Additive on release (R22-R27 and R30 too: one new nullable field, new audience and channel codes; a host older
than the slice answers `VALIDATION_FAILED` for them): a host built against an older api never registers the handler, and the platform
sees `UNSUPPORTED_COMMAND` — exactly the signal phase 1 relies on. The removal in R14 is the only
breaking step, and it is gated on the fallback metric.

## Non-goals

- **Catch-up delivery to offline recipients.** A whisper to an offline addressee is gated, echoed and
  stored (R22), but no packet is queued for later; an offline inbox is a platform-side design, not a
  wire concern.

## Links

- Chat events: [`025-chat-events.md`](../025-chat-events.md)
- Command runtime + handler authoring: [`spec.md`](spec.md), [`guide.md`](guide.md),
  [`catalog.md`](catalog.md)
