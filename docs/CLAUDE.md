# Specs index

Feature specs live in `docs/specs/NNN-<feature>.md`, or `docs/specs/NNN-<feature>/spec.md` when a
feature carries companion docs (`module-discovery.md`, `flow.md`, `catalog.md`, `guide.md`). `NNN` is
a zero-padded sequential id, and one feature keeps one living spec — iterations update it instead of
taking a new number.

Any spec change (new spec, rename, re-scope, deletion) updates this table in the same pass.

| #   | Feature                           | Date       | File                                                   |
| --- | --------------------------------- | ---------- | ------------------------------------------------------ |
| 001 | adapter-bootstrap                 | 2026-04-27 | [spec](specs/001-adapter-bootstrap.md)                 |
| 002 | adapter-modules                   | 2026-04-28 | [spec](specs/002-adapter-modules/spec.md)              |
| 003 | db-sync                           | 2026-04-28 | [spec](specs/003-db-sync/spec.md)                      |
| 004 | jdbc-connection-source            | 2026-04-28 | [spec](specs/004-jdbc-connection-source.md)            |
| 005 | cdc-engine                        | 2026-04-29 | [spec](specs/005-cdc-engine/spec.md)                   |
| 006 | runtime-sync                      | 2026-05-01 | [spec](specs/006-runtime-sync.md)                      |
| 007 | per-server-sync                   | 2026-05-02 | [spec](specs/007-per-server-sync.md)                   |
| 008 | messaging                         | 2026-05-06 | [spec](specs/008-messaging.md)                         |
| 009 | commands                          | 2026-05-07 | [spec](specs/009-commands/spec.md)                     |
| 010 | commands-send-mail                | 2026-05-08 | [spec](specs/010-commands-send-mail.md)                |
| 011 | events-online-snapshot            | 2026-05-09 | [spec](specs/011-events-online-snapshot.md)            |
| 012 | snapshot-persistence              | 2026-05-17 | [spec](specs/012-snapshot-persistence.md)              |
| 013 | character-core-extension          | 2026-05-17 | [spec](specs/013-character-core-extension.md)          |
| 014 | events-raid                       | 2026-05-19 | [spec](specs/014-events-raid.md)                       |
| 015 | olympiad-events                   | 2026-05-22 | [spec](specs/015-olympiad-events.md)                   |
| 016 | character-online-hero             | 2026-05-29 | [spec](specs/016-character-online-hero.md)             |
| 017 | character-runtime-activity        | 2026-05-30 | [spec](specs/017-character-runtime-activity.md)        |
| 018 | castle-siege-sync                 | 2026-05-30 | [spec](specs/018-castle-siege-sync.md)                 |
| 019 | death-status-ratings-events       | 2026-05-31 | [spec](specs/019-death-status-ratings-events.md)       |
| 020 | item-stats-unification            | 2026-06-08 | [spec](specs/020-item-stats-unification.md)            |
| 021 | force-resync                      | 2026-06-12 | [spec](specs/021-force-resync.md)                      |
| 022 | class-canonicalization            | 2026-06-13 | [spec](specs/022-class-canonicalization.md)            |
| 023 | platform-sync-fixes-2026-06       | 2026-06-29 | [spec](specs/023-platform-sync-fixes-2026-06.md)       |
| 024 | ban-commands                      | 2026-06-29 | [spec](specs/024-ban-commands.md)                      |
| 025 | chat-events                       | 2026-06-29 | [spec](specs/025-chat-events.md)                       |
| 026 | item-augmentation-sync            | 2026-07-18 | [spec](specs/026-item-augmentation-sync.md)            |
| 027 | etctype-consumable-market-signals | 2026-07-22 | [spec](specs/027-etctype-consumable-market-signals.md) |
| 028 | character-inventory-capacity      | 2026-07-26 | [spec](specs/028-character-inventory-capacity.md)      |
| 029 | character-class-state-sync        | 2026-07-27 | [spec](specs/029-character-class-state-sync.md)        |
| 030 | gamedata-sync                     | 2026-08-15 | [spec](specs/030-gamedata-sync.md)                     |
| 031 | character-log-events              | 2026-09-01 | [spec](specs/031-character-log-events.md)              |

Companion docs of the folder-form specs:

- `002-adapter-modules/module-discovery.md` — general SPI mechanics for any tier (`ServiceLoader`,
  service descriptors, common authoring mistakes).
- `003-db-sync/module-discovery.md` — Tier-2 `DbSchemaProvider` discovery for schema authors.
- `005-cdc-engine/flow.md` — block diagrams of the two-phase cycle.
- `009-commands/catalog.md` — per-command wire contract (inputs, results, error statuses).
- `009-commands/guide.md` — handler author's guide (lifecycle, threading, registration).
