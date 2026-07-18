# character-core-extension

> Owner: @n1rmata

## Problem

Платформенная `CharacterDbDto` (CDC) и `CharacterRuntimeDto` (runtime) на
этот момент несут только базовый identity / progression / vitals набор.
Для построения generic админ-страницы «Персонажи» в UI на любой L2-сборке
не хватает: владельца аккаунта, статуса нобля, soft-delete временной
отметки и явного online-флага. CDC не подходит для online — login/logout
происходит чаще, чем cycle тика. Нужен компактный extension wire-DTO +
расширение runtime-канала с явной разметкой online/offline, который не
тянет build-specific костыли (lock-флаги, blockedEXP, IP/HWID — см.
[`character-deferred-fields.md`](../../character-deferred-fields.md)).

## Requirements

**Must:**

- [done] R1. `CharacterDbDto` MUST add optional fields `accountName: String`,
  `nobless: Boolean`, `scheduledDeletionAt: Instant`, `online: Boolean` — все nullable;
  провайдеры, чьи источники не несут соответствующие колонки, оставляют
  поля `null`. `id` и `name` стали **required** (constructor throws NPE
  if name is null) — это структурно гарантируется любой L2J-схемой;
  schema provider MUST skip dirty rows перед DTO assembly (см. bohpts
  `CharacterMapping.mapEntity` который return'ит `null` для row'ев с null
  `char_name`, что engine трактует как «skip this pk this cycle»).
- [done] R2. `CharacterRuntimeDto` MUST add optional field
  `online: Boolean`. Wire-конвенция (byte-budget):
    - регулярная live-state строка: `online = null` (omit-on-wire через
      `serializeNulls=false`); потребитель MUST трактовать
      omitted / `null` как `true`;
    - one-shot OFFLINE-tombstone: `online = false` explicit, vitals /
      координаты `null` (omitted); строка существует только для flip'а
      platform-side presence;
    - `online = true` explicit допускается, но лишний — provider должен
      предпочесть `null` для regular ONLINE.
- [done] R3. Bohpts `CharacterMapping` MUST surface новые поля через
  hashed-columns `account_name`, `nobless`, `deletetime` поверх
  существующих. Source sentinel `deletetime = 0` → `null` (по
  L2J-конвенции «не помечен на удаление»). Source `nobless` (tinyint
  0/1) → Boolean.
- [done] R4. Bohpts `CharacterRuntimeMapping` MUST держать
  in-memory `previousOnline: Set<Long>` идентификаторов между тиками.
  За каждый `snapshot()` mapping:
    - эмитит регулярную `CharacterRuntimeDto` (vitals + coords) для
      каждого текущего online-игрока с `online = null`;
    - эмитит one-shot OFFLINE-tombstone (`CharacterRuntimeDto{id,
      online=false}`) для каждого id, который был в `previousOnline`,
      но отсутствует в текущем online-set;
    - перезаписывает `previousOnline = currentOnline` — tombstones не
      переносятся в следующий тик.
- [done] R5. `CharacterRuntimeMapping.hash(dto)` MUST включать
  `online`-поле в FNV-1a свёртку. Это гарантирует, что ONLINE→OFFLINE
  переход проходит engine's hash-diff и публикуется.
- [done] R6. Adapter restart семантика: после рестарта
  `previousOnline` пуст; mapping НЕ эмитит OFFLINE-tombstones для
  игроков, которые залогаутились пока adapter был down. Восстановление
  таких stale `online=true` строк — забота platform-side sweeper
  (см. `nx-gameservers/docs/specs/032-character-core-extension/spec.md`
  R10).

- [done] R12. Discrete `CharacterPresenceEvent` (events.character family,
  single-event family) MUST содержать `eventId: UUIDv7` (REQUIRED, для
  derive `occurredAt`), `charId: long` (REQUIRED), `online: boolean`
  (REQUIRED, `true`=login / `false`=logout), `accountName`/`ip`/`hwid`
  (optional). Партиционирование по `charId`. Bohpts emit'ит из standard
  packet path (`RequestEnterWorld` для login, `Player.deleteMe()` для
  logout) через `CharacterPresencePublisher.publishLogin(player)` /
  `publishLogout(player)`, bound в `BohptsEventsModule.onConnect`. Cheat /
  custom clients bypass'ят packet path → no event — fallback на runtime
  tombstones и CDC online.

- [done] R13. `CharacterDbDto.online` (CDC source) + runtime tombstones
    + discrete events MUST все идти в одну колонку `gs_characters.online`
      с timestamp-gated UPSERT (newest `last_seen_at` wins). Чтение —
      тривиальный `SELECT online`.

**Should:**

- [done] R7. `CharacterRuntimeDto.online` SHOULD не сериализовываться
  на wire когда `null` (`serializeNulls=false` — default для wire
  producer'а). На пиковом online-population ~5–10k characters / tick
  это даёт ~50-100KB/мин экономии по сравнению с явным `true` на
  каждой live-state строке.

**Could:**

- [todo] R8. Mapping COULD держать карту recently-offlined IDs с TTL
  для re-emit OFFLINE при detected ack-failure на следующем тике.
  Сейчас полагаемся на platform-side idle sweeper — простоев на
  одиночные потерянные acks ~1 мин.

**Non-goals:**

- `accessLevel`, `lastAccess`, `onlineTime`, `hero`, lock-флаги,
  blockedEXP, last-login IP/HWID, character_auth_history — отложены
  отдельными слайсами (см. [`character-deferred-fields.md`](../../character-deferred-fields.md)).
- Discrete login/logout event-family на отдельном канале — runtime
  presence канала достаточно.
- Engine-level (vs mapping-level) tombstone механизм — не нужен;
  semantics за mapping'ом.

## Open questions

- [assumed: 60s idle-threshold platform-side покрывает потерянные
  OFFLINE-tombstones и adapter-restart сценарии без false
  flickering под coarse network blips (10s tick × 6 = 60s safety).]
- [assumed: `online` колонка в `characters` (L2J)
  *не* читается через CDC — engine использует только runtime
  канал для presence. Альтернатива (CDC online) даёт BD-authoritative
  truth но загромождает CDC UPDATE'ами login/logout.]

## Links

- Parent feature: [`docs/specs/003-db-sync/spec.md`](../003-db-sync/spec.md) —
  CDC мapping для `CharacterDbDto`.
- Parent feature: [`docs/specs/006-runtime-sync/spec.md`](../006-runtime-sync/spec.md) —
  runtime engine для `CharacterRuntimeDto`.
- Deferred backlog: [`character-deferred-fields.md`](../../character-deferred-fields.md).
- Platform side: `nx-gameservers/docs/specs/032-character-core-extension/`.
