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
[Deferred fields and filters](#deferred-fields-and-filters-backlog)).

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
  - discrete events MUST все идти в одну колонку `gs_characters.online`
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
  отдельными слайсами (см. [Deferred fields and filters](#deferred-fields-and-filters-backlog)).
- Discrete login/logout event-family на отдельном канале — runtime
  presence канала достаточно.
- Engine-level (vs mapping-level) tombstone механизм — не нужен;
  semantics за mapping'ом.

## Open questions

- [assumed: 60s idle-threshold platform-side покрывает потерянные
  OFFLINE-tombstones и adapter-restart сценарии без false
  flickering под coarse network blips (10s tick × 6 = 60s safety).]
- [assumed: `online` колонка в `characters` (L2J)
  _не_ читается через CDC — engine использует только runtime
  канал для presence. Альтернатива (CDC online) даёт BD-authoritative
  truth но загромождает CDC UPDATE'ами login/logout.]

## Links

- Parent feature: [`docs/specs/003-db-sync/spec.md`](003-db-sync/spec.md) —
  CDC мapping для `CharacterDbDto`.
- Parent feature: [`docs/specs/006-runtime-sync.md`](006-runtime-sync.md) —
  runtime engine для `CharacterRuntimeDto`.
- Deferred backlog: [Deferred fields and filters](#deferred-fields-and-filters-backlog).
- Platform side: `nx-gameservers/docs/specs/032-character-core-extension/`.

---

## Technical design

### Overview

Расширение существующих character DTO и bohpts mapping без новых модулей
и без изменения engine internals. Все изменения локализованы:

- `nx-gs-adapter-api`: новые поля в двух POJO DTO + Builder methods.
- `bohpts-core` (`l2e.gameserver.l2nx.sync.db.CharacterMapping`): пара
  новых hashed columns + readers в `CharacterRow`.
- `bohpts-core` (`l2e.gameserver.l2nx.sync.runtime.CharacterRuntimeMapping`):
  in-memory online-set diff + tombstone-генерация.

### Wire schema deltas

#### `CharacterDbDto` (kafka.sync.db.character)

```java
+ @Nullable String  accountName;   // characters.account_name
+ @Nullable Boolean nobless;       // characters.nobless (tinyint 0/1)
+ @Nullable Instant scheduledDeletionAt;    // characters.deletetime (BIGINT epoch ms; 0→null)
```

JSON:

```json
{
  "id": 54321,
  "name": "Cliodhna",
  "accountName": "kiryl@nexus",
  "nobless": true,
  "scheduledDeletionAt": "2026-06-01T12:00:00Z",
  ...
}
```

#### `CharacterRuntimeDto` (kafka.sync.runtime.character)

```java
+ @Nullable Boolean online;        // null/true=ONLINE; false=OFFLINE tombstone
```

Wire (live-state row, online):

```json
{ "id": 54321, "curHp": 4321, ... }   // online omitted → ONLINE
```

Wire (offline tombstone):

```json
{ "id": 54321, "online": false } // vitals null → omitted
```

### Mapping-side details

#### bohpts CharacterMapping

`HASHED` extended:

```java
List.of("char_name", "account_name", "level", "sex", "race",
        "classid", "base_class", "clanid", "pvpkills", "pkkills",
        "karma", "title", "nobless", "deletetime")
```

`mapRow` reads:

- `account_name` (varchar) via `rs.getString`
- `nobless` (tinyint) via `JdbcNulls.nullableInt`, mapped to `Boolean`
  via `code != 0`
- `deletetime` (bigint epoch ms) via `JdbcNulls.nullableLong`, mapped
  to `Instant.ofEpochMilli` with `0` → `null`

UPDATE storm risk: account_name (changes only on `TransferCharToAccount`
command, rare), nobless (one-time quest), deletetime (mark-for-delete
flow, rare). Все безопасны в hashed.

#### bohpts CharacterRuntimeMapping

State:

```java
private final Set<Long> previousOnline = new HashSet<>();
```

`snapshot()` алгоритм:

```
currentOnline = []
rows = []
for player p in GameObjectsStorage.getPlayers() where p.isOnline():
    currentOnline.add(p.objectId)
    rows.add(RuntimeRow{p.objectId, toDto(p)})   // online=null

for id in previousOnline:
    if id not in currentOnline:
        rows.add(RuntimeRow{id, offlineTombstone(id)})   // online=false

previousOnline = currentOnline    # tombstones NOT carried forward
return rows
```

`hash(dto)` теперь mix'ит `online` в FNV-1a (если non-null).

Single-instance assumption: mapping держится одним
`BohptsRuntimeStateProvider` instance'ом, который ServiceLoader
инстанцирует ровно один раз на JVM. Поэтому stateful поле
`previousOnline` корректно. Если SPI начнёт инстанцировать mapping per
tick — feature сломается; safeguard через JUnit-тест отсутствует.

### Data flows

1. **Tick K** (steady-state, 9.5k online players):
   - mapping строит `currentOnline` (9.5k entries), создаёт 9.5k
     ONLINE rows, ноль tombstones (никто не logout'нулся прямо в
     момент тика — типичный случай).
   - engine hash-diff'ит: для ~95% IDs hash unchanged → 0 publishes.
     Для ~5% IDs (moved / damaged / healed) → CHANGED publishes.
   - `previousOnline = currentOnline`.
2. **Tick K+1** (logout: 30 игроков):
   - mapping строит `currentOnline` (9470 entries).
   - removed = {30 IDs}. mapping добавляет 30 OFFLINE-tombstones к
     rows.
   - engine: 30 OFFLINE rows имеют new hash (включая online=false),
     CHANGED publishes на topic.
   - `previousOnline = currentOnline` (без removed-30). Tombstones
     забыты.
3. **Tick K+2** (offlined игроки не возвращаются):
   - mapping не видит ушедших в previousOnline → не эмитит повторных
     tombstones. Чистая seam между mapping-уровневой presence и
     engine-уровневой hash-diff.

### Adapter restart

`previousOnline` сбрасывается. На первом тике mapping эмитит full
snapshot ONLINE rows для всех текущих игроков; OFFLINE-tombstones для
игроков, ушедших пока adapter был down — НЕ эмитятся (mapping не
помнит). Эти строки остаются `online=true` на платформе до
срабатывания idle sweeper'а (см. `nx-gameservers/.../character-core-extension`).

### Memory budget

`HashSet<Long>` с ~10k entries: ~640 KB (Long boxing + HashMap overhead).
Если станет узким местом — replace на napile `LongOpenHashSet` (уже в
classpath bohpts через napile-1.0.5b.jar — но API не доступен с
`LongSet`-shape). Сейчас не bottleneck.

### Integration points

- **nx-gs-adapter-api 0.26+** — две POJO DTO с новыми полями. Wire-back-compat:
  старые consumer'ы игнорируют unknown fields (Jackson / Gson
  тренируется на `fail-on-unknown-properties=false` / `lenient`).
- **bohpts-core** — обновление CharacterMapping + CharacterRuntimeMapping.
  Compile-time bump nx-gs-adapter-api.
- Platform side: `nx-gameservers` ingestit новые поля + presence (см.
  parallel feature на той стороне).

### Decisions

- **`online` как Boolean, не enum.** На high-load (10k chars × 6 tick/min)
  enum-строки `"ONLINE"`/`"OFFLINE"` через Gson — лишние 50–100 KB/min.
  Boolean → 5 bytes (`false`); omit-on-null → 0 bytes для регулярных
  ONLINE. Семантика выводится из null-конвенции, не из distinct enum.
- **Stateful mapping для presence vs engine-level reset marker.**
  Mapping-уровневая previous-online — это локальное know-how одного
  entity, не требует расширения engine API. Reset-marker на wire (как
  protocol-level signal от адаптера к платформе) был бы менее
  generic — потребовал бы reset-semantics в engine. Idle sweeper
  на платформе закрывает stale online после adapter restart дешевле.
- **Tombstones НЕ carried forward после первого emit.** Если engine ack
  на OFFLINE-tombstone failed — потеряли. Платформа закроет дыру через
  idle sweeper (~1 мин). Retry-buffer в mapping (recently-offlined +
  TTL) запланирован как future option (R8 Could).

### Extension points

- **Новое тип presence (например, AWAY)** — расширить семантику `online`
  на третий state. Boolean ограничен — пришлось бы вернуться к enum.
  Решение откладывается до реального ops case.
- **Per-server-id presence в platform-side reconciliation** — уже
  поддерживается через `Nx-Server-Id` header на runtime topic.

---

## Deferred fields and filters (backlog)

Поля и фильтры платформенной character-модели, которые сознательно отложены и не входят в текущий core. Когда созреет
дизайн каждого блока — он добавляется отдельным слайсом.

### Deferred fields

| Field                                         | Reason for deferral                                                                                    |
| --------------------------------------------- | ------------------------------------------------------------------------------------------------------ |
| `lastAccess`                                  | Читать ли из `characters.lastAccess` или derive из online-set transitions — не решено.                 |
| `onlineTime`                                  | То же; вдобавок vanilla L2J пишет `onlinetime` только при logout, что даёт лаг в реальном времени.     |
| `hero`                                        | Generic L2-поле, но не критично для первой итерации UI.                                                |
| `blockedExp`                                  | Build-specific, тот же случай — конкретный ключ в `character_variables`.                               |
| Last-login `ip`, `hwid`                       | Требуют моделирования `character_auth_history` или эквивалента; отложено.                              |
| `coinsDeposited` (выведено)                   | Не character-domain, считается из shop/transfers сервиса. Cross-service read на платформенной стороне. |
| `tgUsers[]`                                   | Не character-domain, держится в nx-users / telegram-link.                                              |
| Clan `crestPath`, `allyCrestPath`, `clanName` | Резолвится через Clan entity, не дублируется в Character.                                              |

### Deferred filters

| Filter                              | Reason for deferral                                                                                                                               |
| ----------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ip`, `hwid`                        | Нет last-login IP/HWID полей в core, нет character_auth_history.                                                                                  |
| `lock-ip / lock-hwid / lock-items`  | Опираются на build-specific lock-флаги.                                                                                                           |
| `blocked-exp`                       | Опирается на build-specific флаг.                                                                                                                 |
| `unique-auth` (HWID / IP / HWID_IP) | Требует character_auth_history агрегата.                                                                                                          |
| `store-mode` (множественный)        | `privateStore` enum в DTO уже есть, но filter-семантика «несколько режимов одновременно» не специфицирована; добавим, когда фронт явно потребует. |
| `telegram-user`                     | Не character-domain (см. tgUsers выше).                                                                                                           |

### Shipped (вышло из backlog)

- `accessLevel` — синкается build-agnostic строкой (`CharacterDbDto.accessLevel`, `@Nullable String`): на int-сборках
  это числовой текст (`"7"`), на string-role сборках — имя роли verbatim; `null`, если источник не отдаёт. Без
  парсинга / enum (read через JDBC `getString`).
- Char-locks (`lockIp` / `lockHwid` / `lockItem`) — синкаются как `CharacterDbDto.locks: List<CharacterLockDbDto>`
  (`lockType` ∈ `WellKnownCharacterLockTypes` = IP / HWID / ITEM, `lockValue?`). Одна запись на каждый **активный**
  lock (значение present, non-blank, != `"0"`). Фильтры по lock-флагам (`lock-ip` / `lock-hwid` / `lock-items`)
  остаются отложенными — синкается только сам список.

### Notes

- Build-specific атрибут `blockedEXP` ждёт отдельного дизайна — generic key-value extension или typed-optional поля.
  Не моделируем, пока не появится core-agnostic форма.
- Любое поле/фильтр из этого backlog добавляется отдельным design spec'ом, не наскоро в существующий core.
