# character-core-extension — tech

> Covers: spec.md

## Overview

Расширение существующих character DTO и bohpts mapping без новых модулей
и без изменения engine internals. Все изменения локализованы:

- `nx-gs-adapter-api`: новые поля в двух POJO DTO + Builder methods.
- `bohpts-core` (`l2e.gameserver.l2nx.sync.db.CharacterMapping`): пара
  новых hashed columns + readers в `CharacterRow`.
- `bohpts-core` (`l2e.gameserver.l2nx.sync.runtime.CharacterRuntimeMapping`):
  in-memory online-set diff + tombstone-генерация.

## Wire schema deltas

### `CharacterDbDto` (kafka.sync.db.character)

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

### `CharacterRuntimeDto` (kafka.sync.runtime.character)

```java
+ @Nullable Boolean online;        // null/true=ONLINE; false=OFFLINE tombstone
```

Wire (live-state row, online):

```json
{ "id": 54321, "curHp": 4321, ... }   // online omitted → ONLINE
```

Wire (offline tombstone):

```json
{ "id": 54321, "online": false }      // vitals null → omitted
```

## Mapping-side details

### bohpts CharacterMapping

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

### bohpts CharacterRuntimeMapping

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

## Data flows

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

## Adapter restart

`previousOnline` сбрасывается. На первом тике mapping эмитит full
snapshot ONLINE rows для всех текущих игроков; OFFLINE-tombstones для
игроков, ушедших пока adapter был down — НЕ эмитятся (mapping не
помнит). Эти строки остаются `online=true` на платформе до
срабатывания idle sweeper'а (см. `nx-gameservers/.../character-core-extension`).

## Memory budget

`HashSet<Long>` с ~10k entries: ~640 KB (Long boxing + HashMap overhead).
Если станет узким местом — replace на napile `LongOpenHashSet` (уже в
classpath bohpts через napile-1.0.5b.jar — но API не доступен с
`LongSet`-shape). Сейчас не bottleneck.

## Integration points

- **nx-gs-adapter-api 0.26+** — две POJO DTO с новыми полями. Wire-back-compat:
  старые consumer'ы игнорируют unknown fields (Jackson / Gson
  тренируется на `fail-on-unknown-properties=false` / `lenient`).
- **bohpts-core** — обновление CharacterMapping + CharacterRuntimeMapping.
  Compile-time bump nx-gs-adapter-api.
- Platform side: `nx-gameservers` ingestit новые поля + presence (см.
  parallel feature на той стороне).

## Decisions

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

## Extension points

- **Новое тип presence (например, AWAY)** — расширить семантику `online`
  на третий state. Boolean ограничен — пришлось бы вернуться к enum.
  Решение откладывается до реального ops case.
- **Per-server-id presence в platform-side reconciliation** — уже
  поддерживается через `Nx-Server-Id` header на runtime topic.
