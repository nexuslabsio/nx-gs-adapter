# Per-Server Sync

## Problem

`nx-gs-adapter` публикует sync-события в per-tenant Kafka-топики
(`<tenantSlug>.gs.sync.db.{character,clan,item}`, `.runtime.character`), общие для
всех game-серверов тенанта. Сообщения сегодня не несут идентичность сервера, поэтому
на стороне `nx-gameservers` две записи character_id=1 от разных серверов одного
тенанта коллидят на PK `(tenant_id, id)` и затирают друг друга. Идентичность сервера
должна путешествовать с каждым sync-событием, чтобы consumer мог скоупить строки
per-server без изменений wire-схемы `SyncEvent<T>`.

Аудитория: adapter-side publisher'ы (`db-sync`, `runtime-sync`, `heartbeat`),
платформенный consumer `nx-gameservers`, `nx-tenants` (источник server-каталога),
операторы (управляют несколькими серверами в одном тенанте без коллизий).

## Requirements

**Must:**

- [done] R1. `nx-gs-adapter-api` MUST expose the wire-level header contract in
  a new class `app.l2nx.gs.adapter.api.kafka.NxHeaders`:
  - constant `String NX_SERVER_ID = "Nx-Server-Id"` — the header name
  - `static byte[] encodeUuid(UUID)` and `static UUID decodeUuid(byte[])` —
    encoding rule. Value is the raw 16-byte UUID: `mostSigBits` big-endian
    followed by `leastSigBits` big-endian, NOT the 36-char string form.
    Implementation is pure JDK (`java.util.UUID` + `java.nio.ByteBuffer`),
    no Kafka coupling — keeps `nx-gs-adapter-api`'s zero-runtime-deps rule
    (Java 8, jspecify only).
  - SC1. `decodeUuid` rejects any input ≠ 16 bytes with a clear
    `IllegalArgumentException`.

  This class is the single source of truth for both sides — `nx-gs-kafka`
  (adapter-side producer wrapper) and `nx-gameservers` (Spring-Kafka consumer)
  read the constant + helpers from the same artifact, no string duplication
  and no encoding drift. Future Kafka-transport headers (e.g. reply-flow
  correlation ids) land in the same class. The Kafka `NxHeaders`
  (`app.l2nx.gs.adapter.api.kafka.NxHeaders`) is intentionally a different
  type from the HTTP `NxHeaders` in `nx-libs/common` — different transport,
  different serdes, different packages; no compile-time collision.

- [done] R2. `nx-gs-kafka` MUST provide a thin Kafka-binding helper layered
  over `NxHeaders`: a producer-wrapper that stamps
  `Nx-Server-Id` on every outbound record via `record.headers().add(name,
NxHeaders.encodeUuid(serverId))`, plus an optional reader-helper
  `Headers → @Nullable UUID` for adapter-side tests / future reply-flow
  modules. `nx-gs-kafka` brings no new constants — just `org.apache.kafka.*`
  ↔ `NxHeaders` glue.

- [done] R3. Adapter-side producers MUST stamp `Nx-Server-Id` on every sync record
  published to topics under `ctx.syncTopics().db()` and `ctx.syncTopics().runtime()`.
  The serverId source is `ConnectContext.getServerId()`, captured once after
  `/connect` and reused for the lifetime of the adapter process. Sync modules
  (`db-sync`, `runtime-sync`) MUST NOT pass serverId per-call — wiring layer
  injects it transparently.
  - SC2. No `db-sync` / `runtime-sync` code references `serverId` directly when
    publishing — search-grep returns zero hits in their publisher classes.

- [done] R4. Heartbeat publisher MUST also stamp `Nx-Server-Id` on every
  `HeartbeatEvent`, even though server identity is already carried in the
  payload. Reason: consistency for any cross-cutting consumer (future
  observability / dead-letter / multi-tenant routing) that wants to read
  identity uniformly across all `nx-gs-adapter`-issued records pre-JSON-parse.

- [done] R5. `nx-gameservers` `SyncEventConsumer` MUST extract `Nx-Server-Id`
  from `ConsumerRecord.headers()` via `NxHeaders.decodeUuid(...)` BEFORE
  attempting JSON parse. The extracted `UUID` is then used together with
  `tenantId` (resolved from topic slug) to group the per-poll batch into
  per-`(tenantId, serverId)` chunks dispatched to the matching ingestor.

- [done] R6. `nx-gameservers` MUST `WARN + skip` per-record when:
  - `Nx-Server-Id` header is absent or has invalid length
  - serverId is not present in the per-tenant active-servers cache
    (unknown server for this tenant)

  Symmetric with the existing `unknown-tenant` handling — one bad record never
  poisons the rest of the batch; offset commit proceeds for the remaining
  successful records.

- [done] R7. `nx-gameservers` schema MUST use composite primary key
  `(tenant_id, server_id, id)` on parent tables (`characters`, `clans`,
  `items`); child tables (`character_subclasses`, `clan_skills`,
  `item_attributes`) MUST carry `tenant_id` + `server_id` + parent-id and FK
  back to the parent through all three columns with `ON DELETE CASCADE`.
  Implementation MUST be a forward-only Liquibase changeset that drops and
  recreates the affected tables (no backfill, no data migration — current data
  is test-only).
  - SC3. UPSERT idempotency: re-delivery of the same `(tenant_id, server_id,
id)` event produces the same row state.
  - SC4. DELETE event on a parent cascades to children via the three-column FK.

- [done] R8. `nx-gameservers` `TenantCache` (or its successor) MUST surface
  per-tenant active servers as a snapshot
  `Map<slug, TenantInfo{tenantId, Map<serverId, serverSlug>}>`. Refresh source
  is the extended `/internal/tenants` payload (R9). On cache-miss for a known
  tenant slug + unknown serverId the consumer triggers one single-flight
  refresh before giving up (same pattern as today's tenant-slug miss).

- [done] R9. `nx-tenants` `InternalTenantsApi` MUST return per-tenant active
  servers in the `GET /internal/tenants` payload, in-place — no separate
  endpoint, no versioned path. Shape:
  `List<TenantSummary{id, slug, servers: List<GameServerSummary{id, serverSlug}>}>`.
  Servers MUST be filtered to `active=true`. The endpoint stays JWT-bypass +
  nginx-ACL-gated; no permission checks. Single consumer today is
  `nx-gameservers`.

**Should:**

**Could:**

**Non-goals:**

- Изменение wire-схемы `SyncEvent<T>` или per-entity DTO в `nx-gs-adapter-api`
  (`CharacterDbDto`, `ClanDbDto`, …) — header'ом покрывается транспортный слой.
- Per-server топик-сплит (например `<slug>.<server>.gs.sync.db.character`) —
  топики остаются shared по тенанту, server — только metadata.
- Refactor multi-tenancy перимтра — `tenantSlug` остаётся в имени топика,
  он же source of truth.
- REST-поверхность `nx-tenants /servers` (CRUD-эндпоинты) — фича не трогает
  внешний API игроков/операторов, только internal cross-service контракт.
- Backwards-compat / data migration в `nx-gameservers` — данные тестовые,
  drop & recreate приемлемо.
- Дополнительные header'ы (`Nx-Tenant-Id`, `Nx-Tenant-Slug`, `Nx-Server-Slug`) —
  tenantSlug derivable из топика, остальное — из кэша.

### Edge cases

- Header `Nx-Server-Id` отсутствует на inbound record → WARN + skip (R5).
- Header присутствует, но длина ≠ 16 байт → WARN + skip (R5).
- Header валидный, но serverId не найден в кэше для известного тенанта
  (server деактивирован после рефреша / только что зарегистрирован) →
  single-flight refresh, при повторном промахе WARN + skip.
- Один tenant имеет ≥ 2 сервера, оба пишут одновременно одну partition'у
  (key = entityId одинаковый): per-server FIFO сохраняется через single-producer
  ordering, два сервера пишут в разные DB-строки — конфликта нет.
- Heartbeat от сервера, который отсутствует в кэше → WARN + skip симметрично
  с sync-событиями (если consumer применяет тот же контракт; для
  `nx-tenants` heartbeat может оставаться payload-driven, header чисто
  для observability).

## Open questions

- [assumed: heartbeat consumer на платформе остаётся payload-driven — header
  `Nx-Server-Id` на heartbeat-сообщении это just-in-case для будущих
  cross-cutting consumer'ов; никакая логика nx-tenants на нём не строится в
  рамках этой фичи]
- [assumed: PLAN-файл живёт в `docs/specs/007-per-server-sync/.plan/` как
  обычный specl-flow, milestone'ы по репам — adapter-api-contract /
  kafka-binding / adapter-wiring / nx-tenants-api / nx-gameservers-schema-and-consumer]
- "Where does the header constant + encoding live?" → resolved: split between
  `nx-gs-adapter-api.NxHeaders` (constant + pure-JDK encoding helpers,
  R1) and `nx-gs-kafka` (Kafka-Headers binding glue, R2). Both nx-gs-adapter
  and nx-gameservers depend on `nx-gs-adapter-api` directly, no string
  duplication, no encoding drift.

---

## Technical design

`per-server-sync` — транспортный слой на Kafka-headers, который добавляет
identity-маркер `Nx-Server-Id` (raw 16-byte UUID) на каждое sync-сообщение
adapter'а и расширяет consumer (nx-gameservers) до per-server-scoped ingestion.
Фича пересекает три репозитория: `nx-gs-adapter` (kafka-фасад + producer
wiring + sync/heartbeat модули), `nx-gameservers` (header extraction + schema
с composite PK + tenant-cache с server-каталогом), `nx-tenants` (internal API
отдаёт активные серверы тенанта). Wire-схема `SyncEvent<T>` в
`nx-gs-adapter-api` не меняется.

### Structure

- `nx-gs-adapter/nx-gs-adapter-api/src/main/java/app/l2nx/gs/adapter/api/kafka/NxHeaders.java`
  — wire-контракт: константа `NX_SERVER_ID` + pure-JDK helper'ы
  `encodeUuid(UUID)` / `decodeUuid(byte[])`. Single source of truth для
  имени header'а и encoding'а (16 raw bytes), используется обеими
  сторонами через зависимость на `nx-gs-adapter-api`.
- `nx-gs-adapter/nx-gs-kafka/src/main/java/app/l2nx/gs/kafka/producer/`
  — Kafka-binding wrapper над `NxProducer`, штампующий
  connection-scoped `Nx-Server-Id` через `NxHeaders.encodeUuid` на каждом
  `send(...)`. Опциональный reader-helper `Headers → @Nullable UUID` для
  adapter-side тестов и будущих reply-flow модулей.
- `nx-gs-adapter/nx-gs-adapter-core/src/main/java/app/l2nx/gs/adapter/core/`
  — место wiring'а: после успешного `/connect` adapter-core
  оборачивает базовый `NxProducer` в server-stamping wrapper, передаёт его
  модулям через `ConnectContext` (или через Tier-2 Kafka-publish capability,
  когда оно появится в SPI).
- `nx-gs-adapter/nx-gs-db-sync-core/`, `nx-gs-runtime-sync-core/` —
  publisher-классы переключаются с прямого `NxProducer` на server-stamping
  wrapper; per-call API не меняется.
- `nx-gs-adapter/nx-gs-adapter-core/.../heartbeat/HeartbeatService.java`
  — publish heartbeat через тот же wrapper.
- `nx-gameservers/src/main/java/app/l2nx/gameservers/infra/kafka/SyncEventConsumer.java`
  — pre-parse extraction header'а; dispatch group key меняется с
  `tenantId` на `(tenantId, serverId)`.
- `nx-gameservers/src/main/java/app/l2nx/gameservers/infra/tenants/TenantCache.java`
  — расширенная схема снапшота: `Map<slug, TenantInfo>` где
  `TenantInfo` несёт `tenantId` + `Map<serverId, serverSlug>`.
- `nx-gameservers/src/main/resources/db/liquibase/v1.0.0_baseline.sql` —
  baseline changeset обновлён in-place: 6 таблиц с трёхколоночным PK
  `(tenant_id, server_id, id)` на parent и FK + cascade на child. Данные
  testing-only, dev wipe'ает БД при apply.
- `nx-gameservers/src/main/java/app/l2nx/gameservers/ingest/*Ingestor.java`
  — UPSERT/DELETE SQL переключается на трёхколоночный PK; batch
  dedup ключ становится `(server_id, pk)`.
- `nx-tenants/src/main/java/app/l2nx/tenants/api/internal/tenant/InternalTenantsApi.java`
  — расширение payload'а контракта.
- `nx-tenants/src/main/java/app/l2nx/tenants/domain/tenant/TenantSummary.java`
  — добавление поля `List<GameServerSummary> servers`.
- `nx-tenants/src/main/java/app/l2nx/tenants/infra/postgres/`
  — query JOIN `tenants × game_servers WHERE active=true`.

### Key components

- **`NxHeaders` (Kafka)** (implements R1) — `app.l2nx.gs.adapter.
api.kafka.NxHeaders` в `nx-gs-adapter-api`. Публичная константа
  `NX_SERVER_ID = "Nx-Server-Id"` + pure-JDK helper'ы
  `encodeUuid(UUID): byte[]` и `decodeUuid(byte[]): UUID`. Encoding: 16 raw
  bytes, `mostSigBits` big-endian затем `leastSigBits` big-endian.
  `decodeUuid` кидает `IllegalArgumentException` при длине ≠ 16. Будущие
  Kafka-headers (reply-flow correlation, observability) — добавляются
  константами в этот же класс.
- **Server-stamping producer wrapper** (implements R2, R3, R4) —
  обёртка над `NxProducer` в `nx-gs-kafka`, держит `serverId` (16 байт raw,
  предкодированных через `NxHeaders.encodeUuid` один раз) в final-поле, на
  каждом `send(...)` достраивает `ProducerRecord.headers()`. Wrapper
  создаётся в `nx-gs-adapter-core` после `/connect`, передаётся sync-модулям
  и heartbeat-сервису; сами модули `serverId` напрямую не видят.
- **`SyncEventConsumer` (header extraction)** (implements R5, R6) —
  до `gson.fromJson(...)` читает `record.headers().lastHeader(NxHeaders.
NX_SERVER_ID)`, декодирует через `NxHeaders.decodeUuid`. Group key
  dispatch'а становится pair `(tenantId, serverId)`. Missing/invalid header →
  WARN + skip без подсчёта в byTenant. Unknown serverId для известного
  tenantId → WARN + skip после single-flight refresh кэша.
- **`TenantCache` extended snapshot** (implements R8) — снапшот
  `Map<slug, TenantInfo>` через `AtomicReference`, single-flight refresh
  через `AtomicBoolean`, hydrates из расширенного `/internal/tenants`. Метод
  `resolveServer(tenantId, serverId): @Nullable String slug` для логирования.
- **`InternalTenantsApi` extended payload** (implements R9) —
  controller отдаёт `List<TenantSummary>` где каждый `TenantSummary`
  включает `List<GameServerSummary>` с активными серверами. Repository-query
  делает `LEFT JOIN game_servers ON game_servers.tenant_id = tenants.id AND
game_servers.active = true` за один round-trip; mapper аггрегирует строки
  по `tenant.id`.
- **`nx-gameservers` schema baseline** (implements R7) —
  `v1.0.0_baseline.sql` обновлён in-place: 6 таблиц с трёхколоночным PK
  `(tenant_id, server_id, id)` на parent и трёхколоночным FK +
  `ON DELETE CASCADE` на child. Без отдельной миграции — данные
  testing-only, dev пересоздаёт БД при apply.

### Data flows

**Publish (adapter-side):**

1. db-sync / runtime-sync / heartbeat вызывает `producer.send(topic, key, message)`.
2. Wrapper строит `ProducerRecord(topic, partition=null, key, value)`,
   докладывает `Nx-Server-Id` в `record.headers()`.
3. KafkaProducer сериализует value через `GsonSerializer`, формирует
   record-batch (headers попадают под общую batch compression).
4. Брокер принимает; per-key partitioning сохраняет партицию через хэш
   ключа = entityId.

**Consume (nx-gameservers):**

1. `@KafkaListener` получает `List<ConsumerRecord<Long, String>>` per poll.
2. Для каждой записи: `tenantId = TenantResolver.resolveOrThrow(topic)`,
   `serverId = NxHeaders.decodeUuid(record.headers().lastHeader(NxHeaders.
NX_SERVER_ID).value())`.
3. Если serverId == null или length invalid → WARN + skip.
4. `serverSlug = tenantCache.resolveServer(tenantId, serverId)`; если null
   после single-flight refresh → WARN + skip.
5. JSON-parse через Gson.
6. Group by `(tenantId, serverId)` → per-group `ingestor.ingestBatch(tenantId,
serverId, events)`.
7. После всех групп — `ack.acknowledge()`.

### Data model

- **`nx-gameservers.characters`** (R7) — PK `(tenant_id, server_id, id)`;
  все non-PK колонки nullable как сейчас.
- **`nx-gameservers.character_subclasses`** (R7) — PK `(tenant_id,
server_id, char_id, class)`; FK `(tenant_id, server_id, char_id) →
characters(tenant_id, server_id, id) ON DELETE CASCADE`.
- **`nx-gameservers.clans`** (R7) — PK `(tenant_id, server_id, id)`.
- **`nx-gameservers.clan_skills`** (R7) — PK + трёхколоночный FK
  по тому же шаблону.
- **`nx-gameservers.items`** (R7) — PK `(tenant_id, server_id, id)`.
- **`nx-gameservers.item_attributes`** (R7) — PK + трёхколоночный FK.

`nx-tenants.game_servers` (существующая таблица) — без изменений; читается
JOIN'ом из `InternalTenantsController` с фильтром `active = true`.

### Integration points

- **`nx-gs-adapter` ↔ `nx-tenants`** — без изменений: handshake остаётся
  `/api/tenants/servers/connect`, `ConnectContext.serverId` уже несёт UUID.
- **`nx-gs-adapter` (publisher) ↔ Kafka** (R3, R4) — все publish'ы
  идут через server-stamping wrapper; key/value serdes не меняются.
- **`nx-gameservers` ↔ Kafka** (R5, R6) — header extraction
  до JSON-parse; topic-pattern-based discovery новых тенантов через Kafka
  metadata refresh не меняется.
- **`nx-gameservers` ↔ `nx-tenants` `/internal/tenants`** (R9) —
  расширенный payload, plain HTTP + nginx ACL (никаких shared secrets, как
  и сейчас).

### Decisions

- **Decision:** identity передаётся через Kafka-header `Nx-Server-Id`, не
  через ключ и не через payload.
  **Why:** key currently = `entityId` (`Long`) обеспечивает per-entity FIFO
  через partition affinity. Класть `serverId` в ключ (composite или вместо)
  меняет partitioning без выгоды: per-server ordering уже даёт single-producer
  FIFO независимо от партиции; за то ломается консистентный hash для same
  entityId across servers. Класть в payload — это bump `SyncEvent<T>` schema
  и стоимость touching контракта на каждом entity-DTO; header — чисто
  транспортный слой, контракт `nx-gs-adapter-api` не двигается.
- **Decision:** raw 16-byte UUID, не string.
  **Why:** 16 байт vs 36 — фактор 2.25 на каждое сообщение, headers попадают
  в record-batch compression так что absolute saving после gzip/zstd не
  драматический, но raw bytes дешевле и для encode (no string formatting),
  и для compare (memcmp). Reader/writer тривиально мала.
- **Decision:** только один header (`Nx-Server-Id`), не четыре.
  **Why:** `tenantSlug` уже в имени топика — single source of truth, дублировать
  = риск рассинхрона; `tenantId` derivable из slug через `TenantCache`;
  `serverSlug` derivable из `serverId` через расширенный кэш. Headers ценны
  когда a) данные нельзя восстановить из топика/payload, b) consumer хочет
  skip-path до JSON-парса. Под оба критерия попадает только `serverId`.
- **Decision:** header-name + encoding в `nx-gs-adapter-api.NxHeaders`
  (контракт), Kafka-binding wrapper в `nx-gs-kafka`.
  **Why:** обе стороны (`nx-gs-adapter`, `nx-gameservers`) уже зависят от
  `nx-gs-adapter-api` через composite build — single source of truth для
  имени header'а и encoding'а живёт там, никакой string-duplication и
  encoding drift между сторонами. Pure-JDK helper'ы (`UUID` + `ByteBuffer`)
  не нарушают zero-runtime-deps правило `nx-gs-adapter-api`. `nx-gs-kafka`
  остаётся client-side facade для adapter'а — тонкая обёртка над
  `org.apache.kafka.common.header.Headers`, не контракт-артефакт. Будущие
  Kafka-headers (reply-flow correlation, observability) ложатся в тот же
  `NxHeaders` константами.
- **Decision:** Kafka `NxHeaders` ≠ HTTP `NxHeaders` из `nx-libs/common`.
  **Why:** разные транспорты, разные conventions (HTTP — case-insensitive
  ASCII strings, Kafka — bytes-keyed), разные packages
  (`app.l2nx.gs.adapter.api.kafka.NxHeaders` vs
  `app.l2nx.common.http.NxHeaders` или аналог) — collision'а нет. Класть
  всё в один класс — coupling без выгоды, потому что serdes тоже разные
  (HTTP value = String, Kafka value = byte[]).
- **Decision:** consumer warn+skip на missing/unknown `Nx-Server-Id`, не
  fail-fast.
  **Why:** симметрично с unknown-tenant обработкой — один bad record не
  poison'ит остальной poll. Также упрощает rollout adapter'ов в гетерогенной
  среде (хотя в этой фиче data — testing, no real backwards-compat
  concern).
- **Decision:** edit `v1.0.0_baseline.sql` in-place, не отдельная миграция.
  **Why:** данные testing-only, реальных environments с применённым старым
  changeset checksum'ом нет; dev пересоздаёт БД при apply. Чище: один
  baseline без тянущегося shim'а `v1.1.0_per_server_pk` который иначе
  повисает в репе навсегда. Платформенное правило forward-only / no
  `--rollback` сохраняется — это про разрешённые операции в новых
  миграциях, а не запрет на in-place edit нерелизнутого baseline'а.
- **Decision:** `/internal/tenants` контракт меняется in-place, не через
  versioned endpoint.
  **Why:** единственный consumer — `nx-gameservers` в том же монорепе под
  координируемым релизом. Versioned endpoint (`/internal/v2/tenants` или
  `/internal/tenants/with-servers`) добавил бы code-path, который мы немедленно
  бы выпиливали.

### Extension points

- **Дополнительные Kafka headers** — добавляются константами в
  `nx-gs-adapter-api.NxHeaders` (имя + encoding если custom); Kafka-binding
  если нужен — в helper'ах `nx-gs-kafka` producer-wrapper'а (можно
  расширить ещё одним connection-scoped header, например `Nx-Trace-Id` для
  cross-system tracing, или per-call header для reply-flow). Sync-модули
  не трогаются.
- **Per-server scoped subscriptions** — в будущем consumer может фильтровать
  события одного сервера тенанта на стороне приложения через тот же
  `Nx-Server-Id` header (например, отдельный observability-pipeline для
  одного «канарейкового» сервера); фильтр кладётся в `SyncEventConsumer`
  pre-parse без изменений wire-схемы.
- **Не-sync-каналы** — любой будущий Kafka-канал adapter'а (metrics, audit,
  command-replies) автоматически получает `Nx-Server-Id` через тот же
  wrapper, если использует базовый `NxProducer` через core.
