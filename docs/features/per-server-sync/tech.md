# Per-Server Sync — tech

> Covers: spec.md

## Overview

`per-server-sync` — транспортный слой на Kafka-headers, который добавляет
identity-маркер `Nx-Server-Id` (raw 16-byte UUID) на каждое sync-сообщение
adapter'а и расширяет consumer (nx-gameservers) до per-server-scoped ingestion.
Фича пересекает три репозитория: `nx-gs-adapter` (kafka-фасад + producer
wiring + sync/heartbeat модули), `nx-gameservers` (header extraction + schema
с composite PK + tenant-cache с server-каталогом), `nx-tenants` (internal API
отдаёт активные серверы тенанта). Wire-схема `SyncEvent<T>` в
`nx-gs-adapter-api` не меняется.

## Structure

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

## Key components

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

## Data flows

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

## Data model

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

## Integration points

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

## Decisions

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

## Extension points

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
