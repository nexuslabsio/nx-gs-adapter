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
