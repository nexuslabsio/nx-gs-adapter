# Plan: per-server-sync — full rollout (R1–R9)

> **Covers:** R1, R2, R3, R4, R5, R6, R7, R8, R9 — all spec requirements.
>
> **Cross-repo scope:**
> - `nx-gs-adapter` — `nx-gs-adapter-api` (NxHeaders contract), `nx-gs-kafka`
    > (Kafka binding), `nx-gs-adapter-core` (post-connect wiring),
    > `nx-gs-db-sync-core` + `nx-gs-runtime-sync-core` (publishers stay unchanged
    > — wiring layer hands them a stamping producer), heartbeat service
> - `nx-gameservers` — Liquibase forward-only changeset, repository SQL,
    > ingestors, `SyncEventConsumer` header extraction, `TenantCache` extended
    > snapshot, `NxTenantsClient` DTOs
> - `nx-tenants` — `TenantSummary` shape, repository query, controller,
    > integration tests
>
> **Resolved decisions (locked in spec/tech, no rediscussion):**
> - Header `Nx-Server-Id`, raw 16-byte UUID, NOT in key, NOT in payload
> - Single header only; tenantSlug stays in topic name, others derivable
> - Constant + encoding live in `nx-gs-adapter-api.NxHeaders`; Kafka-binding
    > in `nx-gs-kafka`
> - Forward-only Liquibase, drop & recreate; data is testing-only
> - `/internal/tenants` payload extended in-place (no versioned endpoint)
> - Missing/unknown header on consumer → warn + skip (symmetric with
    > unknown-tenant handling)

## Approach

Six layers delivered in dependency order; each group is buildable independently
of the next, so we can stop / commit / hand off mid-stream without leaving
either repo half-broken.

1. **Contract (`nx-gs-adapter-api`)** — pure-JDK `NxHeaders` class with
   `NX_SERVER_ID` constant + `encodeUuid` / `decodeUuid` helpers (16 raw bytes,
   `mostSigBits` BE then `leastSigBits` BE). Single source of truth for both
   sides. Zero runtime deps preserved.
2. **Kafka binding (`nx-gs-kafka`)** — `DefaultNxProducer` accepts an optional
   `Map<String, byte[]> staticHeaders` and stamps them on every outbound
   record (all `send(...)` overloads + `sendRecord`). New `NxProducer.create`
   factory overload exposes this.
3. **Platform server catalog (`nx-tenants`)** — `/internal/tenants` payload
   extended in-place to include per-tenant active servers. Single query JOIN
   `tenants × game_servers WHERE active=true`, manual aggregation in row
   mapper to avoid N+1.
4. **Consumer schema (`nx-gameservers`)** — drop & recreate all six tables
   with composite PK `(tenant_id, server_id, id)` on parents and three-column
   FK + cascade on children. Repository SQL switches accordingly. Ingestor
   signatures accept `serverId` per call.
5. **Consumer cache + listener (`nx-gameservers`)** — `TenantCache` holds
   `Map<slug, TenantInfo{tenantId, Map<serverId, serverSlug>}>` snapshot;
   `SyncEventConsumer` extracts `Nx-Server-Id` pre-JSON-parse, resolves
   tenant+server, groups by `(tenantId, serverId)`, dispatches to ingestors;
   warn+skip on missing header / unknown server.
6. **Adapter wiring (`nx-gs-adapter-core` + sync modules + heartbeat)** —
   after `/connect` succeeds, core builds a stamping `NxProducer` with
   `{NX_SERVER_ID → encodeUuid(serverId)}` and hands it to sync modules and
   heartbeat. db-sync, runtime-sync, heartbeat code is untouched — they
   already publish through `NxProducer`.

## Milestones

### Group A — `nx-gs-adapter-api`: NxHeaders contract (R1)

1. [x] **Create `NxHeaders` class.** Add
   `nx-gs-adapter-api/src/main/java/app/l2nx/gs/adapter/api/kafka/NxHeaders.java`:
   `public static final String NX_SERVER_ID = "Nx-Server-Id"`,
   `public static byte[] encodeUuid(UUID)`,
   `public static UUID decodeUuid(byte[])`. Encoding via
   `ByteBuffer.allocate(16).putLong(uuid.getMostSignificantBits()).putLong(
   uuid.getLeastSignificantBits()).array()`. Decode rejects length ≠ 16
   with `IllegalArgumentException` (clear message). Java 8, no deps beyond
   JDK + jspecify (already on classpath). Javadoc on class + each public
   method, including the WHY for raw-16-byte choice and the encoding
   ordering.

2. [x] **Unit tests for `NxHeaders`.** Add
   `nx-gs-adapter-api/src/test/java/app/l2nx/gs/adapter/api/kafka/NxHeadersTest.java`:
    - roundtrip: `decodeUuid(encodeUuid(uuid))` equals `uuid` for several
      UUIDs (zero, max, random)
    - known-vector: specific UUID encodes to specific 16-byte hex (lock
      the byte ordering)
    - `decodeUuid(null)` and `decodeUuid(new byte[15])` /
      `decodeUuid(new byte[17])` throw `IllegalArgumentException`
    - constant value is exactly the literal `"Nx-Server-Id"` (catches typo
      regressions)

**Checkpoint R1:** contract compiles and tests pass.
`./gradlew :nx-gs-adapter-api:test` green. Commit candidate:
`feat(nx-gs-adapter-api): add NxHeaders kafka header contract`.

### Group B — `nx-gs-kafka`: header-stamping producer (R2)

3. [x] **Extend `DefaultNxProducer` constructor with `staticHeaders`.**
   Add a new constructor overload
   `DefaultNxProducer(Map<String, Object> config, Gson gson, Map<String,
   byte[]> staticHeaders)` storing an immutable copy in a final field
   (default empty for the existing constructor). On every `send(...)` /
   `sendRecord(...)` overload, before delegating to the underlying
   `KafkaProducer`, iterate `staticHeaders` and add each to
   `record.headers()` via `record.headers().add(name, value)`. Java 8,
   no streams in hot path. Cover all five send paths
   (no-key, string-key, no-key+callback, string-key+callback,
   bytes-key+callback) plus `sendRecord`.

4. [x] **Add `NxProducer.create` factory overload.** New static method on
   the `NxProducer` interface:
   `static NxProducer create(Map<String, Object> config, Gson gson,
   Map<String, byte[]> staticHeaders)`. Existing two-arg overload stays
   untouched — delegates to the three-arg form with empty map. Javadoc
   spells out that the headers are stamped per-record.

5. [x] **Integration test for header stamping.** Add to
   `nx-gs-kafka/src/test/java/app/l2nx/gs/kafka/integration/`:
   `HeaderStampingIntegrationTest` (`@Tag("integration")`). Spin up
   Testcontainers Kafka, create producer with `staticHeaders =
   {"Nx-Server-Id": NxHeaders.encodeUuid(uuid)}`, send via each `send`
   overload, consume back, assert `record.headers().lastHeader(...)`
   contains the expected 16 bytes and decodes back to the original UUID.

**Checkpoint R2:** kafka facade stamps headers on every send path.
`./gradlew :nx-gs-kafka:test` green; integration test passes locally with
Docker. Commit candidate:
`feat(nx-gs-kafka): support per-producer static headers for server-id stamping`.

### Group C — `nx-tenants`: extended `/internal/tenants` payload (R9)

6. [x] **Extend `TenantSummary` record.** Update
   `nx-tenants/.../domain/tenant/TenantSummary.java`:
   `record TenantSummary(UUID id, String slug, List<GameServerSummary>
   servers)`. Keep `@Builder(toBuilder = true)`. `servers` is never null —
   empty list when tenant has no active servers. Update `TenantSummary`
   instantiations across the codebase to pass an explicit list.

7. [x] **Update repository query.** Modify
   `nx-tenants/.../infra/postgres/TenantRepositoryAdapter` (or wherever
   `findAllSummaries(`-equivalent) lives) to return tenants with their
   active servers loaded. Use `JdbcClient` with a single SQL:
   `SELECT t.id, t.slug, gs.id, gs.server_slug FROM tenants t LEFT JOIN
   game_servers gs ON gs.tenant_id = t.id AND gs.active = true WHERE
   t.system = false ORDER BY t.id`. Aggregate rows by `t.id` in the row
   mapper into `TenantSummary{id, slug, servers}`. Single round-trip; no
   N+1.

8. [x] **Update `TenantUseCases` / `TenantService`.** The `listAll`
   use case (or whatever feeds `InternalTenantsController`) returns the
   new shape. No new method needed — same contract, richer payload.

9. [x] **Update `InternalTenantsApi` Javadoc.** Reflect new payload shape
   in `@Operation` description on
   `nx-tenants/.../api/internal/tenant/InternalTenantsApi.java`. Single
   source of truth for the OpenAPI doc.

10. [x] **Update existing tests + integration test.** Adjust unit tests
    on `TenantService` / repository to expect `servers` field. Add an
    integration test in `nx-tenants/.../integration/` that registers a
    tenant with two active and one deactivated server, hits
    `GET /internal/tenants`, asserts the response includes the two active
    ones in `servers` and excludes the deactivated one.

**Checkpoint R9:** nx-tenants returns extended payload.
`./gradlew test` green. Commit candidate:
`feat(internal-tenants): include active servers in tenant summary payload`.

### Group D — `nx-gameservers`: schema (R7)

11. [x] **Liquibase baseline (in-place edit).** Update
    `nx-gameservers/src/main/resources/db/liquibase/v1.0.0_baseline.sql`
    in place — баз с реальными данными нет, отдельная миграция не нужна;
    меняем changeset bodies, dev пересоздаёт БД при apply.
    Tables to recreate with composite PK / FK:
    - `characters` PK `(tenant_id, server_id, id)`; all other columns as
      they are today.
    - `character_subclasses` PK `(tenant_id, server_id, char_id, class)`,
      FK `(tenant_id, server_id, char_id) REFERENCES characters
      (tenant_id, server_id, id) ON DELETE CASCADE`.
    - `clans` PK `(tenant_id, server_id, id)`.
    - `clan_skills` PK `(tenant_id, server_id, clan_id, skill_id)` + FK by
      same pattern.
    - `items` PK `(tenant_id, server_id, id)`.
    - `item_attributes` PK `(tenant_id, server_id, item_id, type)` + FK.

    Forward-only — no `--rollback`. Changeset id format
    `gameservers:1.x-per-server-pk` per platform convention; author
    `n1rmata`.

12. [x] **Update parent repository SQL — characters.** Modify
    `CharacterRepository`:
    - UPSERT: `INSERT ... ON CONFLICT (tenant_id, server_id, id) DO
      UPDATE SET ...`
    - DELETE batch: `DELETE FROM characters WHERE tenant_id = :tid AND
      server_id = :sid AND id IN (:ids)` (per-tenant-per-server batch)
    - Child collection delete (subclasses): `DELETE FROM
      character_subclasses WHERE tenant_id = :tid AND server_id = :sid
      AND char_id IN (:parentIds)`
    - Child INSERT: include `server_id` in column list and parameter
      source.

13. [x] **Update parent repository SQL — clans + items.** Same shape as
    M12 but on `ClanRepository` (with `clan_skills` child) and
    `ItemRepository` (with `item_attributes` child).

14. [x] **Update ingestor signatures.** Modify
    `CharacterDbIngestor.ingestBatch(UUID tenantId, UUID serverId,
    List<SyncEvent<CharacterDto>> events)`,
    `CharacterRuntimeIngestor.ingestBatch(...)`,
    `ClanIngestor.ingestBatch(...)`,
    `ItemIngestor.ingestBatch(...)`. Pass `serverId` through to
    repository calls; include in dedup key (`(server_id, pk)` last-wins
    inside the batch). Update existing unit tests to construct events
    with serverId.

15. [x] **Update existing integration tests for new schema.** Adjust
    `nx-gameservers/.../integration/` tests to write events with a
    chosen serverId, assert rows land at composite PK. No new behavior
    test yet — just keep existing coverage green.

**Checkpoint R7:** nx-gameservers schema migrated, repos + ingestors
compile + green. `./gradlew test` (with Docker for Testcontainers).
Commit candidate:
`feat(gameservers): composite (tenant_id, server_id, id) PK for sync entities`.

### Group E — `nx-gameservers`: TenantCache + SyncEventConsumer (R5, R6, R8)

16. [x] **Add `TenantInfo` domain model.** New record under
    `nx-gameservers/.../infra/tenants/TenantInfo.java`:
    `record TenantInfo(UUID tenantId, Map<UUID, String> servers) {}`
    where the map key is serverId and value is serverSlug. Unmodifiable
    view returned from cache. `@Builder(toBuilder = true)`.

17. [x] **Update `NxTenantsClient` DTOs.** Add `GameServerSummaryDto`
    record (id + serverSlug); update `TenantSummaryDto` to include
    `List<GameServerSummaryDto> servers`. Adjust deserialization (Jackson
    binds positionally).

18. [x] **Refactor `TenantCache` snapshot type.** Change internal state
    from `AtomicReference<Map<String, UUID>>` to
    `AtomicReference<Map<String, TenantInfo>>`. `resolve(slug)` now
    returns `@Nullable TenantInfo`. Add `resolveServer(UUID tenantId,
    UUID serverId): @Nullable String slug` helper that walks all
    snapshot entries (small N, micro-optimization-free). Single-flight
    refresh logic stays as-is.

19. [x] **Update `TenantResolver` to surface `TenantInfo`.** Method
    rename / new signature: `resolveOrThrow(String topic): TenantInfo` (or
    keep old method and add `resolveByIdAndServer`); pick whichever fits
    the consumer flow cleanly.

20. [x] **Update `SyncEventConsumer.dispatchBatch` per R5+R6.** Within the
    per-record loop, BEFORE `gson.fromJson(...)`:
    - extract header bytes:
      `Header h = record.headers().lastHeader(NxHeaders.NX_SERVER_ID)`
    - if `h == null` → warn + skip (record offset stays committed via
      batch ack, since we are inside the per-record skip path)
    - decode: `UUID serverId; try { serverId =
      NxHeaders.decodeUuid(h.value()); } catch (IllegalArgumentException
      e) { warn + skip; }`
    - resolve tenant slug → `TenantInfo` from cache (existing flow); if
      tenant unknown → warn + skip (existing behavior)
    - check `tenantInfo.servers().containsKey(serverId)`; if missing,
      trigger single-flight refresh once and re-check. Still missing →
      warn + skip with serverId in the message.
    - parse JSON
    - group key becomes `Map<TenantServerKey, List<SyncEvent<T>>>` where
      `TenantServerKey` is a small record `(tenantId, serverId)`.
    - dispatch: `handler.accept(key.tenantId(), key.serverId(), events)`.

21. [x] **Update `dispatchBatch` handler signature.** The lambda type
    changes from `BiConsumer<UUID, List<SyncEvent<T>>>` to a new
    `TriConsumer`-style — define a tiny SAM
    `interface IngestHandler<T> { void accept(UUID tenantId, UUID
    serverId, List<SyncEvent<T>> events); }` in the consumer file (or
    reuse a JDK functional interface if one fits). Plug ingestor method
    references through it.

22. [x] **Integration test: per-server routing happy path.** New test in
    `nx-gameservers/.../integration/`: register a tenant with two active
    servers, send sync events with each server's `Nx-Server-Id` header
    for the same character_id=1, assert two distinct rows land at
    `(tenant_id, server_a_id, 1)` and `(tenant_id, server_b_id, 1)`.

23. [x] **Integration test: warn + skip paths.** Send sync events with
    a) no header, b) invalid-length header, c) unknown serverId.
    Assert no rows are written for the bad records, the rest of the
    batch lands cleanly, offset is committed.

**Checkpoint R5+R6+R8:** consumer fully scopes by server.
`./gradlew test` green. Commit candidate:
`feat(gameservers): per-server scoping in sync consumer + tenant cache`.

### Group F — `nx-gs-adapter`: producer wiring (R3, R4)

24. [x] **Build stamping producer in `nx-gs-adapter-core` post-connect.**
    In the bootstrap flow (after `/connect` succeeds and before module
    `onConnect`s fire), construct the `NxProducer` via
    `NxProducer.create(kafkaConfig, gson,
    Collections.singletonMap(NxHeaders.NX_SERVER_ID,
    NxHeaders.encodeUuid(connectResponse.getServerId())))`. The
    pre-encoded byte[] is computed once and shared as an immutable map.
    Pass this producer to module `onConnect(ctx)` flow as before.

25. [x] **Verify db-sync + runtime-sync inherit stamping transparently.**
    `db-sync` `SyncEventPublisher` and `runtime-sync` equivalent call
    `producer.send(topic, key, message, callback)` — no code change
    needed; the wrapped producer stamps the header. Run existing module
    integration tests to confirm they still pass; extend at least one
    test (e.g. `CdcEngineE2ETest`) to consume events back and assert
    the header is present and decodes to the expected serverId.

26. [x] **Wire heartbeat publisher to use the same producer.** Confirm
    `HeartbeatService` publishes via the post-connect `NxProducer`
    instance (it already does per `adapter-bootstrap`); add a header
    assertion to its integration test.

27. [x] **End-to-end smoke (manual or test).** With Testcontainers
    Kafka + WireMock platform returning a `ConnectResponse` with a
    known `serverId`, drive the adapter through bootstrap → start sync →
    publish → consume back. Assert every sync record AND the heartbeat
    carry `Nx-Server-Id` matching the connect-response serverId.

**Checkpoint R3+R4:** adapter end-to-end.
`./gradlew :nx-gs-adapter-core:test :nx-gs-db-sync-core:test
:nx-gs-runtime-sync-core:test` green. Commit candidate:
`feat(adapter-core): stamp Nx-Server-Id on every produced record`.

### Final

28. [x] **Update R statuses across spec/tech.** Run `/specl-sync` (or
    flip `[todo] → [done]` manually for R1–R9) on
    `docs/features/per-server-sync/spec.md`. Remove any remaining
    `[planned]` markers from `tech.md` for components now landed.

29. [x] **Cross-repo CLAUDE.md updates.** Sync the three CLAUDE.md
    files with the new reality:
    - `nx-gs-adapter/CLAUDE.md` — mention `NxHeaders` in `nx-gs-adapter-api`
      module description; mention header stamping in `nx-gs-kafka`
    - `nx-gameservers/CLAUDE.md` — update DB schema sample (composite PK),
      `TenantCache` description, consumer flow
    - `nx-tenants/CLAUDE.md` — `/internal/tenants` payload shape

## Notes

- `.plan/` is gitignored per platform convention; this plan file lives
  locally only.
- Composite-build resolution makes `nx-gs-adapter-api` changes available
  to `nx-gameservers` and `nx-tenants` immediately — no Maven publish
  needed during development.
- No `nx-gs-adapter-api` version bump is needed for local development;
  release-time the CI tags `api/v0.X.Y`.
- Integration tests across all three repos require Docker (Testcontainers
    + Kafka + Postgres).
- The plan does NOT touch `nx-gs-adapter-api` `SyncEvent<T>` schema —
  header is purely transport-level by design.
