# nx-gs-adapter-api

Subproject of the `nx-gs-adapter` monorepo. See [`../CLAUDE.md`](../CLAUDE.md) for repo-wide
conventions (per-module slash-namespaced versioning, Maven Central publishing flow, license,
shared `:nx-gs-log`).

## Purpose

Contracts-only artifact: pure Java interfaces and POJOs that define the wire shape exchanged
by the L2NX game-server adapter and its consumers. Published as
`app.l2nx:nx-gs-adapter-api`. Consumed by:

- `:nx-gs-adapter-core` (sibling subproject) — adapter-side producer of `ConnectRequest` /
  consumer of `ConnectResponse` / `HeartbeatMessage`
- `nx-tenants` (separate repo, composite-includes this monorepo) — platform-side handler of
  `POST /api/tenants/servers/connect`

## Package layout

- `app.l2nx.gs.adapter.api.rest` — REST request/response DTOs (`ConnectRequest`,
  `ConnectResponse`, `KafkaConfig`, `Topics`)
- `app.l2nx.gs.adapter.api.kafka` — Kafka message payloads (`HeartbeatEvent`)

## Constraints

- **Java 8 source + target** — no `var`, no `Stream.toList()`, no records, no `Map.of`, no
  text blocks, no switch expressions, no pattern matching. Stream API + lambda + Optional
  are fine.
- **Zero runtime dependencies** — pure JDK only. No Spring, no Lombok, no Jackson, no Gson.
  JSON serialization is the consumer's responsibility (any binder works — Gson, Jackson,
  etc.). One exception: `org.jspecify:jspecify` is allowed for nullability annotations
  (`@Nullable` / `@NonNull`); JSpecify uses `RetentionPolicy.CLASS`, so it carries no
  runtime cost — annotations live in classfiles for static tooling but are not loaded at
  runtime. Wired as `api(libs.jspecify)` so consumers receive the annotations
  transitively and can run their own static nullness checking against the wire types.
- **POJOs, not records** — final fields + private constructor + static `builder()` +
  `equals/hashCode`. Records are Java 14+. Stick to plain classes.
- **Builder pattern** — every multi-field DTO ships with a hand-written `Builder` (no
  Lombok).
- **Public API → Javadoc mandatory** — every public type carries Javadoc; field-level JSON
  wire names documented next to the field.
- **No framework annotations** — `@Component`, `@JsonProperty`, `@NotBlank` etc. are
  forbidden. The artifact is consumed by both Spring and non-Spring sides; framework
  coupling stays out of contracts.
- **Constructor parameter names preserved** (`-parameters` javac flag) so Jackson and other
  parameter-name-binding deserializers can build the POJOs without `@JsonProperty`
  annotations.

## Versioning

Slash-namespaced tag `api/vX.Y.Z` releases this module independently. Fallback when no
`-Pnx-gs-adapter-api.version=...` is passed: the literal in this module's `build.gradle.kts`.
Release flow lives in the monorepo root — see [`../CLAUDE.md`](../CLAUDE.md).

## Testing

- **Naming**: `methodName_shouldExpectedBehavior` or `methodName_shouldExpectedBehavior_whenCondition`
- JUnit 5, no Mockito (no behavior to mock — pure DTOs)
- Test what builders / equals / hashCode actually guarantee, not framework code
