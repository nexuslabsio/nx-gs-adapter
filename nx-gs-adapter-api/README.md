# nx-gs-adapter-api

Wire contracts (DTOs + SPI) for the L2NX game-server adapter — the canonical types exchanged on
the network between the adapter and any consumer that speaks its protocol.

## Project

- **Organization:** nexuslabsio
- **Group:** `app.l2nx`
- **Artifact:** `nx-gs-adapter-api`
- **Package:** `app.l2nx.gs.adapter.api`
- **Java:** 8 (source + target)
- **Build:** Gradle (Kotlin DSL)
- **Dependencies:** none (pure Java interfaces + POJOs)

## Contents

- `app.l2nx.gs.adapter.api.rest.*` — REST request / response DTOs (`ConnectRequest`,
  `ConnectResponse`, `KafkaConfig`, `SyncTopics`, `MessagingTopics` — the wire shape of the
  adapter handshake)
- `app.l2nx.gs.adapter.api.kafka.*` — Kafka message payloads + header contract (`NxHeaders`),
  per-family event DTOs under `kafka.events.<family>`, inbound command marker
  `kafka.commands.NxCommand` + reply envelope `CommandResult` + `ErrorCode`, operational
  telemetry under `kafka.ops`
- `app.l2nx.gs.adapter.api.spi.*` — `AdapterModule` SPI (Tier-1) + `DbSchemaProvider` /
  `RuntimeStateProvider` (Tier-2) + `JdbcConnectionSource` (Tier-3), plus context bundles
  (`ConnectContext` / `CommandContext`, both exposing `io()` for blocking-IO hops),
  capabilities (`NxEvents`, `NxCommands`), and helpers (`CommandHandler`, `HostExecutor`)

See [`CLAUDE.md`](./CLAUDE.md) for the per-package detail.

