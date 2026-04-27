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
  `ConnectResponse`, `KafkaConfig`, `Topics` — the wire shape of the adapter handshake)
- `app.l2nx.gs.adapter.api.kafka.*` (planned, future minor) — Kafka message payloads. Will land
  here once a paired consumer needs a shared compile-time type
- `app.l2nx.gs.adapter.api.spi.*` (planned) — `AdapterModule` SPI loaded via `ServiceLoader`

