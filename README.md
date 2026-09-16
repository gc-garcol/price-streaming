# price-streaming

![gif](./price-streaming.gif)

## Stacks

![Java](https://img.shields.io/badge/Java-25-007396?logo=openjdk&logoColor=white)
![Spring Boot](https://img.shields.io/badge/Spring%20Boot-4.1.1-6DB33F?logo=springboot&logoColor=white)
![LMAX Disruptor](https://img.shields.io/badge/LMAX%20Disruptor-4.0.0-FF6F00)
![Kafka](https://img.shields.io/badge/Kafka%20%2F%20Redpanda-v24.1.7-231F20?logo=apachekafka&logoColor=white)
![Debezium](https://img.shields.io/badge/Debezium-2.7.3-D6412B)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-17-4169E1?logo=postgresql&logoColor=white)
![Liquibase](https://img.shields.io/badge/Liquibase-5.0.3-2962FF?logo=liquibase&logoColor=white)
![Redis](https://img.shields.io/badge/Redis%20Stack-latest-DC382D?logo=redis&logoColor=white)
![Centrifugo](https://img.shields.io/badge/Centrifugo-v4.1.2-2C3E50)
![gRPC](https://img.shields.io/badge/gRPC-1.83.1-244C5A?logo=grpc&logoColor=white)
![Protobuf](https://img.shields.io/badge/Protobuf-4.35.1-4285F4?logo=googledocs&logoColor=white)
![Swagger](https://img.shields.io/badge/OpenAPI-3.1.1-85EA2D?logo=swagger&logoColor=black)
![Docker](https://img.shields.io/badge/Docker%20Compose-2496ED?logo=docker&logoColor=white)

## High level design

![high level design](./docs/high-level-design.png)

Writes land in PostgreSQL and the outbox table in one transaction. Debezium turns outbox rows into
Kafka events, the consumer feeds them into the ring buffer, and a single threaded domain handler owns
all mutable state.

Prices reach the ring from two independent sources:

| source | mode | class |
| --- | --- | --- |
| polling | fixed interval snapshot of every symbol | `SymbolPriceFeedScheduler` |
| streaming | continuous realtime ticks over a websocket | `SymbolPriceStreamClient` |

### Ring buffer

![ring buffer](./docs/ring-buffer.png)

| Aspect | Value |
| --- | --- |
| Size | `1 << disruptor.ringBufferPowSize` = **4096** slots, power of two so a sequence maps to a slot by bit mask instead of modulo |
| Producer type | `ProducerType.MULTI` — kafka consumer, feed scheduler, websocket stream and config refresh all claim sequences concurrently |
| Wait strategy | `disruptor.waitStrategy` — `BLOCKING` by default; `YIELDING` / `BUSY_SPIN` trade CPU for latency, `TIMEOUT_BLOCKING` uses `disruptor.waitTimeout` |
| Allocation | every slot holds a `DisruptorEvent` built once at startup; producers copy values **into** the slot through `EventTranslatorOneArg`, so steady state allocates nothing |
| Ordering | `handleEventsWith(H1).then(H2).then(H3).then(H4)` is a dependency chain, so each event clears one stage before the next sees it |
| Batching | H2 and H3 accumulate into pre-allocated buffers and flush on `endOfBatch` or at `disruptor-handler.maxHandler{2,3}BatchSize` |
| Cleanup | H4 is the only stage calling `event.reset()`, so earlier stages still see a populated event |
| Back pressure | producers block once the ring is full; the gating sequence is the slowest consumer, so a stalled Redis or Centrifugo throttles ingestion instead of dropping events |
| Failure isolation | `DisruptorExceptionHandler` logs and continues, otherwise one bad event kills the consumer thread and stalls the ring |

State lives only in H1, which is single threaded by construction, so `PriceState` needs no locks.

Diagram sources: [`docs/high-level-design.puml`](./docs/high-level-design.puml) (PlantUML),
[`docs/ring-buffer.svg`](./docs/ring-buffer.svg) (SVG)

## Kafka Streams architecture

The same writes feed a second, independent pipeline. Instead of one ring buffer holding all mutable
state in a single thread, state lives in partitioned kafka state stores and the joining is done by
the brokers plus a `KTable` join. The two pipelines share PostgreSQL, Debezium, Redis and
Centrifugo, but nothing else; `price-stream.enabled=false` removes the whole topology, its topics
and its consumer, and the ring buffer keeps working on its own.

```
          PriceConfig snapshot ──┐
                                 ├── latest wins ──▶ KTable<Symbol, PriceConfig>
          PriceConfig CDC ───────┘                            │
                                                              │ INNER JOIN
                                                              ▼
          MarketPrice ──────────▶ KTable<Symbol, Price> ──▶ KTable<Symbol, FullConfig>
                                                              │
                                                   ┌──────────┴──────────┐
                                                   ▼                     ▼
                                           full-config.events      full-config-store
                                                                 (redis projection pages it)
```

Topology is built in [`PriceTopology`](./src/main/java/gc/garcol/pricestreaming/stream/PriceTopology.java),
wiring in [`PriceStreamConfig`](./src/main/java/gc/garcol/pricestreaming/stream/PriceStreamConfig.java).

### Topics

Every topic is a table changelog keyed by symbol pair, so all of them are **compacted** and share
the same partition count — without co-partitioning the join sides would not line up.

| topic | key | written by | role |
| --- | --- | --- | --- |
| `PRICE_CONFIG.events` | aggregate id | Debezium outbox connector | config changes, re-keyed to symbol pair inside the topology |
| `stream.PRICE_CONFIG.snapshot` | symbol pair | `PriceConfigSnapshotPublisher` | full database load, published at startup |
| `stream.MARKET_PRICE.events` | symbol pair | `MarketPricePublisher` | price ticks from the feed scheduler |
| `stream.full-config.events` | symbol pair | the topology | join result, plus a tombstone when a pair leaves the join |

### Join semantics

| Aspect | Value |
| --- | --- |
| Config table | outbox CDC `merge` snapshot, then `groupByKey().reduce(PriceConfigEvent::latest)` — the revision with the highest `configEventAt` wins, so startup order between the two producers does not matter |
| Deletions | a `PriceConfigDeleted` event reduces to `deleted=true`, and `filter(!deleted)` turns it into a tombstone, which drops the pair from the join |
| CDC parsing | `PriceConfigCdcProcessor` is a `FixedKeyProcessor`, not a `mapValues`, because the event type only exists in the `eventType` header; unparsable rows are dropped instead of forwarded |
| Price table | `builder.table(...)` straight off `MARKET_PRICE.events` — already keyed by symbol pair, so no repartition |
| Join type | **inner** — a symbol pair is published only once it has both an active config and a price, and disappears again when either side goes away |
| Derived fields | `FullConfig.join` computes `lowPrice` / `highPrice` from `price ± price * deltaPercent / 100`, scaled 8 / 6 with `HALF_UP` |
| Latency | `state-store-cache-max-size: 0B` and `commit.interval.ms: 200` — every update is forwarded instead of being buffered until the next commit |
| Serdes | `JacksonJsonSerde` pinned per target class with `noTypeInfo().ignoreTypeHeaders()`, so no `gc.garcol` class names travel in the records |
| Bootstrap | `PriceStreamBootstrap` (a `SmartLifecycle` in an earlier phase than the streams client) seeds both source topics before the tables are built; a broker failure there is logged, not fatal, since the topology catches up from the compacted topics |
| Failure isolation | a failed stream thread is replaced (`REPLACE_THREAD`) and deserialization errors use `LogAndContinueExceptionHandler`, so one bad record cannot leave the store unqueryable |

### Serving the join result

An instance's state store only holds the partitions it owns, so paging is served from Redis instead
of an interactive query:

| step | class | behaviour |
| --- | --- | --- |
| projection | `FullConfigEventConsumer` | batch listener (`max-batch-size` 500) that collapses a poll to the last record per key, then one `saveAll` / `deleteAllById` per batch |
| push | `CentrifugoPublisher` | the same collapsed batch goes to the `full-config-stream` channel; a tombstone becomes a `FullConfigDeletion` so subscribers drop the row |
| storage | `StreamFullConfigEntity` | its own Redis keyspace (`price-streaming:stream:full-config`), separate from the ring buffer projection |
| expiry | `StreamFullConfigCache` | every write carries the ttl, and the existing renew schedule pushes it back out so a quiet symbol does not expire while the topology still holds it |
| query | `GET /api/stream/full-configs` | `FullConfigStreamController` paging over the projection |

Redis failures in the listener are deliberately **not** swallowed — the batch is redelivered,
otherwise a dropped deletion would leave a symbol pair in the projection forever. A failed
Centrifugo push is only logged, so it never holds up the offset commit.

[`index.kafkastream.html`](./index.kafkastream.html) is the browser client for this pipeline, the
counterpart of the ring buffer page.

### Where the two pipelines differ

| | ring buffer | kafka streams |
| --- | --- | --- |
| state | in process, single thread, `PriceState` | partitioned RocksDB state stores + changelogs |
| joining | domain handler code | `KTable` inner join |
| scaling | one JVM owns everything | partitions spread across stream threads and instances |
| back pressure | producers block on a full ring | consumer lag |
| recovery | rebuild from Redis / database on boot | replay compacted topics and changelogs |
| price source | feed scheduler **and** websocket stream client | feed scheduler only (`SymbolPriceFeedScheduler` publishes to both pipelines) |
| read api | `/api/full-symbol-configs` | `/api/stream/full-configs` |
| push channel | `price-stream` | `full-config-stream` |

## Setup
```shell
docker compose up -d
```

```shell
curl -i -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  --data @etl/outbox-connector.json
```

```shell
export POSTGRES_USER=username
export POSTGRES_PASSWORD=password

./mvnw liquibase:status

./mvnw liquibase:update
```

Centrifuge .proto
```shell
https://github.com/centrifugal/centrifugo/blob/master/internal/apiproto/api.proto
```
