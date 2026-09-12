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
