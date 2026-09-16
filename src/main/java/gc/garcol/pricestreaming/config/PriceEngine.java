package gc.garcol.pricestreaming.config;

/**
 * Which of the two pipelines the application runs. They share PostgreSQL, Debezium, Redis and
 * Centrifugo but nothing else, so either one alone is a complete price pipeline.
 */
public enum PriceEngine {

    /** ring buffer only: the disruptor, its four handlers and the redis projection they write */
    LMAX,

    /** kafka streams only: the topology, its topics and the listener projecting the join result */
    KAFKA_STREAM,

    /** both pipelines side by side, each with its own redis keyspace and centrifugo channel */
    BOTH;

    public boolean includes(PriceEngine engine) {
        return this == BOTH || this == engine;
    }
}
