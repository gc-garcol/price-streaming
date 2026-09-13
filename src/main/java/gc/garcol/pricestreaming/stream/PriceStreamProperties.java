package gc.garcol.pricestreaming.stream;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Topics, store names and topic provisioning for the kafka streams topology.
 */
@Data
@ConfigurationProperties(prefix = "price-stream")
public class PriceStreamProperties {

    private boolean enabled = true;

    private int partitions = 3;

    private short replicas = 1;

    private Topic topic = new Topic();

    private Store store = new Store();

    private Consumer consumer = new Consumer();

    private Cache cache = new Cache();

    @Data
    public static class Topic {
        /** debezium outbox topic, keyed by aggregate id, value is the outbox envelope */
        private String priceConfigEvents = "PRICE_CONFIG.events";
        /** full database load, keyed by symbol pair, published on startup */
        private String priceConfigSnapshot = "stream.PRICE_CONFIG.snapshot";
        /** market prices, keyed by symbol pair, published by the feed scheduler */
        private String marketPrice = "stream.MARKET_PRICE.events";
        /** join result, keyed by symbol pair */
        private String fullConfig = "stream.full-config.events";
    }

    @Data
    public static class Cache {
        /** expiry of the redis projection, renewed on the schedule of the ring buffer cache */
        private Duration ttl = Duration.ofMinutes(30);
    }

    @Data
    public static class Consumer {
        /** group of the listener that projects the join result onto redis */
        private String groupId = "price-streaming-full-config";
        /** how many records one poll hands to the listener, so one redis round trip covers them */
        private int maxBatchSize = 500;
        private int concurrency = 1;
    }

    @Data
    public static class Store {
        private String priceConfig = "price-config-store";
        private String marketPrice = "market-price-store";
        private String fullConfig = "full-config-store";
    }
}
