package gc.garcol.pricestreaming.domainlogic;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.math.BigDecimal;
import java.time.Duration;

@Data
@ConfigurationProperties(prefix = "price-state")
public class PriceStateProperties {

    private Cache cache = new Cache();

    private SymbolFeed symbolFeed = new SymbolFeed();

    private PriceConfig priceConfig = new PriceConfig();

    @Data
    public static class Cache {
        private Duration ttl = Duration.ofMinutes(30);
        private Duration renewInterval = Duration.ofMinutes(5);
    }

    @Data
    public static class PriceConfig {
        private Duration refreshInterval = Duration.ofMinutes(5);
    }

    @Data
    public static class SymbolFeed {
        private Duration minLatency = Duration.ofSeconds(1);
        private Duration maxLatency = Duration.ofSeconds(5);
        private BigDecimal priceJitterPercent = new BigDecimal("5");
        private Duration fetchInterval = Duration.ofMinutes(1);
        private Duration burstInterval = Duration.ofSeconds(1);
        private int maxBurst = 5;
        private double burstPublishRatio = 1.0 / 3;
    }
}
