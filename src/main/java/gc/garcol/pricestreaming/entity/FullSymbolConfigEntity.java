package gc.garcol.pricestreaming.entity;

import gc.garcol.pricestreaming.dto.FullSymbolConfig;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.redis.core.RedisHash;
import org.springframework.data.redis.core.TimeToLive;

import java.math.BigDecimal;
import java.util.concurrent.TimeUnit;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@RedisHash("price-streaming:full-price")
public class FullSymbolConfigEntity {

    @Id
    private String symbolPair;

    private Long configId;

    private BigDecimal price;

    private BigDecimal deltaPercent;

    private BigDecimal lowPrice;

    private BigDecimal highPrice;

    private boolean presented;

    private boolean deleted;

    private long configEventAt;

    @TimeToLive(unit = TimeUnit.SECONDS)
    private Long ttlSeconds;

    public static FullSymbolConfigEntity from(FullSymbolConfig config, long ttlSeconds) {
        return FullSymbolConfigEntity.builder()
                .symbolPair(config.getSymbolPair())
                .configId(config.getId())
                .price(config.getPrice())
                .deltaPercent(config.getDeltaPercent())
                .lowPrice(config.getLowPrice())
                .highPrice(config.getHighPrice())
                .presented(config.isPresented())
                .deleted(config.isDeleted())
                .configEventAt(config.getConfigEventAt())
                .ttlSeconds(ttlSeconds)
                .build();
    }

    public FullSymbolConfig toFullSymbolConfig() {
        FullSymbolConfig config = new FullSymbolConfig();
        config.setSymbolPair(symbolPair);
        config.setId(configId);
        config.setPrice(price);
        config.setDeltaPercent(deltaPercent);
        config.setLowPrice(lowPrice);
        config.setHighPrice(highPrice);
        config.setPresented(presented);
        config.setDeleted(deleted);
        config.setConfigEventAt(configEventAt);
        return config;
    }
}
