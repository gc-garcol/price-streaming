package gc.garcol.pricestreaming.entity;

import gc.garcol.pricestreaming.stream.FullConfig;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.redis.core.RedisHash;
import org.springframework.data.redis.core.TimeToLive;

import java.math.BigDecimal;
import java.util.concurrent.TimeUnit;

/**
 * Redis projection of the kafka streams join result. It lives in its own keyspace so it cannot
 * collide with {@link FullSymbolConfigEntity}, which the ring buffer pipeline owns.
 *
 * <p>A tombstone on {@code full-config.events} is what normally removes a symbol pair; the time to
 * live is the backstop for a projection nobody is maintaining any more, and
 * {@code StreamFullConfigCache} pushes it back out while this application is alive.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@RedisHash("price-streaming:stream:full-config")
public class StreamFullConfigEntity {

    @Id
    private String symbolPair;

    private Long configId;

    private BigDecimal price;

    private BigDecimal deltaPercent;

    private BigDecimal lowPrice;

    private BigDecimal highPrice;

    private long priceAt;

    private long configEventAt;

    @TimeToLive(unit = TimeUnit.SECONDS)
    private Long ttlSeconds;

    public static StreamFullConfigEntity from(FullConfig config, long ttlSeconds) {
        return StreamFullConfigEntity.builder()
                .symbolPair(config.symbolPair())
                .configId(config.configId())
                .price(config.price())
                .deltaPercent(config.deltaPercent())
                .lowPrice(config.lowPrice())
                .highPrice(config.highPrice())
                .priceAt(config.priceAt())
                .configEventAt(config.configEventAt())
                .ttlSeconds(ttlSeconds)
                .build();
    }

    public FullConfig toFullConfig() {
        return new FullConfig(
                symbolPair,
                configId,
                price,
                deltaPercent,
                lowPrice,
                highPrice,
                priceAt,
                configEventAt);
    }
}
