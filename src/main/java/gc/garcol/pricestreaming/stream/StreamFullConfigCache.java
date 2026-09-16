package gc.garcol.pricestreaming.stream;

import gc.garcol.pricestreaming.config.ConditionalOnPriceEngine;
import gc.garcol.pricestreaming.config.PriceEngine;
import gc.garcol.pricestreaming.entity.StreamFullConfigEntity;
import gc.garcol.pricestreaming.repository.redis.StreamFullConfigRedisRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * Keeps the redis projection of the join result alive. A symbol pair that stops ticking would
 * otherwise expire even though the topology still holds it, so the ttl is pushed back out on a
 * schedule, the same way {@code PriceStateCache} does it for the ring buffer cache.
 */
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnPriceEngine(PriceEngine.KAFKA_STREAM)
public class StreamFullConfigCache {

    private final StreamFullConfigRedisRepository repository;
    private final PriceStreamProperties properties;

    /**
     * A failed renew is logged and dropped: the next tick rewrites the key with a fresh ttl
     * anyway, and the following run tries again.
     */
    public void renewTtl() {
        try {
            long ttlSeconds = properties.getCache().getTtl().toSeconds();
            List<StreamFullConfigEntity> entities = new ArrayList<>();
            repository.findAll().forEach(entity -> {
                if (entity != null && entity.getSymbolPair() != null) {
                    entity.setTtlSeconds(ttlSeconds);
                    entities.add(entity);
                }
            });
            if (entities.isEmpty()) {
                return;
            }
            repository.saveAll(entities);
            log.info("Renewed ttl to {} for {} projected full config keys",
                    properties.getCache().getTtl(), entities.size());
        } catch (RuntimeException exception) {
            log.warn("Failed to renew ttl of the full config projection in redis", exception);
        }
    }
}
