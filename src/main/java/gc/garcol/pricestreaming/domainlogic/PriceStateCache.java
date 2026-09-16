package gc.garcol.pricestreaming.domainlogic;

import gc.garcol.pricestreaming.config.ConditionalOnPriceEngine;
import gc.garcol.pricestreaming.config.PriceEngine;
import gc.garcol.pricestreaming.dto.FullSymbolConfig;
import gc.garcol.pricestreaming.entity.FullSymbolConfigEntity;
import gc.garcol.pricestreaming.repository.redis.FullSymbolConfigRedisRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
@RequiredArgsConstructor
@Slf4j
@ConditionalOnPriceEngine(PriceEngine.LMAX)
public class PriceStateCache {

    private static final String PHANTOM_SUFFIX = ":phantom";

    private final FullSymbolConfigRedisRepository repository;
    private final PriceStateProperties properties;

    public List<FullSymbolConfig> read() {
        try {
            List<FullSymbolConfig> configs = new ArrayList<>();
            repository.findAll().forEach(entity -> {
                if (isCacheable(entity)) {
                    configs.add(entity.toFullSymbolConfig());
                }
            });
            return configs.isEmpty() ? null : configs;
        } catch (RuntimeException exception) {
            log.warn("Failed to read price state from redis, falling back to source of truth", exception);
            return null;
        }
    }

    public void write(List<FullSymbolConfig> configs) {
        try {
            long ttlSeconds = properties.getCache().getTtl().toSeconds();
            List<FullSymbolConfigEntity> entities = configs.stream()
                    .filter(config -> config.getSymbolPair() != null)
                    .map(config -> FullSymbolConfigEntity.from(config, ttlSeconds))
                    .toList();
            repository.saveAll(entities);
            log.info("Cached {} symbol configs with ttl {}", entities.size(), properties.getCache().getTtl());
        } catch (RuntimeException exception) {
            log.warn("Failed to write price state to redis", exception);
        }
    }

    private boolean isCacheable(FullSymbolConfigEntity entity) {
        return entity != null
                && entity.getSymbolPair() != null
                && !entity.getSymbolPair().endsWith(PHANTOM_SUFFIX);
    }

    public int renewTtl() {
        try {
            long ttlSeconds = properties.getCache().getTtl().toSeconds();
            List<FullSymbolConfigEntity> entities = new ArrayList<>();
            repository.findAll().forEach(entity -> {
                if (isCacheable(entity)) {
                    entity.setTtlSeconds(ttlSeconds);
                    entities.add(entity);
                }
            });
            repository.saveAll(entities);
            return entities.size();
        } catch (RuntimeException exception) {
            log.warn("Failed to renew price state ttl in redis", exception);
            return 0;
        }
    }
}
