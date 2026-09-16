package gc.garcol.pricestreaming.domainlogic;

import com.lmax.disruptor.EventHandler;
import gc.garcol.pricestreaming.config.ConditionalOnPriceEngine;
import gc.garcol.pricestreaming.config.PriceEngine;
import gc.garcol.pricestreaming.dto.FullSymbolConfig;
import gc.garcol.pricestreaming.entity.FullSymbolConfigEntity;
import gc.garcol.pricestreaming.repository.redis.FullSymbolConfigRedisRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Component
@ConditionalOnPriceEngine(PriceEngine.LMAX)
public class Disruptor2EventCachingHandler implements EventHandler<DisruptorEvent> {

    private final FullSymbolConfigRedisRepository repository;
    private final long ttlSeconds;
    private final int maxBatchingSize;
    private final List<FullSymbolConfigEntity> cacheEntities;
    private final List<String> deletedSymbolPairs;

    private int batchingCount = 0;
    private int cacheIndex = -1;

    public Disruptor2EventCachingHandler(FullSymbolConfigRedisRepository repository,
                                         PriceStateProperties priceStateProperties,
                                         DisruptorHandlerProperties handlerProperties) {
        this.repository = repository;
        this.ttlSeconds = priceStateProperties.getCache().getTtl().toSeconds();
        this.maxBatchingSize = Math.max(1, handlerProperties.getMaxHandler2BatchSize());
        this.cacheEntities = new ArrayList<>(maxBatchingSize);
        for (int i = 0; i < maxBatchingSize; i++) {
            cacheEntities.add(new FullSymbolConfigEntity());
        }
        this.deletedSymbolPairs = new ArrayList<>(maxBatchingSize);
    }

    @Override
    public void onEvent(DisruptorEvent event, long sequence, boolean endOfBatch) {
        FullSymbolConfig config = event.getSymbolConfig();
        if (config.isPresented() && config.getSymbolPair() != null) {
            if (config.isDeleted()) {
                deletedSymbolPairs.add(config.getSymbolPair());
            } else {
                cacheIndex++;
                copyInto(cacheEntities.get(cacheIndex), config);
            }
            batchingCount++;
        }
        if ((endOfBatch && batchingCount > 0) || batchingCount == maxBatchingSize) {
            flush();
        }
    }

    private void flush() {
        if (cacheIndex < 0 && deletedSymbolPairs.isEmpty()) {
            return;
        }
        try {
            if (cacheIndex >= 0) {
                repository.saveAll(cacheEntities.subList(0, cacheIndex + 1));
            }
            if (!deletedSymbolPairs.isEmpty()) {
                repository.deleteAllById(deletedSymbolPairs);
            }
        } catch (RuntimeException exception) {
            log.error("Failed to persist {} cached configs and {} deletions to redis",
                    cacheIndex + 1, deletedSymbolPairs.size(), exception);
        } finally {
            cacheIndex = -1;
            batchingCount = 0;
            deletedSymbolPairs.clear();
        }
    }

    private void copyInto(FullSymbolConfigEntity target, FullSymbolConfig source) {
        target.setSymbolPair(source.getSymbolPair());
        target.setConfigId(source.getId());
        target.setPrice(source.getPrice());
        target.setDeltaPercent(source.getDeltaPercent());
        target.setLowPrice(source.getLowPrice());
        target.setHighPrice(source.getHighPrice());
        target.setPresented(source.isPresented());
        target.setDeleted(source.isDeleted());
        target.setConfigEventAt(source.getConfigEventAt());
        target.setTtlSeconds(ttlSeconds);
    }
}
