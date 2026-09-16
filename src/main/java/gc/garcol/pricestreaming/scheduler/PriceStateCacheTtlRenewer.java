package gc.garcol.pricestreaming.scheduler;

import gc.garcol.pricestreaming.domainlogic.PriceStateCache;
import gc.garcol.pricestreaming.domainlogic.PriceStateProperties;
import gc.garcol.pricestreaming.stream.StreamFullConfigCache;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class PriceStateCacheTtlRenewer {

    private final PriceStateProperties properties;

    /** null when {@code price.engine} leaves the ring buffer out */
    private final PriceStateCache priceStateCache;

    /** null when {@code price.engine} leaves the kafka streams pipeline out */
    private final StreamFullConfigCache streamFullConfigCache;

    public PriceStateCacheTtlRenewer(PriceStateProperties properties,
                                     ObjectProvider<PriceStateCache> priceStateCache,
                                     ObjectProvider<StreamFullConfigCache> streamFullConfigCache) {
        this.properties = properties;
        this.priceStateCache = priceStateCache.getIfAvailable();
        this.streamFullConfigCache = streamFullConfigCache.getIfAvailable();
    }

    /**
     * One schedule renews the redis projection of every running pipeline: the ring buffer cache,
     * and the full config projection the kafka listener maintains.
     */
    @Scheduled(fixedDelayString = "${price-state.cache.renew-interval}",
            initialDelayString = "${price-state.cache.renew-interval}")
    public void renew() {
        if (priceStateCache != null) {
            int renewed = priceStateCache.renewTtl();
            if (renewed > 0) {
                log.info("Renewed ttl to {} for {} cached symbol keys", properties.getCache().getTtl(), renewed);
            }
        }
        if (streamFullConfigCache != null) {
            streamFullConfigCache.renewTtl();
        }
    }
}
