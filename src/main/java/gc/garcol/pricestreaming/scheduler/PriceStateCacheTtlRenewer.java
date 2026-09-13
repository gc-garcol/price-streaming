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

    private final PriceStateCache priceStateCache;
    private final PriceStateProperties properties;

    /** null when the kafka streams pipeline is switched off */
    private final StreamFullConfigCache streamFullConfigCache;

    public PriceStateCacheTtlRenewer(PriceStateCache priceStateCache,
                                     PriceStateProperties properties,
                                     ObjectProvider<StreamFullConfigCache> streamFullConfigCache) {
        this.priceStateCache = priceStateCache;
        this.properties = properties;
        this.streamFullConfigCache = streamFullConfigCache.getIfAvailable();
    }

    /**
     * One schedule renews both redis projections: the ring buffer cache and, when the topology is
     * running, the full config projection the kafka listener maintains.
     */
    @Scheduled(fixedDelayString = "${price-state.cache.renew-interval}",
            initialDelayString = "${price-state.cache.renew-interval}")
    public void renew() {
        int renewed = priceStateCache.renewTtl();
        if (renewed > 0) {
            log.info("Renewed ttl to {} for {} cached symbol keys", properties.getCache().getTtl(), renewed);
        }
        if (streamFullConfigCache != null) {
            streamFullConfigCache.renewTtl();
        }
    }
}
