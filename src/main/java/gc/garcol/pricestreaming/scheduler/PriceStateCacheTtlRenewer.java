package gc.garcol.pricestreaming.scheduler;

import gc.garcol.pricestreaming.domainlogic.PriceStateCache;
import gc.garcol.pricestreaming.domainlogic.PriceStateProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class PriceStateCacheTtlRenewer {

    private final PriceStateCache priceStateCache;
    private final PriceStateProperties properties;

    @Scheduled(fixedDelayString = "${price-state.cache.renew-interval}",
            initialDelayString = "${price-state.cache.renew-interval}")
    public void renew() {
        int renewed = priceStateCache.renewTtl();
        if (renewed > 0) {
            log.info("Renewed ttl to {} for {} cached symbol keys", properties.getCache().getTtl(), renewed);
        }
    }
}
