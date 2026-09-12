package gc.garcol.pricestreaming.scheduler;

import gc.garcol.pricestreaming.domainlogic.DisruptorEventPublisher;
import gc.garcol.pricestreaming.domainlogic.PriceStateProperties;
import gc.garcol.pricestreaming.domainlogic.SymbolPriceFeed;
import gc.garcol.pricestreaming.dto.SymbolDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@Component
@RequiredArgsConstructor
@Slf4j
public class SymbolPriceFeedScheduler {

    private final SymbolPriceFeed symbolPriceFeed;
    private final DisruptorEventPublisher disruptorEventPublisher;
    private final PriceStateProperties properties;

    @Scheduled(fixedDelayString = "${price-state.symbol-feed.fetch-interval}",
            initialDelayString = "${price-state.symbol-feed.fetch-interval}")
    public void fetchSymbolPrices() {
        try {
            List<SymbolDto> symbols = symbolPriceFeed.fetchAll(true);
            symbols.forEach(disruptorEventPublisher::publishSymbolPrice);
            log.info("Published {} symbol prices from scheduled feed fetch", symbols.size());
        } catch (RuntimeException exception) {
            log.error("Scheduled symbol price fetch failed", exception);
        }
    }

    @Scheduled(fixedDelayString = "${price-state.symbol-feed.burst-interval}",
            initialDelayString = "${price-state.symbol-feed.burst-interval}",
            timeUnit = TimeUnit.MILLISECONDS)
    public void burstFetchSymbolPrices() {
        try {
            int maxBurst = Math.max(1, properties.getSymbolFeed().getMaxBurst());
            double publishRatio = properties.getSymbolFeed().getBurstPublishRatio();
            int rounds = ThreadLocalRandom.current().nextInt(1, maxBurst + 1);
            for (int round = 0; round < rounds; round++) {
                List<SymbolDto> symbols = symbolPriceFeed.fetchAll(false);
                symbols.forEach(symbol -> {
                    if (ThreadLocalRandom.current().nextDouble() < publishRatio) {
                        disruptorEventPublisher.publishSymbolPrice(symbol);
                    }
                });
            }
        } catch (RuntimeException exception) {
            log.error("Burst symbol price fetch failed", exception);
        }
    }
}
