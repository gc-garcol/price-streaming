package gc.garcol.pricestreaming.scheduler;

import gc.garcol.pricestreaming.domainlogic.DisruptorEventPublisher;
import gc.garcol.pricestreaming.domainlogic.PriceStateProperties;
import gc.garcol.pricestreaming.domainlogic.SymbolPriceFeed;
import gc.garcol.pricestreaming.dto.SymbolDto;
import gc.garcol.pricestreaming.stream.MarketPricePublisher;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class SymbolPriceFeedScheduler {

    private final SymbolPriceFeed symbolPriceFeed;
    private final DisruptorEventPublisher disruptorEventPublisher;
    private final PriceStateProperties properties;

    /**
     * null when the kafka streams pipeline is switched off
     */
    private final MarketPricePublisher marketPricePublisher;

    public SymbolPriceFeedScheduler(SymbolPriceFeed symbolPriceFeed,
                                    DisruptorEventPublisher disruptorEventPublisher,
                                    PriceStateProperties properties,
                                    ObjectProvider<MarketPricePublisher> marketPricePublisher) {
        this.symbolPriceFeed = symbolPriceFeed;
        this.disruptorEventPublisher = disruptorEventPublisher;
        this.properties = properties;
        this.marketPricePublisher = marketPricePublisher.getIfAvailable();
    }

    @Scheduled(fixedDelayString = "${price-state.symbol-feed.fetch-interval}",
            initialDelayString = "${price-state.symbol-feed.fetch-interval}")
    public void fetchSymbolPrices() {
        try {
            List<SymbolDto> symbols = symbolPriceFeed.fetchAll(true);
            symbols.forEach(this::publish);
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
                        publish(symbol);
                    }
                });
            }
        } catch (RuntimeException exception) {
            log.error("Burst symbol price fetch failed", exception);
        }
    }

    /**
     * The same tick feeds both pipelines: the ring buffer, and the MarketPrice topic the kafka
     * streams topology joins against.
     */
    private void publish(SymbolDto symbol) {
        disruptorEventPublisher.publishSymbolPrice(symbol);
        if (marketPricePublisher != null) {
            marketPricePublisher.publish(symbol);
        }
    }
}
