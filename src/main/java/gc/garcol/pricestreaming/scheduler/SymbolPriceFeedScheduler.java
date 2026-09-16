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
    private final PriceStateProperties properties;

    /**
     * null when {@code price.engine} leaves the ring buffer out
     */
    private final DisruptorEventPublisher disruptorEventPublisher;

    /**
     * null when {@code price.engine} leaves the kafka streams pipeline out
     */
    private final MarketPricePublisher marketPricePublisher;

    public SymbolPriceFeedScheduler(SymbolPriceFeed symbolPriceFeed,
                                    PriceStateProperties properties,
                                    ObjectProvider<DisruptorEventPublisher> disruptorEventPublisher,
                                    ObjectProvider<MarketPricePublisher> marketPricePublisher) {
        this.symbolPriceFeed = symbolPriceFeed;
        this.properties = properties;
        this.disruptorEventPublisher = disruptorEventPublisher.getIfAvailable();
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
     * The same tick feeds whichever pipelines {@code price.engine} selected: the ring buffer, and
     * the MarketPrice topic the kafka streams topology joins against.
     */
    private void publish(SymbolDto symbol) {
        if (disruptorEventPublisher != null) {
            disruptorEventPublisher.publishSymbolPrice(symbol);
        }
        if (marketPricePublisher != null) {
            marketPricePublisher.publish(symbol);
        }
    }
}
