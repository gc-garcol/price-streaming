package gc.garcol.pricestreaming.stream;

import gc.garcol.pricestreaming.config.ConditionalOnPriceEngine;
import gc.garcol.pricestreaming.config.PriceEngine;
import gc.garcol.pricestreaming.domainlogic.SymbolPriceFeed;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/**
 * Builds the full state both source streams assume before the topology starts consuming: the whole
 * price config table from the database, and one full fetch of the upstream price feed. Runs in an
 * earlier lifecycle phase than the kafka streams client so both snapshots are already on their
 * topics when the tables are built.
 */
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnPriceEngine(PriceEngine.KAFKA_STREAM)
public class PriceStreamBootstrap implements SmartLifecycle {

    private static final int PHASE = Integer.MAX_VALUE - 2000;

    private final PriceConfigSnapshotPublisher priceConfigSnapshotPublisher;
    private final MarketPricePublisher marketPricePublisher;
    private final SymbolPriceFeed symbolPriceFeed;

    private volatile boolean running;

    @Override
    public void start() {
        try {
            priceConfigSnapshotPublisher.publishSnapshot();
            marketPricePublisher.publishAll(symbolPriceFeed.fetchAll(false));
        } catch (RuntimeException exception) {
            // The topology still starts: it catches up from the compacted topics, and the
            // schedulers keep publishing, so a slow broker at boot is not fatal.
            log.error("Failed to seed the price stream topics", exception);
        }
        running = true;
    }

    @Override
    public void stop() {
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return PHASE;
    }
}
