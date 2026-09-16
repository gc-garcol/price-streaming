package gc.garcol.pricestreaming.domainlogic;

import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.dsl.Disruptor;
import gc.garcol.pricestreaming.config.ConditionalOnPriceEngine;
import gc.garcol.pricestreaming.config.PriceEngine;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Component
@RequiredArgsConstructor
@Slf4j
@ConditionalOnPriceEngine(PriceEngine.LMAX)
public class DisruptorLifecycle implements SmartLifecycle {

    private static final int PHASE = Integer.MAX_VALUE - 1000;

    private final Disruptor<DisruptorEvent> priceDisruptor;
    private final DisruptorProperties properties;
    private final PriceStateLoader priceStateLoader;

    private volatile boolean running;

    @Override
    public void start() {
        priceStateLoader.load();
        priceDisruptor.start();
        running = true;
        log.info("Disruptor started with ring buffer size {}", priceDisruptor.getRingBuffer().getBufferSize());
    }

    @Override
    public void stop() {
        running = false;
        try {
            priceDisruptor.shutdown(properties.getShutdownTimeoutSeconds(), TimeUnit.SECONDS);
            log.info("Disruptor shut down cleanly");
        } catch (TimeoutException exception) {
            log.warn("Disruptor did not drain within {}s, halting", properties.getShutdownTimeoutSeconds());
            haltQuietly();
        } catch (RuntimeException exception) {
            log.error("Disruptor shutdown failed, halting", exception);
            haltQuietly();
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    private void haltQuietly() {
        try {
            priceDisruptor.halt();
        } catch (RuntimeException exception) {
            log.error("Disruptor halt failed", exception);
        }
    }

    @Override
    public int getPhase() {
        return PHASE;
    }
}
