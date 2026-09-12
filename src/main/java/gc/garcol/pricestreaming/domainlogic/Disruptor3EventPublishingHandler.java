package gc.garcol.pricestreaming.domainlogic;

import com.lmax.disruptor.EventHandler;
import gc.garcol.pricestreaming.centrifugo.CentrifugoPublisher;
import gc.garcol.pricestreaming.dto.FullSymbolConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Component
public class Disruptor3EventPublishingHandler implements EventHandler<DisruptorEvent> {

    private final CentrifugoPublisher centrifugoPublisher;
    private final int maxBatchingSize;
    private final List<FullSymbolConfig> changedConfigs;

    private int changedIndex = -1;

    public Disruptor3EventPublishingHandler(CentrifugoPublisher centrifugoPublisher,
                                            DisruptorHandlerProperties handlerProperties) {
        this.centrifugoPublisher = centrifugoPublisher;
        this.maxBatchingSize = Math.max(1, handlerProperties.getMaxHandler3BatchSize());
        this.changedConfigs = new ArrayList<>(maxBatchingSize);
        for (int i = 0; i < maxBatchingSize; i++) {
            changedConfigs.add(new FullSymbolConfig());
        }
    }

    @Override
    public void onEvent(DisruptorEvent event, long sequence, boolean endOfBatch) {
        FullSymbolConfig config = event.getSymbolConfig();
        if (config.isPresented() && !config.isDeleted() && config.getSymbolPair() != null) {
            changedIndex++;
            copyInto(changedConfigs.get(changedIndex), config);
        }
        if ((endOfBatch && changedIndex >= 0) || changedIndex == maxBatchingSize - 1) {
            flush();
        }
    }

    private void flush() {
        if (changedIndex < 0) {
            return;
        }
        try {
            centrifugoPublisher.publish(changedConfigs.subList(0, changedIndex + 1));
        } catch (RuntimeException exception) {
            log.error("Failed to publish {} changed configs", changedIndex + 1, exception);
        } finally {
            changedIndex = -1;
        }
    }

    private void copyInto(FullSymbolConfig target, FullSymbolConfig source) {
        target.setSymbolPair(source.getSymbolPair());
        target.setId(source.getId());
        target.setPrice(source.getPrice());
        target.setDeltaPercent(source.getDeltaPercent());
        target.setLowPrice(source.getLowPrice());
        target.setHighPrice(source.getHighPrice());
        target.setPresented(source.isPresented());
        target.setDeleted(source.isDeleted());
        target.setConfigEventAt(source.getConfigEventAt());
    }
}
