package gc.garcol.pricestreaming.domainlogic;

import com.lmax.disruptor.EventHandler;
import gc.garcol.pricestreaming.config.ConditionalOnPriceEngine;
import gc.garcol.pricestreaming.config.PriceEngine;
import gc.garcol.pricestreaming.dto.FullSymbolConfig;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnPriceEngine(PriceEngine.LMAX)
public class Disruptor1EventDomainHandler implements EventHandler<DisruptorEvent> {

    @Getter
    private final PriceState priceState;

    @Override
    public void onEvent(DisruptorEvent event, long sequence, boolean endOfBatch) {
        try {
            dispatch(event);
        } catch (RuntimeException exception) {
            log.error("Failed to handle disruptor event {} at sequence {}", event.getEventType(), sequence, exception);
        }
    }

    private void dispatch(DisruptorEvent event) {
        FullSymbolConfig config = switch (event.getEventType()) {
            case PRICE_CONFIG, PRICE_CONFIG_CHANGED -> priceState.applyPriceConfig(event.getPriceConfigDto());
            case SYMBOL_PRICE, SYMBOL_PRICE_CHANGED -> priceState.applySymbolPrice(event.getSymbolDto());
            case PRICE_CONFIG_DROP -> priceState.dropPriceConfig(event.getPriceConfigDto());
            case NONE -> {
                log.warn("Received disruptor event with type NONE");
                yield null;
            }
        };
        if (config != null && config.isPresented()) {
            copyInto(event.getSymbolConfig(), config);
        }
    }

    private void copyInto(FullSymbolConfig target, FullSymbolConfig source) {
        target.setId(source.getId());
        target.setSymbolPair(source.getSymbolPair());
        target.setPrice(source.getPrice());
        target.setDeltaPercent(source.getDeltaPercent());
        target.setLowPrice(source.getLowPrice());
        target.setHighPrice(source.getHighPrice());
        target.setPresented(true);
        target.setDeleted(source.isDeleted());
        target.setConfigEventAt(source.getConfigEventAt());
    }
}
