package gc.garcol.pricestreaming.domainlogic;

import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import gc.garcol.pricestreaming.dto.PriceConfigDto;
import gc.garcol.pricestreaming.dto.SymbolDto;
import org.springframework.stereotype.Component;

@Component
public class DisruptorEventPublisher {

    private static final EventTranslatorOneArg<DisruptorEvent, PriceConfigDto> PRICE_CONFIG_TRANSLATOR =
            (event, sequence, source) -> {
                event.setEventType(DisruptorEventType.PRICE_CONFIG);
                copyPriceConfig(source, event);
            };

    private static final EventTranslatorOneArg<DisruptorEvent, PriceConfigDto> PRICE_CONFIG_DROP_TRANSLATOR =
            (event, sequence, source) -> {
                event.setEventType(DisruptorEventType.PRICE_CONFIG_DROP);
                copyPriceConfig(source, event);
            };

    private static final EventTranslatorOneArg<DisruptorEvent, SymbolDto> SYMBOL_PRICE_TRANSLATOR =
            (event, sequence, source) -> {
                event.setEventType(DisruptorEventType.SYMBOL_PRICE);
                copySymbol(source, event);
            };

    private static final EventTranslatorOneArg<DisruptorEvent, SymbolDto> SYMBOL_PRICE_CHANGED_TRANSLATOR =
            (event, sequence, source) -> {
                event.setEventType(DisruptorEventType.SYMBOL_PRICE_CHANGED);
                copySymbol(source, event);
            };

    private final RingBuffer<DisruptorEvent> ringBuffer;

    public DisruptorEventPublisher(Disruptor<DisruptorEvent> priceDisruptor) {
        this.ringBuffer = priceDisruptor.getRingBuffer();
    }

    public void publishPriceConfig(PriceConfigDto priceConfig) {
        ringBuffer.publishEvent(PRICE_CONFIG_TRANSLATOR, priceConfig);
    }

    public void publishPriceConfigDrop(PriceConfigDto priceConfig) {
        ringBuffer.publishEvent(PRICE_CONFIG_DROP_TRANSLATOR, priceConfig);
    }

    public void publishSymbolPrice(SymbolDto symbol) {
        ringBuffer.publishEvent(SYMBOL_PRICE_TRANSLATOR, symbol);
    }

    public void publishSymbolPriceChanged(SymbolDto symbol) {
        ringBuffer.publishEvent(SYMBOL_PRICE_CHANGED_TRANSLATOR, symbol);
    }

    private static void copyPriceConfig(PriceConfigDto source, DisruptorEvent event) {
        PriceConfigDto target = event.getPriceConfigDto();
        target.setId(source.getId());
        target.setSymbolPair(source.getSymbolPair());
        target.setDeltaPercent(source.getDeltaPercent());
        target.setUpdatedAt(source.getUpdatedAt());
    }

    private static void copySymbol(SymbolDto source, DisruptorEvent event) {
        SymbolDto target = event.getSymbolDto();
        target.setSymbolPair(source.getSymbolPair());
        target.setPrice(source.getPrice());
    }
}
