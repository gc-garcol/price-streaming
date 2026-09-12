package gc.garcol.pricestreaming.domainlogic;

import gc.garcol.pricestreaming.dto.FullSymbolConfig;
import gc.garcol.pricestreaming.dto.PriceConfigDto;
import gc.garcol.pricestreaming.dto.SymbolDto;
import lombok.Data;

@Data
public class DisruptorEvent {
    DisruptorEventType eventType = DisruptorEventType.NONE;

    PriceConfigDto priceConfigDto = new PriceConfigDto();
    SymbolDto symbolDto = new SymbolDto();
    FullSymbolConfig symbolConfig = new FullSymbolConfig();

    public void reset() {
        eventType = DisruptorEventType.NONE;
        symbolConfig.setPresented(false);
    }
}
