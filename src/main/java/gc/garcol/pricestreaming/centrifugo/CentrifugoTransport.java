package gc.garcol.pricestreaming.centrifugo;

import gc.garcol.pricestreaming.dto.FullSymbolConfig;

import java.util.List;

public interface CentrifugoTransport {

    void publish(String channel, List<FullSymbolConfig> changedConfigs);

    CentrifugoTransportType type();
}
