package gc.garcol.pricestreaming.centrifugo;

import java.util.List;

public interface CentrifugoTransport {

    void publish(String channel, List<?> payload);

    CentrifugoTransportType type();
}
