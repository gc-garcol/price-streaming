package gc.garcol.pricestreaming.centrifugo;

import gc.garcol.pricestreaming.dto.FullSymbolConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Slf4j
@EnableConfigurationProperties(CentrifugoProperties.class)
public class CentrifugoPublisher {

    private final CentrifugoTransport transport;
    private final CentrifugoProperties properties;

    public CentrifugoPublisher(CentrifugoTransport transport, CentrifugoProperties properties) {
        this.transport = transport;
        this.properties = properties;
        log.info("Centrifugo publisher using {} transport", transport.type());
    }

    public void publish(List<FullSymbolConfig> changedConfigs) {
        publish(properties.getChannel(), changedConfigs);
    }

    /**
     * A failed push is logged and dropped: the caller owns state that must not roll back because
     * a browser channel is unreachable.
     */
    public void publish(String channel, List<?> payload) {
        if (payload.isEmpty()) {
            return;
        }
        try {
            transport.publish(channel, payload);
        } catch (RuntimeException exception) {
            log.error("Failed to publish {} changes to centrifugo channel {} over {}",
                    payload.size(), channel, transport.type(), exception);
        }
    }
}
