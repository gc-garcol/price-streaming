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
        if (changedConfigs.isEmpty()) {
            return;
        }
        try {
            transport.publish(properties.getChannel(), changedConfigs);
        } catch (RuntimeException exception) {
            log.error("Failed to publish {} changed configs to centrifugo channel {} over {}",
                    changedConfigs.size(), properties.getChannel(), transport.type(), exception);
        }
    }
}
