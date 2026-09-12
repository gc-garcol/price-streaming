package gc.garcol.pricestreaming.domainlogic;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "disruptor-handler")
public class DisruptorHandlerProperties {

    private int maxHandler2BatchSize = 100;

    private int maxHandler3BatchSize = 100;
}
