package gc.garcol.pricestreaming.domainlogic;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@Data
@ConfigurationProperties(prefix = "disruptor")
public class DisruptorProperties {

    private int ringBufferPowSize = 12;

    private long shutdownTimeoutSeconds = 10;

    private WaitStrategyType waitStrategy = WaitStrategyType.BLOCKING;

    private Duration waitTimeout = Duration.ofMillis(100);
}
