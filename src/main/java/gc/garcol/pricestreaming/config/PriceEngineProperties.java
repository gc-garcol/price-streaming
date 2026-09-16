package gc.garcol.pricestreaming.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Selects the pipeline. Read through {@link ConditionalOnPriceEngine} at bean registration time,
 * so this binding exists for injection and for the startup log rather than for runtime switching:
 * the choice is fixed for the life of the context.
 */
@Data
@ConfigurationProperties(prefix = "price")
public class PriceEngineProperties {

    private PriceEngine engine = PriceEngine.BOTH;
}
