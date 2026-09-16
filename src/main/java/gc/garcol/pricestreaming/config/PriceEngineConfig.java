package gc.garcol.pricestreaming.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;

/**
 * Logs the selected pipeline once, so a deployment running only half of the system says so in its
 * first lines instead of only through the beans that are missing.
 */
@Slf4j
@Configuration
public class PriceEngineConfig {

    public PriceEngineConfig(PriceEngineProperties properties) {
        log.info("Price engine {}: lmax pipeline {}, kafka streams pipeline {}",
                properties.getEngine(),
                properties.getEngine().includes(PriceEngine.LMAX) ? "on" : "off",
                properties.getEngine().includes(PriceEngine.KAFKA_STREAM) ? "on" : "off");
    }
}
