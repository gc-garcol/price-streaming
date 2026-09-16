package gc.garcol.pricestreaming;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
// Both pipelines can be switched off by price.engine, so the property beans are scanned here
// instead of being enabled from a configuration class that the selector may have removed.
@ConfigurationPropertiesScan
public class PriceStreamingApplication {

    public static void main(String[] args) {
        SpringApplication.run(PriceStreamingApplication.class, args);
    }

}
