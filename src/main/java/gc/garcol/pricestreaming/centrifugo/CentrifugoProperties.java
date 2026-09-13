package gc.garcol.pricestreaming.centrifugo;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@Data
@ConfigurationProperties(prefix = "centrifugo")
public class CentrifugoProperties {

    private String url = "http://localhost:8000";

    private String apiPath = "/api";

    private String apiKey = "";

    private String channel = "price-stream";

    /** channel the kafka streams join result is pushed to */
    private String streamChannel = "full-config-stream";

    private Duration timeout = Duration.ofSeconds(2);

    private CentrifugoTransportType transport = CentrifugoTransportType.REST;

    private Grpc grpc = new Grpc();

    @Data
    public static class Grpc {
        private String host = "localhost";
        private int port = 10000;
        private Duration deadline = Duration.ofSeconds(2);
    }
}
