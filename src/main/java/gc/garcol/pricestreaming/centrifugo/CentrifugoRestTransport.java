package gc.garcol.pricestreaming.centrifugo;

import gc.garcol.pricestreaming.dto.FullSymbolConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Component
@Slf4j
@ConditionalOnProperty(name = "centrifugo.transport", havingValue = "rest", matchIfMissing = true)
public class CentrifugoRestTransport implements CentrifugoTransport {

    private final RestClient restClient;
    private final CentrifugoProperties properties;
    private final Map<String, Object> request = new LinkedHashMap<>();
    private final Map<String, Object> params = new LinkedHashMap<>();

    public CentrifugoRestTransport(CentrifugoProperties properties) {
        this.properties = properties;
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(properties.getTimeout());
        requestFactory.setReadTimeout(properties.getTimeout());
        this.restClient = RestClient.builder()
                .baseUrl(properties.getUrl())
                .requestFactory(requestFactory)
                .build();
        this.request.put("method", "publish");
        this.request.put("params", params);
    }

    @Override
    public void publish(String channel, List<FullSymbolConfig> changedConfigs) {
        params.put("channel", channel);
        params.put("data", changedConfigs);
        restClient.post()
                .uri(properties.getApiPath())
                .header(HttpHeaders.AUTHORIZATION, "apikey " + properties.getApiKey())
                .contentType(MediaType.APPLICATION_JSON)
                .body(request)
                .retrieve()
                .toBodilessEntity();
    }

    @Override
    public CentrifugoTransportType type() {
        return CentrifugoTransportType.REST;
    }
}
