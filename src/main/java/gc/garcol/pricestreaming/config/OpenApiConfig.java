package gc.garcol.pricestreaming.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI priceStreamingOpenApi() {
        return new OpenAPI().info(new Info()
                .title("price-streaming API")
                .version("v1")
                .description("Price configuration and streaming endpoints"));
    }
}
