package gc.garcol.pricestreaming.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.repository.configuration.EnableRedisRepositories;
import org.springframework.data.redis.core.RedisKeyValueAdapter;

@Configuration
@EnableRedisRepositories(
        basePackages = "gc.garcol.pricestreaming.repository.redis",
        shadowCopy = RedisKeyValueAdapter.ShadowCopy.OFF,
        enableKeyspaceEvents = RedisKeyValueAdapter.EnableKeyspaceEvents.ON_STARTUP)
public class RedisRepositoryConfig {
}
