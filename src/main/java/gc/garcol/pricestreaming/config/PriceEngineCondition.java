package gc.garcol.pricestreaming.config;

import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.boot.context.properties.bind.Binder;

import java.util.Map;

/**
 * Backs {@link ConditionalOnPriceEngine}. The value is bound rather than read as a raw string so
 * relaxed binding applies: {@code kafka-stream}, {@code KAFKA_STREAM} and {@code kafka_stream} all
 * select the same pipeline, and a typo fails the context at startup instead of silently leaving
 * both pipelines switched off.
 */
public class PriceEngineCondition implements Condition {

    static PriceEngine resolve(Environment environment) {
        return Binder.get(environment)
                .bind("price.engine", PriceEngine.class)
                .orElse(PriceEngine.BOTH);
    }

    @Override
    public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
        Map<String, Object> attributes = metadata.getAnnotationAttributes(ConditionalOnPriceEngine.class.getName());
        if (attributes == null) {
            return true;
        }
        PriceEngine required = (PriceEngine) attributes.get("value");
        return resolve(context.getEnvironment()).includes(required);
    }
}
