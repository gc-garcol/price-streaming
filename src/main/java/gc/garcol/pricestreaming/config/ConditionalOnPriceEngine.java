package gc.garcol.pricestreaming.config;

import org.springframework.context.annotation.Conditional;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Registers the bean only when {@code price.engine} selects the given pipeline, either on its own
 * or through {@link PriceEngine#BOTH}.
 */
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Conditional(PriceEngineCondition.class)
public @interface ConditionalOnPriceEngine {

    PriceEngine value();
}
