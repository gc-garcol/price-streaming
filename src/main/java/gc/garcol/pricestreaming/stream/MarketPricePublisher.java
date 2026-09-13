package gc.garcol.pricestreaming.stream;

import gc.garcol.pricestreaming.dto.SymbolDto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Feeds the {@code MarketPrice} stream. Records are keyed by symbol pair so the topology can read
 * the topic straight into a KTable without a repartition.
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "price-stream", name = "enabled", havingValue = "true", matchIfMissing = true)
public class MarketPricePublisher {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String topic;

    public MarketPricePublisher(KafkaTemplate<String, Object> priceStreamKafkaTemplate,
                                PriceStreamProperties properties) {
        this.kafkaTemplate = priceStreamKafkaTemplate;
        this.topic = properties.getTopic().getMarketPrice();
    }

    public void publish(SymbolDto symbol) {
        if (symbol.getSymbolPair() == null) {
            return;
        }
        MarketPrice marketPrice = MarketPrice.from(symbol, System.currentTimeMillis());
        kafkaTemplate.send(topic, marketPrice.symbolPair(), marketPrice)
                .whenComplete((result, exception) -> {
                    if (exception != null) {
                        log.error("Failed to publish market price for {}", marketPrice.symbolPair(), exception);
                    }
                });
    }

    /**
     * Publishes a whole feed snapshot and waits for the broker, so the caller knows the market
     * price table has every symbol before it moves on.
     */
    public void publishAll(List<SymbolDto> symbols) {
        symbols.forEach(this::publish);
        kafkaTemplate.flush();
        log.info("Published {} market prices to {}", symbols.size(), topic);
    }
}
