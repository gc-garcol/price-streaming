package gc.garcol.pricestreaming.stream;

import gc.garcol.pricestreaming.entity.PriceConfig;
import gc.garcol.pricestreaming.repository.PriceConfigRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * Debezium only carries the outbox rows written since the connector was created, so the config
 * table is seeded from the database first. Every snapshot record carries its {@code updated_at},
 * which is what keeps it from overwriting a newer change data capture event on replay.
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "price-stream", name = "enabled", havingValue = "true", matchIfMissing = true)
public class PriceConfigSnapshotPublisher {

    private final PriceConfigRepository repository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String topic;

    public PriceConfigSnapshotPublisher(PriceConfigRepository repository,
                                        KafkaTemplate<String, Object> priceStreamKafkaTemplate,
                                        PriceStreamProperties properties) {
        this.repository = repository;
        this.kafkaTemplate = priceStreamKafkaTemplate;
        this.topic = properties.getTopic().getPriceConfigSnapshot();
    }

    @Transactional(readOnly = true)
    public void publishSnapshot() {
        List<PriceConfig> priceConfigs = repository.findAll();
        priceConfigs.stream()
                .map(PriceConfigEvent::from)
                .forEach(event -> kafkaTemplate.send(topic, event.symbolPair(), event)
                        .whenComplete((result, exception) -> {
                            if (exception != null) {
                                log.error("Failed to publish price config snapshot for {}", event.symbolPair(), exception);
                            }
                        }));
        kafkaTemplate.flush();
        log.info("Published {} price config snapshots to {}", priceConfigs.size(), topic);
    }
}
