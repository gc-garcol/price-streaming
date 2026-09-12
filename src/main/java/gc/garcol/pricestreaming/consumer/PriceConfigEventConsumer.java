package gc.garcol.pricestreaming.consumer;

import gc.garcol.pricestreaming.constant.PriceConfigEventType;
import gc.garcol.pricestreaming.domainlogic.DisruptorEventPublisher;
import gc.garcol.pricestreaming.dto.PriceConfigDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

@Component
@RequiredArgsConstructor
@Slf4j
public class PriceConfigEventConsumer {

    private static final String EVENT_TYPE_HEADER = "eventType";
    private static final String PAYLOAD_FIELD = "payload";

    private final ObjectMapper objectMapper;
    private final DisruptorEventPublisher disruptorEventPublisher;

    @KafkaListener(
            topics = "${kafka.topic.price-config-events}",
            groupId = "${kafka.group.price-config-events}")
    public void consume(ConsumerRecord<String, String> record) {
        String eventType = header(record, EVENT_TYPE_HEADER);
        if (eventType == null) {
            log.warn("Skipping record without {} header at {}-{}@{}",
                    EVENT_TYPE_HEADER, record.topic(), record.partition(), record.offset());
            return;
        }
        if (record.value() == null) {
            log.debug("Skipping tombstone at {}-{}@{}", record.topic(), record.partition(), record.offset());
            return;
        }
        try {
            PriceConfigDto priceConfig = toPriceConfig(objectMapper.readTree(record.value()));
            log.info("Parsed {} from {}-{}@{}: {}",
                    eventType, record.topic(), record.partition(), record.offset(), priceConfig);
            switch (eventType) {
                case PriceConfigEventType.CREATED, PriceConfigEventType.UPDATED ->
                        disruptorEventPublisher.publishPriceConfig(priceConfig);
                case PriceConfigEventType.DELETED ->
                        disruptorEventPublisher.publishPriceConfigDrop(priceConfig);
                default -> log.warn("Ignoring unknown event type {}", eventType);
            }
        } catch (RuntimeException exception) {
            log.error("Failed to consume record at {}-{}@{}",
                    record.topic(), record.partition(), record.offset(), exception);
        }
    }

    private PriceConfigDto toPriceConfig(JsonNode root) {
        JsonNode payload = root.has(PAYLOAD_FIELD) ? root.get(PAYLOAD_FIELD) : root;
        PriceConfigDto priceConfig = new PriceConfigDto();
        priceConfig.setId(payload.hasNonNull("id") ? payload.get("id").asLong() : null);
        priceConfig.setSymbolPair(payload.hasNonNull("symbolPair") ? payload.get("symbolPair").asString() : null);
        priceConfig.setDeltaPercent(payload.hasNonNull("deltaPercent")
                ? new BigDecimal(payload.get("deltaPercent").asString())
                : null);
        priceConfig.setUpdatedAt(payload.hasNonNull("updatedAt")
                ? Instant.parse(payload.get("updatedAt").asString())
                : null);
        return priceConfig;
    }

    private String header(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
