package gc.garcol.pricestreaming.stream;

import gc.garcol.pricestreaming.constant.PriceConfigEventType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

/**
 * Turns the debezium outbox envelope into a {@link PriceConfigEvent}. The event type only lives in
 * the {@code eventType} header, which is why this stage is a processor instead of a plain
 * {@code mapValues}. Records that cannot be parsed are dropped instead of forwarded, so one bad
 * outbox row cannot stall the table.
 */
@Slf4j
@RequiredArgsConstructor
public class PriceConfigCdcProcessor implements FixedKeyProcessor<String, String, PriceConfigEvent> {

    private static final String EVENT_TYPE_HEADER = "eventType";
    private static final String PAYLOAD_FIELD = "payload";

    private final ObjectMapper objectMapper;

    private FixedKeyProcessorContext<String, PriceConfigEvent> context;

    @Override
    public void init(FixedKeyProcessorContext<String, PriceConfigEvent> context) {
        this.context = context;
    }

    @Override
    public void process(FixedKeyRecord<String, String> record) {
        if (record.value() == null) {
            log.debug("Skipping outbox tombstone for key {}", record.key());
            return;
        }
        Boolean deleted = deleted(header(record, EVENT_TYPE_HEADER));
        if (deleted == null) {
            return;
        }
        try {
            PriceConfigEvent event = toEvent(objectMapper.readTree(record.value()), deleted);
            if (event.symbolPair() == null) {
                log.warn("Skipping outbox record without symbolPair, key {}", record.key());
                return;
            }
            context.forward(record.withValue(event));
        } catch (RuntimeException exception) {
            log.error("Failed to parse outbox record for key {}", record.key(), exception);
        }
    }

    /**
     * @return whether the event drops the symbol pair, or {@code null} when the record carries no
     * event type this topology understands
     */
    private Boolean deleted(String eventType) {
        if (eventType == null) {
            log.warn("Skipping outbox record without {} header", EVENT_TYPE_HEADER);
            return null;
        }
        return switch (eventType) {
            case PriceConfigEventType.CREATED, PriceConfigEventType.UPDATED -> Boolean.FALSE;
            case PriceConfigEventType.DELETED -> Boolean.TRUE;
            default -> {
                log.warn("Ignoring unknown event type {}", eventType);
                yield null;
            }
        };
    }

    private PriceConfigEvent toEvent(JsonNode root, boolean deleted) {
        JsonNode payload = root.has(PAYLOAD_FIELD) ? root.get(PAYLOAD_FIELD) : root;
        return new PriceConfigEvent(
                payload.hasNonNull("id") ? payload.get("id").asLong() : null,
                payload.hasNonNull("symbolPair") ? payload.get("symbolPair").asString() : null,
                payload.hasNonNull("deltaPercent") ? new BigDecimal(payload.get("deltaPercent").asString()) : null,
                payload.hasNonNull("updatedAt") ? Instant.parse(payload.get("updatedAt").asString()).toEpochMilli() : 0L,
                deleted);
    }

    private String header(FixedKeyRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
