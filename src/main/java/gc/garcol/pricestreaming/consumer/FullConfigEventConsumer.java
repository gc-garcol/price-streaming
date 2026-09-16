package gc.garcol.pricestreaming.consumer;

import gc.garcol.pricestreaming.centrifugo.CentrifugoProperties;
import gc.garcol.pricestreaming.centrifugo.CentrifugoPublisher;
import gc.garcol.pricestreaming.config.ConditionalOnPriceEngine;
import gc.garcol.pricestreaming.config.PriceEngine;
import gc.garcol.pricestreaming.entity.StreamFullConfigEntity;
import gc.garcol.pricestreaming.repository.redis.StreamFullConfigRedisRepository;
import gc.garcol.pricestreaming.stream.FullConfig;
import gc.garcol.pricestreaming.stream.FullConfigDeletion;
import gc.garcol.pricestreaming.stream.PriceStreamProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Projects the join result onto redis, so paging is served from a shared store instead of the
 * state store of whichever instance owns the partition, then pushes the same changes to the
 * centrifugo channel browsers subscribe to.
 *
 * <p>The listener is a batch listener: a busy feed writes the same symbol pair many times per
 * poll, and only the last record of each key has to reach redis and the browser.
 */
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnPriceEngine(PriceEngine.KAFKA_STREAM)
public class FullConfigEventConsumer {

    private final ObjectMapper objectMapper;
    private final StreamFullConfigRedisRepository repository;
    private final CentrifugoPublisher centrifugoPublisher;
    private final CentrifugoProperties centrifugoProperties;
    private final PriceStreamProperties streamProperties;

    @KafkaListener(
            topics = "${price-stream.topic.full-config}",
            groupId = "${price-stream.consumer.group-id}",
            containerFactory = "fullConfigListenerContainerFactory")
    public void consume(List<ConsumerRecord<String, String>> records) {
        Map<String, FullConfig> upserts = new LinkedHashMap<>();
        Set<String> deletions = new LinkedHashSet<>();
        for (ConsumerRecord<String, String> record : records) {
            collapse(record, upserts, deletions);
        }
        if (upserts.isEmpty() && deletions.isEmpty()) {
            return;
        }
        // Redis failures are not swallowed: the batch is redelivered, otherwise a dropped
        // deletion would leave a symbol pair in the projection forever.
        if (!deletions.isEmpty()) {
            repository.deleteAllById(deletions);
        }
        if (!upserts.isEmpty()) {
            // Every write carries the ttl, otherwise saving would persist the key and undo what
            // StreamFullConfigCache renews.
            long ttlSeconds = streamProperties.getCache().getTtl().toSeconds();
            List<StreamFullConfigEntity> entities = new ArrayList<>(upserts.size());
            upserts.values().forEach(config -> entities.add(StreamFullConfigEntity.from(config, ttlSeconds)));
            repository.saveAll(entities);
        }
        push(upserts, deletions);
        log.debug("Projected {} records onto redis as {} upserts and {} deletions",
                records.size(), upserts.size(), deletions.size());
    }

    /**
     * One push per batch carrying both shapes: the joined config for a change, and a deletion
     * marker for a symbol pair the join dropped. Failures are logged inside the publisher, so a
     * missed push never holds up the offset commit.
     */
    private void push(Map<String, FullConfig> upserts, Set<String> deletions) {
        List<Object> changes = new ArrayList<>(upserts.size() + deletions.size());
        changes.addAll(upserts.values());
        deletions.forEach(symbolPair -> changes.add(FullConfigDeletion.of(symbolPair)));
        centrifugoPublisher.publish(centrifugoProperties.getStreamChannel(), changes);
    }

    /**
     * Keeps only the last record per symbol pair. Records of one key always arrive in order
     * because the topic is keyed by symbol pair, so the last one wins.
     */
    private void collapse(ConsumerRecord<String, String> record,
                          Map<String, FullConfig> upserts,
                          Set<String> deletions) {
        String symbolPair = record.key();
        if (symbolPair == null) {
            log.warn("Skipping record without key at {}-{}@{}",
                    record.topic(), record.partition(), record.offset());
            return;
        }
        if (record.value() == null) {
            upserts.remove(symbolPair);
            deletions.add(symbolPair);
            return;
        }
        try {
            FullConfig config = objectMapper.readValue(record.value(), FullConfig.class);
            deletions.remove(symbolPair);
            upserts.put(symbolPair, config);
        } catch (RuntimeException exception) {
            log.error("Failed to parse full config at {}-{}@{}",
                    record.topic(), record.partition(), record.offset(), exception);
        }
    }
}
