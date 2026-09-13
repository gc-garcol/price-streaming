package gc.garcol.pricestreaming.stream;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import tools.jackson.databind.json.JsonMapper;

import java.util.Map;

/**
 * Wiring for the kafka streams pipeline: the topics it reads and writes, the json serdes for the
 * three value types, and the producer the schedulers use to feed it.
 */
@Slf4j
@Configuration
@EnableKafkaStreams
@EnableConfigurationProperties(PriceStreamProperties.class)
@ConditionalOnProperty(prefix = "price-stream", name = "enabled", havingValue = "true", matchIfMissing = true)
public class PriceStreamConfig {

    /**
     * Type headers would leak {@code gc.garcol} class names into every record, and both ends of
     * each topic already know the type, so the serdes are pinned to a target class instead.
     */
    private static <T> Serde<T> jsonSerde(Class<T> type, JsonMapper jsonMapper) {
        return new JacksonJsonSerde<>(type, jsonMapper).noTypeInfo().ignoreTypeHeaders();
    }

    @Bean
    public Serde<PriceConfigEvent> priceConfigEventSerde(JsonMapper jsonMapper) {
        return jsonSerde(PriceConfigEvent.class, jsonMapper);
    }

    @Bean
    public Serde<MarketPrice> marketPriceSerde(JsonMapper jsonMapper) {
        return jsonSerde(MarketPrice.class, jsonMapper);
    }

    @Bean
    public Serde<FullConfig> fullConfigSerde(JsonMapper jsonMapper) {
        return jsonSerde(FullConfig.class, jsonMapper);
    }

    /**
     * Every topic in the pipeline is a table changelog keyed by symbol pair, so all of them are
     * compacted: a restarted topology replays the latest state per symbol instead of the whole
     * history. Partition counts must match, otherwise the join sides are not co-partitioned.
     */
    @Bean
    public NewTopic marketPriceTopic(PriceStreamProperties properties) {
        return compactedTopic(properties.getTopic().getMarketPrice(), properties);
    }

    @Bean
    public NewTopic priceConfigSnapshotTopic(PriceStreamProperties properties) {
        return compactedTopic(properties.getTopic().getPriceConfigSnapshot(), properties);
    }

    @Bean
    public NewTopic fullConfigTopic(PriceStreamProperties properties) {
        return compactedTopic(properties.getTopic().getFullConfig(), properties);
    }

    private NewTopic compactedTopic(String name, PriceStreamProperties properties) {
        return TopicBuilder.name(name)
                .partitions(properties.getPartitions())
                .replicas(properties.getReplicas())
                .compact()
                .build();
    }

    @Bean
    public ProducerFactory<String, Object> priceStreamProducerFactory(KafkaProperties kafkaProperties,
                                                                      JsonMapper jsonMapper) {
        return new DefaultKafkaProducerFactory<>(
                kafkaProperties.buildProducerProperties(),
                new StringSerializer(),
                new JacksonJsonSerializer<>(jsonMapper).noTypeInfo());
    }

    @Bean
    public KafkaTemplate<String, Object> priceStreamKafkaTemplate(ProducerFactory<String, Object> priceStreamProducerFactory) {
        return new KafkaTemplate<>(priceStreamProducerFactory);
    }

    /**
     * The redis projection reads the join result in batches: a busy feed writes the same symbol
     * pair many times per poll, and collapsing a poll into one redis round trip is much cheaper
     * than a save per record.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> fullConfigListenerContainerFactory(
            KafkaProperties kafkaProperties, PriceStreamProperties properties) {
        Map<String, Object> consumerProperties = kafkaProperties.buildConsumerProperties();
        consumerProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, properties.getConsumer().getMaxBatchSize());
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(
                consumerProperties, new StringDeserializer(), new StringDeserializer()));
        factory.setBatchListener(true);
        factory.setConcurrency(properties.getConsumer().getConcurrency());
        return factory;
    }

    /**
     * A failed stream thread is replaced instead of taking the whole client down, so a transient
     * broker problem does not leave the state store permanently unqueryable.
     */
    @Bean
    public StreamsBuilderFactoryBeanConfigurer priceStreamFactoryBeanConfigurer() {
        return factoryBean -> {
            factoryBean.setStreamsUncaughtExceptionHandler(exception -> {
                log.error("Kafka streams thread failed, replacing it", exception);
                return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
            });
            factoryBean.setStateListener((newState, oldState) ->
                    log.info("Kafka streams state {} -> {}", oldState, newState));
        };
    }
}
