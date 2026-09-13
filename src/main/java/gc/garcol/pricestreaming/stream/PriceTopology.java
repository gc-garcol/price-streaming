package gc.garcol.pricestreaming.stream;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import tools.jackson.databind.ObjectMapper;

/**
 * <pre>
 *           PriceConfig snapshot ──┐
 *                                  ├── latest wins ──▶ KTable&lt;Symbol, PriceConfig&gt;
 *           PriceConfig CDC ───────┘                            │
 *                                                               │ INNER JOIN
 *                                                               ▼
 *           MarketPrice ──────────▶ KTable&lt;Symbol, Price&gt; ──▶ KTable&lt;Symbol, FullConfig&gt;
 *                                                               │
 *                                                    ┌──────────┴──────────┐
 *                                                    ▼                     ▼
 *                                            full-config.events      full-config-store
 *                                                                  (REST paging queries it)
 * </pre>
 *
 * The join is inner, so a symbol pair is only published once it has both an active config and a
 * price, and it disappears again when the config is deleted.
 */
@Slf4j
@Configuration
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "price-stream", name = "enabled", havingValue = "true", matchIfMissing = true)
public class PriceTopology {

    private final PriceStreamProperties properties;
    private final ObjectMapper objectMapper;
    private final Serde<PriceConfigEvent> priceConfigEventSerde;
    private final Serde<MarketPrice> marketPriceSerde;
    private final Serde<FullConfig> fullConfigSerde;

    @Bean
    public KStream<String, FullConfig> fullConfigStream(StreamsBuilder builder) {
        KTable<String, PriceConfigEvent> priceConfigs = priceConfigTable(builder);
        KTable<String, MarketPrice> marketPrices = marketPriceTable(builder);

        KTable<String, FullConfig> fullConfigs = priceConfigs.join(
                marketPrices,
                FullConfig::join,
                Named.as("full-config-join"),
                store(properties.getStore().getFullConfig(), fullConfigSerde));

        KStream<String, FullConfig> fullConfigStream = fullConfigs.toStream(Named.as("full-config-stream"));
        fullConfigStream.to(
                properties.getTopic().getFullConfig(),
                Produced.with(Serdes.String(), fullConfigSerde).withName("full-config-sink"));
        return fullConfigStream;
    }

    /**
     * The outbox topic is keyed by aggregate id and the snapshot by symbol pair, so the change data
     * capture side is re-keyed before the two are folded together. Startup order between the two
     * producers does not matter: the reducer keeps the revision with the highest
     * {@code configEventAt}, and the filter turns a deleted config into a tombstone so the join
     * drops the symbol pair.
     */
    private KTable<String, PriceConfigEvent> priceConfigTable(StreamsBuilder builder) {
        KStream<String, PriceConfigEvent> changeDataCapture = builder
                .stream(properties.getTopic().getPriceConfigEvents(),
                        Consumed.with(Serdes.String(), Serdes.String()).withName("price-config-cdc-source"))
                .processValues(() -> new PriceConfigCdcProcessor(objectMapper), Named.as("price-config-cdc"))
                .selectKey((aggregateId, config) -> config.symbolPair(), Named.as("price-config-rekey"));

        KStream<String, PriceConfigEvent> snapshot = builder
                .stream(properties.getTopic().getPriceConfigSnapshot(),
                        Consumed.with(Serdes.String(), priceConfigEventSerde).withName("price-config-snapshot-source"));

        return changeDataCapture
                .merge(snapshot, Named.as("price-config-merge"))
                .groupByKey(Grouped.with("price-config-grouped", Serdes.String(), priceConfigEventSerde))
                .reduce(PriceConfigEvent::latest, Named.as("price-config-reduce"),
                        store(properties.getStore().getPriceConfig(), priceConfigEventSerde))
                .filter((symbolPair, config) -> !config.deleted(), Named.as("price-config-active"));
    }

    private KTable<String, MarketPrice> marketPriceTable(StreamsBuilder builder) {
        return builder.table(
                properties.getTopic().getMarketPrice(),
                Consumed.with(Serdes.String(), marketPriceSerde).withName("market-price-source"),
                store(properties.getStore().getMarketPrice(), marketPriceSerde));
    }

    private <V> Materialized<String, V, KeyValueStore<Bytes, byte[]>> store(String name, Serde<V> valueSerde) {
        return Materialized.<String, V, KeyValueStore<Bytes, byte[]>>as(name)
                .withKeySerde(Serdes.String())
                .withValueSerde(valueSerde);
    }
}
