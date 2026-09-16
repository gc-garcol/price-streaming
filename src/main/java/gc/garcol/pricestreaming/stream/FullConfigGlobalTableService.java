package gc.garcol.pricestreaming.stream;

import gc.garcol.pricestreaming.config.ConditionalOnPriceEngine;
import gc.garcol.pricestreaming.config.PriceEngine;
import gc.garcol.pricestreaming.exception.AppException;
import gc.garcol.pricestreaming.exception.ErrorCode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Serves the join result straight out of the {@code GlobalKTable} store instead of redis. The
 * store is fully replicated, so a scan here sees every symbol pair the topology has ever joined,
 * not only the partitions this instance owns — and it needs no network hop at all.
 */
@Slf4j
@Service
@RequiredArgsConstructor
@ConditionalOnPriceEngine(PriceEngine.KAFKA_STREAM)
public class FullConfigGlobalTableService {

    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;
    private final PriceStreamProperties properties;

    /**
     * A full scan of the store, ordered by symbol pair. The iterator holds a RocksDB snapshot, so
     * it is drained into a list and closed rather than handed to the serializer.
     */
    public List<FullConfig> findAll() {
        List<FullConfig> configs = new ArrayList<>();
        try (KeyValueIterator<String, FullConfig> iterator = store().all()) {
            iterator.forEachRemaining(entry -> {
                if (entry.value != null) {
                    configs.add(entry.value);
                }
            });
        }
        configs.sort(Comparator.comparing(FullConfig::symbolPair, Comparator.nullsLast(Comparator.naturalOrder())));
        return configs;
    }

    private ReadOnlyKeyValueStore<String, FullConfig> store() {
        KafkaStreams streams = streamsBuilderFactoryBean.getKafkaStreams();
        // Rebalancing is fine: the global thread bootstraps independently of the stream threads,
        // so the store stays queryable while partitions move.
        if (streams == null || !streams.state().isRunningOrRebalancing()) {
            throw new AppException(ErrorCode.STREAM_STORE_UNAVAILABLE);
        }
        try {
            return streams.store(StoreQueryParameters.fromNameAndType(
                    properties.getStore().getFullConfigGlobal(),
                    QueryableStoreTypes.keyValueStore()));
        } catch (InvalidStateStoreException exception) {
            log.warn("Global full config store is not queryable yet", exception);
            throw new AppException(ErrorCode.STREAM_STORE_UNAVAILABLE);
        }
    }
}
