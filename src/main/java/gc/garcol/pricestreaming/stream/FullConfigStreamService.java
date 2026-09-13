package gc.garcol.pricestreaming.stream;

import gc.garcol.pricestreaming.dto.PageResponse;
import gc.garcol.pricestreaming.entity.StreamFullConfigEntity;
import gc.garcol.pricestreaming.repository.redis.StreamFullConfigRedisRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Serves the join result from the redis projection that {@code FullConfigEventConsumer} maintains,
 * so any instance can answer for every symbol pair instead of only the partitions its own state
 * store holds.
 */
@Service
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "price-stream", name = "enabled", havingValue = "true", matchIfMissing = true)
public class FullConfigStreamService {

    private final StreamFullConfigRedisRepository repository;

    public PageResponse<FullConfig> findAll(Pageable pageable) {
        // An id whose hash already expired still sits in the index set until the keyspace
        // listener catches up, and it reads back as null.
        List<FullConfig> sorted = repository.findAll(pageable.getSort()).stream()
                .filter(entity -> entity != null && entity.getSymbolPair() != null)
                .map(StreamFullConfigEntity::toFullConfig)
                .toList();
        int from = (int) Math.min(pageable.getOffset(), sorted.size());
        int to = Math.min(from + pageable.getPageSize(), sorted.size());
        List<FullConfig> content = sorted.subList(from, to);
        Page<FullConfig> page = new PageImpl<>(content, pageable, sorted.size());
        return PageResponse.of(page, content);
    }
}
