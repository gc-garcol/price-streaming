package gc.garcol.pricestreaming.service;

import gc.garcol.pricestreaming.dto.FullSymbolConfig;
import gc.garcol.pricestreaming.dto.PageResponse;
import gc.garcol.pricestreaming.entity.FullSymbolConfigEntity;
import gc.garcol.pricestreaming.repository.redis.FullSymbolConfigRedisRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Objects;

@Service
@RequiredArgsConstructor
public class FullSymbolConfigService {

    private final FullSymbolConfigRedisRepository repository;

    public PageResponse<FullSymbolConfig> findAll(Pageable pageable) {
        List<FullSymbolConfig> sorted = repository.findAll(pageable.getSort()).stream()
                .map(FullSymbolConfigEntity::toFullSymbolConfig)
                .toList();
        int from = (int) Math.min(pageable.getOffset(), sorted.size());
        int to = Math.min(from + pageable.getPageSize(), sorted.size());
        List<FullSymbolConfig> content = sorted.subList(from, to);
        Page<FullSymbolConfig> page = new PageImpl<>(content, pageable, sorted.size());
        return PageResponse.of(page, content);
    }
}
