package gc.garcol.pricestreaming.controller;

import gc.garcol.pricestreaming.config.ConditionalOnPriceEngine;
import gc.garcol.pricestreaming.config.PriceEngine;
import gc.garcol.pricestreaming.dto.PageResponse;
import gc.garcol.pricestreaming.stream.FullConfig;
import gc.garcol.pricestreaming.stream.FullConfigGlobalTableService;
import gc.garcol.pricestreaming.stream.FullConfigStreamService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springdoc.core.annotations.ParameterObject;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.web.PageableDefault;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/stream/full-configs")
@RequiredArgsConstructor
@ConditionalOnPriceEngine(PriceEngine.KAFKA_STREAM)
@Tag(name = "Full configs (kafka streams)", description = "Join result served from the redis projection or the global state store")
public class FullConfigStreamController {

    private final FullConfigStreamService service;
    private final FullConfigGlobalTableService globalTableService;

    @GetMapping
    @Operation(summary = "Page through the joined full configs cached in redis")
    public PageResponse<FullConfig> findAll(
            @ParameterObject @PageableDefault(size = 20, sort = "symbolPair", direction = Sort.Direction.ASC) Pageable pageable) {
        return service.findAll(pageable);
    }

    /**
     * Reads the {@code GlobalKTable} store directly. It is fully replicated, so this instance
     * answers for every symbol pair without touching redis or querying another instance — the
     * whole set is returned rather than paged, since the store holds one row per symbol pair.
     */
    @GetMapping("/global")
    @Operation(summary = "List every symbol pair from the full config global state store")
    public List<FullConfig> findAllFromGlobalTable() {
        return globalTableService.findAll();
    }
}
