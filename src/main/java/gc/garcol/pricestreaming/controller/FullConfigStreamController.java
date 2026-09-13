package gc.garcol.pricestreaming.controller;

import gc.garcol.pricestreaming.dto.PageResponse;
import gc.garcol.pricestreaming.stream.FullConfig;
import gc.garcol.pricestreaming.stream.FullConfigStreamService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springdoc.core.annotations.ParameterObject;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.web.PageableDefault;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/stream/full-configs")
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "price-stream", name = "enabled", havingValue = "true", matchIfMissing = true)
@Tag(name = "Full configs (kafka streams)", description = "Join result projected onto redis from full-config.events")
public class FullConfigStreamController {

    private final FullConfigStreamService service;

    @GetMapping
    @Operation(summary = "Page through the joined full configs cached in redis")
    public PageResponse<FullConfig> findAll(
            @ParameterObject @PageableDefault(size = 20, sort = "symbolPair", direction = Sort.Direction.ASC) Pageable pageable) {
        return service.findAll(pageable);
    }
}
