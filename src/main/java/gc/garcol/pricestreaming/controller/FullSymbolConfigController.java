package gc.garcol.pricestreaming.controller;

import gc.garcol.pricestreaming.config.ConditionalOnPriceEngine;
import gc.garcol.pricestreaming.config.PriceEngine;
import gc.garcol.pricestreaming.dto.FullSymbolConfig;
import gc.garcol.pricestreaming.dto.PageResponse;
import gc.garcol.pricestreaming.service.FullSymbolConfigService;
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

@RestController
@RequestMapping("/api/full-symbol-configs")
@RequiredArgsConstructor
@Tag(name = "Full symbol configs", description = "Cached price state served from redis")
@ConditionalOnPriceEngine(PriceEngine.LMAX)
public class FullSymbolConfigController {

    private final FullSymbolConfigService service;

    @GetMapping
    @Operation(summary = "Page through the cached full symbol configs in redis")
    public PageResponse<FullSymbolConfig> findAll(
            @ParameterObject @PageableDefault(size = 20, sort = "symbolPair", direction = Sort.Direction.ASC) Pageable pageable) {
        return service.findAll(pageable);
    }
}
