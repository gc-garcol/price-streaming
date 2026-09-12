package gc.garcol.pricestreaming.domainlogic;

import gc.garcol.pricestreaming.dto.FullSymbolConfig;
import gc.garcol.pricestreaming.dto.PriceConfigDto;
import gc.garcol.pricestreaming.dto.SymbolDto;
import gc.garcol.pricestreaming.entity.PriceConfig;
import gc.garcol.pricestreaming.repository.PriceConfigRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Component
@RequiredArgsConstructor
@Slf4j
public class PriceStateLoader {

    private final PriceConfigRepository priceConfigRepository;
    private final PriceStateCache priceStateCache;
    private final SymbolPriceFeed symbolPriceFeed;
    private final PriceState priceState;

    @Transactional(readOnly = true)
    public void load() {
        List<FullSymbolConfig> cached = priceStateCache.read();
        if (cached != null) {
            priceState.restore(cached);
            loadSymbolPrices();
            log.info("Loaded {} full symbol configs from redis cache", cached.size());
            return;
        }
        log.info("Redis cache miss, rebuilding price state from source of truth");
        loadPriceConfigs();
        loadSymbolPrices();
        priceStateCache.write(priceState.snapshot());
    }

    private void loadPriceConfigs() {
        List<PriceConfig> priceConfigs = priceConfigRepository.findAll();
        priceConfigs.stream()
                .map(PriceConfigDto::from)
                .forEach(priceState::applyPriceConfig);
        log.info("Loaded {} price configs from database", priceConfigs.size());
    }

    private void loadSymbolPrices() {
        List<SymbolDto> symbols = symbolPriceFeed.fetchAll(false);
        long applied = symbols.stream()
                .filter(symbol -> priceState.applySymbolPrice(symbol) != null)
                .count();
        log.info("Applied {} of {} symbol prices from feed", applied, symbols.size());
    }
}
