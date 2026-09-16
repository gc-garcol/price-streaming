package gc.garcol.pricestreaming.domainlogic;

import gc.garcol.pricestreaming.config.ConditionalOnPriceEngine;
import gc.garcol.pricestreaming.config.PriceEngine;
import gc.garcol.pricestreaming.dto.FullSymbolConfig;
import gc.garcol.pricestreaming.dto.PriceConfigDto;
import gc.garcol.pricestreaming.dto.SymbolDto;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@ConditionalOnPriceEngine(PriceEngine.LMAX)
public class PriceState {

    private static final BigDecimal ONE_HUNDRED = BigDecimal.valueOf(100);

    private final Map<String, FullSymbolConfig> configsBySymbolPair = new HashMap<>();

    public void restore(List<FullSymbolConfig> configs) {
        configsBySymbolPair.clear();
        configs.stream()
                .filter(config -> config.getSymbolPair() != null)
                .forEach(config -> configsBySymbolPair.put(config.getSymbolPair(), config));
    }

    public List<FullSymbolConfig> snapshot() {
        return configsBySymbolPair.values().stream()
                .filter(config -> config.isPresented() && !config.isDeleted())
                .toList();
    }

    public int size() {
        return configsBySymbolPair.size();
    }

    public FullSymbolConfig applyPriceConfig(PriceConfigDto priceConfig) {
        long configEventAt = priceConfig.getUpdatedAt() == null ? 0L : priceConfig.getUpdatedAt().toEpochMilli();
        FullSymbolConfig config = configsBySymbolPair.get(priceConfig.getSymbolPair());
        if (config != null && configEventAt <= config.getConfigEventAt()) {
            return null;
        }
        if (config == null) {
            config = new FullSymbolConfig();
            config.setSymbolPair(priceConfig.getSymbolPair());
            configsBySymbolPair.put(priceConfig.getSymbolPair(), config);
        }
        BigDecimal previousDeltaPercent = config.getDeltaPercent();
        config.setId(priceConfig.getId());
        config.setConfigEventAt(configEventAt);
        config.setDeltaPercent(priceConfig.getDeltaPercent());
        if (sameValue(previousDeltaPercent, config.getDeltaPercent())) {
            return null;
        }
        recalculateBand(config);
        return config;
    }

    public FullSymbolConfig dropPriceConfig(PriceConfigDto priceConfig) {
        FullSymbolConfig config = configsBySymbolPair.remove(priceConfig.getSymbolPair());
        config.setDeleted(true);
        return config;
    }

    public FullSymbolConfig applySymbolPrice(SymbolDto symbol) {
        if (symbol.getSymbolPair() == null) {
            return null;
        }
        FullSymbolConfig config = configsBySymbolPair.get(symbol.getSymbolPair());
        if (config == null) {
            config = new FullSymbolConfig();
            config.setSymbolPair(symbol.getSymbolPair());
            configsBySymbolPair.put(symbol.getSymbolPair(), config);
        }
        BigDecimal previousPrice = config.getPrice();
        config.setPrice(symbol.getPrice());
        if (sameValue(previousPrice, config.getPrice())) {
            return null;
        }
        recalculateBand(config);
        return config;
    }

    private boolean sameValue(BigDecimal current, BigDecimal candidate) {
        if (current == null || candidate == null) {
            return current == candidate;
        }
        return current.compareTo(candidate) == 0;
    }

    private void recalculateBand(FullSymbolConfig config) {
        BigDecimal price = config.getPrice();
        BigDecimal deltaPercent = config.getDeltaPercent();
        if (price == null || deltaPercent == null) {
            config.setPresented(false);
            config.setLowPrice(null);
            config.setHighPrice(null);
            return;
        }
        config.setPresented(true);
        BigDecimal delta = price.multiply(deltaPercent).divide(ONE_HUNDRED, MathContext.DECIMAL64);
        config.setLowPrice(price.subtract(delta));
        config.setHighPrice(price.add(delta));
    }
}
