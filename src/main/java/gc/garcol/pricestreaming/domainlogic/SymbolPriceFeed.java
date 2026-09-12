package gc.garcol.pricestreaming.domainlogic;

import gc.garcol.pricestreaming.dto.SymbolDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

@Component
@RequiredArgsConstructor
@Slf4j
public class SymbolPriceFeed {

    private static final int PRICE_SCALE = 8;
    private static final BigDecimal ONE_HUNDRED = BigDecimal.valueOf(100);

    private static final Map<String, String> BASE_PRICES = Map.of(
            "BTC-USDT", "64250.12345678",
            "ETH-USDT", "3120.55",
            "SOL-USDT", "148.56",
            "BNB-USDT", "585.4",
            "XRP-USDT", "0.5321",
            "DOGE-USDT", "0.8480",
            "XAU-USDT", "4358.34",
            "LINK-USDT", "11.476",
            "TRX-USDT", "0.33940"
            );

    private final PriceStateProperties properties;

    public List<SymbolDto> fetchAll(boolean slow) {
        long latencyMillis = slow ? randomLatencyMillis() : 1;
        if (slow) {
            log.info("Fetching symbol prices from upstream feed, simulated latency {}ms", latencyMillis);
        }
        try {
            Thread.sleep(latencyMillis);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while fetching symbol prices");
            return List.of();
        }
        return BASE_PRICES.entrySet().stream()
                .map(entry -> {
                    SymbolDto symbol = new SymbolDto();
                    symbol.setSymbolPair(entry.getKey());
                    symbol.setPrice(randomPrice(new BigDecimal(entry.getValue())));
                    return symbol;
                })
                .toList();
    }

    private long randomLatencyMillis() {
        long minLatency = Math.max(0, properties.getSymbolFeed().getMinLatency().toMillis());
        long maxLatency = properties.getSymbolFeed().getMaxLatency().toMillis();
        if (maxLatency <= minLatency) {
            return minLatency;
        }
        return ThreadLocalRandom.current().nextLong(minLatency, maxLatency + 1);
    }

    private BigDecimal randomPrice(BigDecimal basePrice) {
        BigDecimal jitterPercent = properties.getSymbolFeed().getPriceJitterPercent();
        if (jitterPercent == null || jitterPercent.signum() <= 0) {
            return basePrice.setScale(PRICE_SCALE, RoundingMode.HALF_UP);
        }
        double bound = jitterPercent.doubleValue();
        BigDecimal offsetPercent = BigDecimal.valueOf(ThreadLocalRandom.current().nextDouble(-bound, bound));
        BigDecimal factor = BigDecimal.ONE.add(offsetPercent.divide(ONE_HUNDRED, java.math.MathContext.DECIMAL64));
        return basePrice.multiply(factor).setScale(PRICE_SCALE, RoundingMode.HALF_UP);
    }
}
