package gc.garcol.pricestreaming.stream;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;

/**
 * Result of {@code PriceConfig KTable INNER JOIN MarketPrice KTable}: a symbol pair only shows up
 * once it has both an active config and a price.
 */
public record FullConfig(
        String symbolPair,
        Long configId,
        BigDecimal price,
        BigDecimal deltaPercent,
        BigDecimal lowPrice,
        BigDecimal highPrice,
        long priceAt,
        long configEventAt) {

    public static final int PRICE_SCALE = 8;
    public static final int DELTA_PERCENT_SCALE = 6;
    public static final RoundingMode ROUNDING_MODE = RoundingMode.HALF_UP;

    private static final BigDecimal ONE_HUNDRED = BigDecimal.valueOf(100);

    public static FullConfig join(PriceConfigEvent config, MarketPrice marketPrice) {
        BigDecimal price = scaled(marketPrice.price(), PRICE_SCALE);
        BigDecimal deltaPercent = scaled(config.deltaPercent(), DELTA_PERCENT_SCALE);
        BigDecimal delta = price == null || deltaPercent == null
                ? null
                : price.multiply(deltaPercent).divide(ONE_HUNDRED, MathContext.DECIMAL64);
        return new FullConfig(
                config.symbolPair(),
                config.id(),
                price,
                deltaPercent,
                delta == null ? null : scaled(price.subtract(delta), PRICE_SCALE),
                delta == null ? null : scaled(price.add(delta), PRICE_SCALE),
                marketPrice.priceAt(),
                config.configEventAt());
    }

    private static BigDecimal scaled(BigDecimal value, int scale) {
        return value == null ? null : value.setScale(scale, ROUNDING_MODE);
    }
}
