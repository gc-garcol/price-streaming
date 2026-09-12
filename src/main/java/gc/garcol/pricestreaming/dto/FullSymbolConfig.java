package gc.garcol.pricestreaming.dto;

import lombok.Data;

import java.math.BigDecimal;
import java.math.RoundingMode;

@Data
public class FullSymbolConfig {

    public static final int PRICE_SCALE = 8;
    public static final int DELTA_PERCENT_SCALE = 6;
    public static final RoundingMode ROUNDING_MODE = RoundingMode.HALF_UP;

    private boolean presented = false;
    private boolean deleted = false;
    private Long id;
    private String symbolPair;
    private BigDecimal price;
    private BigDecimal deltaPercent;
    private BigDecimal lowPrice;
    private BigDecimal highPrice;

    private long configEventAt;

    public void setPrice(BigDecimal price) {
        this.price = scaled(price, PRICE_SCALE);
    }

    public void setDeltaPercent(BigDecimal deltaPercent) {
        this.deltaPercent = scaled(deltaPercent, DELTA_PERCENT_SCALE);
    }

    public void setLowPrice(BigDecimal lowPrice) {
        this.lowPrice = scaled(lowPrice, PRICE_SCALE);
    }

    public void setHighPrice(BigDecimal highPrice) {
        this.highPrice = scaled(highPrice, PRICE_SCALE);
    }

    private static BigDecimal scaled(BigDecimal value, int scale) {
        return value == null ? null : value.setScale(scale, ROUNDING_MODE);
    }
}
