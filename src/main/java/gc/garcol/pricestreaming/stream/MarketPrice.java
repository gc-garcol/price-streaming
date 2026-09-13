package gc.garcol.pricestreaming.stream;

import gc.garcol.pricestreaming.dto.SymbolDto;

import java.math.BigDecimal;

/**
 * One tick of the {@code MARKET_PRICE.events} stream, keyed by symbol pair.
 */
public record MarketPrice(String symbolPair, BigDecimal price, long priceAt) {

    public static MarketPrice from(SymbolDto symbol, long priceAt) {
        return new MarketPrice(symbol.getSymbolPair(), symbol.getPrice(), priceAt);
    }
}
