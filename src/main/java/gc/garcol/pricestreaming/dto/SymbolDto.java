package gc.garcol.pricestreaming.dto;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class SymbolDto {
    private String symbolPair;

    private BigDecimal price;
}
