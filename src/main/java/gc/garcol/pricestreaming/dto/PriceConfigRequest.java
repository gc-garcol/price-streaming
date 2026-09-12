package gc.garcol.pricestreaming.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;
import jakarta.validation.constraints.Size;

import java.math.BigDecimal;

@Schema(name = "PriceConfigRequest")
public record PriceConfigRequest(

        @NotBlank
        @Size(max = 32)
        @Schema(example = "BTC-USDT")
        String symbolPair,

        @NotNull
        @PositiveOrZero
        @Digits(integer = 3, fraction = 6)
        @Schema(example = "0.5")
        BigDecimal deltaPercent
) {
}
