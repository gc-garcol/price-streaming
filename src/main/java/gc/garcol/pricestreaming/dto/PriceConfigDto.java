package gc.garcol.pricestreaming.dto;

import gc.garcol.pricestreaming.entity.PriceConfig;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;

@Schema(name = "PriceConfigResponse")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PriceConfigDto {

    private Long id;

    private String symbolPair;

    private BigDecimal deltaPercent;

    private Instant updatedAt;

    public static PriceConfigDto from(PriceConfig entity) {
        return new PriceConfigDto(
                entity.getId(),
                entity.getSymbolPair(),
                entity.getDeltaPercent(),
                entity.getUpdatedAt()
        );
    }
}
