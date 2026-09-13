package gc.garcol.pricestreaming.stream;

import gc.garcol.pricestreaming.dto.PriceConfigDto;
import gc.garcol.pricestreaming.entity.PriceConfig;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * One price config revision, keyed by symbol pair. Both the startup database snapshot and the
 * debezium outbox stream produce this shape, so the topology can fold them into a single table.
 *
 * @param configEventAt the row {@code updated_at}, used to keep the latest revision when the
 *                      snapshot and the change data capture stream race on the same symbol pair
 * @param deleted       {@code true} for a {@code PriceConfigDeleted} event, which drops the symbol
 *                      pair from the config table and therefore from the join
 */
public record PriceConfigEvent(
        Long id,
        String symbolPair,
        BigDecimal deltaPercent,
        long configEventAt,
        boolean deleted) {

    public static PriceConfigEvent from(PriceConfig entity) {
        return new PriceConfigEvent(
                entity.getId(),
                entity.getSymbolPair(),
                entity.getDeltaPercent(),
                epochMillis(entity.getUpdatedAt()),
                false);
    }

    public static PriceConfigEvent from(PriceConfigDto dto, boolean deleted) {
        return new PriceConfigEvent(
                dto.getId(),
                dto.getSymbolPair(),
                dto.getDeltaPercent(),
                epochMillis(dto.getUpdatedAt()),
                deleted);
    }

    /**
     * Reducer for the config table: the highest {@code configEventAt} wins, ties go to the record
     * that arrived last.
     */
    public static PriceConfigEvent latest(PriceConfigEvent current, PriceConfigEvent incoming) {
        return incoming.configEventAt() >= current.configEventAt() ? incoming : current;
    }

    private static long epochMillis(Instant instant) {
        return instant == null ? 0L : instant.toEpochMilli();
    }
}
