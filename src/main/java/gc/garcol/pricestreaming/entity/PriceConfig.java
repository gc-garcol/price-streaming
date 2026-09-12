package gc.garcol.pricestreaming.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.math.BigDecimal;
import java.time.Instant;

@Entity
@Table(name = "price_configs")
@Getter
@Setter
@NoArgsConstructor
public class PriceConfig {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id", nullable = false)
    private Long id;

    @Column(name = "symbol_pair", nullable = false, unique = true, length = 32)
    private String symbolPair;

    @Column(name = "delta_percent", nullable = false, precision = 9, scale = 6)
    private BigDecimal deltaPercent;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    public PriceConfig(String symbolPair, BigDecimal deltaPercent) {
        this.symbolPair = symbolPair;
        this.deltaPercent = deltaPercent;
    }

    @PrePersist
    @PreUpdate
    void touch() {
        this.updatedAt = Instant.now();
    }
}
