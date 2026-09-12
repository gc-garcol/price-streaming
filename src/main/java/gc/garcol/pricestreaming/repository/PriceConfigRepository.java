package gc.garcol.pricestreaming.repository;
import gc.garcol.pricestreaming.entity.PriceConfig;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface PriceConfigRepository extends JpaRepository<PriceConfig, Long> {

    Optional<PriceConfig> findBySymbolPair(String symbolPair);

    boolean existsBySymbolPair(String symbolPair);
}
