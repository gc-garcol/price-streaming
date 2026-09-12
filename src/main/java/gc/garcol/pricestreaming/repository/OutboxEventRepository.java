package gc.garcol.pricestreaming.repository;

import gc.garcol.pricestreaming.entity.OutboxEvent;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, Long> {
}
