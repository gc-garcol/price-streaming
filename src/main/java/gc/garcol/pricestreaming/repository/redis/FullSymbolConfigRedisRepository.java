package gc.garcol.pricestreaming.repository.redis;

import gc.garcol.pricestreaming.entity.FullSymbolConfigEntity;
import org.springframework.data.keyvalue.repository.KeyValueRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface FullSymbolConfigRedisRepository extends KeyValueRepository<FullSymbolConfigEntity, String> {
}
