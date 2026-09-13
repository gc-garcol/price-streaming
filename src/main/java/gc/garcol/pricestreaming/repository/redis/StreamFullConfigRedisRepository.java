package gc.garcol.pricestreaming.repository.redis;

import gc.garcol.pricestreaming.entity.StreamFullConfigEntity;
import org.springframework.data.keyvalue.repository.KeyValueRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface StreamFullConfigRedisRepository extends KeyValueRepository<StreamFullConfigEntity, String> {
}
