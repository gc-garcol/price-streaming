package gc.garcol.pricestreaming.service;

import gc.garcol.pricestreaming.entity.OutboxEvent;
import gc.garcol.pricestreaming.exception.AppException;
import gc.garcol.pricestreaming.exception.ErrorCode;
import gc.garcol.pricestreaming.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

@Service
@RequiredArgsConstructor
public class OutboxEventService {

    private final OutboxEventRepository repository;
    private final ObjectMapper objectMapper;

    @Transactional(propagation = Propagation.MANDATORY)
    public void publish(String aggregateType, String aggregateId, String type, Object payload) {
        repository.save(new OutboxEvent(aggregateType, aggregateId, type, serialize(payload)));
    }

    private String serialize(Object payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JacksonException exception) {
            throw new AppException(ErrorCode.INTERNAL_ERROR);
        }
    }
}
