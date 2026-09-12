package gc.garcol.pricestreaming.service;

import gc.garcol.pricestreaming.dto.PriceConfigRequest;
import gc.garcol.pricestreaming.dto.PriceConfigDto;
import gc.garcol.pricestreaming.entity.PriceConfig;
import gc.garcol.pricestreaming.exception.AppException;
import gc.garcol.pricestreaming.exception.ErrorCode;
import gc.garcol.pricestreaming.constant.AggregateType;
import gc.garcol.pricestreaming.constant.PriceConfigEventType;
import gc.garcol.pricestreaming.repository.PriceConfigRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class PriceConfigService {

    private final PriceConfigRepository repository;
    private final OutboxEventService outboxEventService;

    public List<PriceConfigDto> findAll() {
        return repository.findAll().stream()
                .map(PriceConfigDto::from)
                .toList();
    }

    public PriceConfigDto findById(Long id) {
        return repository.findById(id)
                .map(PriceConfigDto::from)
                .orElseThrow(() -> new AppException(ErrorCode.PRICE_CONFIG_NOT_FOUND, id));
    }

    @Transactional
    public PriceConfigDto create(PriceConfigRequest request) {
        if (repository.existsBySymbolPair(request.symbolPair())) {
            throw new AppException(ErrorCode.DUPLICATE_SYMBOL_PAIR, request.symbolPair());
        }
        PriceConfig saved = repository.save(new PriceConfig(request.symbolPair(), request.deltaPercent()));
        PriceConfigDto response = PriceConfigDto.from(saved);
        publish(PriceConfigEventType.CREATED, response);
        return response;
    }

    @Transactional
    public PriceConfigDto update(Long id, PriceConfigRequest request) {
        PriceConfig entity = repository.findById(id)
                .orElseThrow(() -> new AppException(ErrorCode.PRICE_CONFIG_NOT_FOUND, id));

        repository.findBySymbolPair(request.symbolPair())
                .filter(other -> !other.getId().equals(id))
                .ifPresent(other -> {
                    throw new AppException(ErrorCode.DUPLICATE_SYMBOL_PAIR, request.symbolPair());
                });

        entity.setSymbolPair(request.symbolPair());
        entity.setDeltaPercent(request.deltaPercent());
        PriceConfigDto response = PriceConfigDto.from(repository.saveAndFlush(entity));
        publish(PriceConfigEventType.UPDATED, response);
        return response;
    }

    @Transactional
    public void delete(Long id) {
        PriceConfig entity = repository.findById(id)
                .orElseThrow(() -> new AppException(ErrorCode.PRICE_CONFIG_NOT_FOUND, id));
        PriceConfigDto response = PriceConfigDto.from(entity);
        repository.delete(entity);
        publish(PriceConfigEventType.DELETED, response);
    }

    private void publish(String eventType, PriceConfigDto response) {
        outboxEventService.publish(
                AggregateType.PRICE_CONFIG,
                String.valueOf(response.getId()),
                eventType,
                response);
    }
}
