package gc.garcol.pricestreaming.scheduler;

import gc.garcol.pricestreaming.config.ConditionalOnPriceEngine;
import gc.garcol.pricestreaming.config.PriceEngine;
import gc.garcol.pricestreaming.domainlogic.DisruptorEventPublisher;
import gc.garcol.pricestreaming.dto.PriceConfigDto;
import gc.garcol.pricestreaming.entity.PriceConfig;
import gc.garcol.pricestreaming.repository.PriceConfigRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Component
@RequiredArgsConstructor
@Slf4j
@ConditionalOnPriceEngine(PriceEngine.LMAX)
public class PriceConfigScheduler {

    private final PriceConfigRepository priceConfigRepository;
    private final DisruptorEventPublisher disruptorEventPublisher;

    @Scheduled(fixedDelayString = "${price-state.price-config.refresh-interval}",
            initialDelayString = "${price-state.price-config.refresh-interval}")
    @Transactional(readOnly = true)
    public void publishPriceConfigs() {
        try {
            List<PriceConfig> priceConfigs = priceConfigRepository.findAll();
            priceConfigs.stream()
                    .map(PriceConfigDto::from)
                    .forEach(disruptorEventPublisher::publishPriceConfig);
            log.info("Published {} price configs from scheduled database refresh", priceConfigs.size());
        } catch (RuntimeException exception) {
            log.error("Scheduled price config fetch failed", exception);
        }
    }
}
