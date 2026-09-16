package gc.garcol.pricestreaming.domainlogic;

import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;
import gc.garcol.pricestreaming.config.ConditionalOnPriceEngine;
import gc.garcol.pricestreaming.config.PriceEngine;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnPriceEngine(PriceEngine.LMAX)
public class DisruptorConfig {

    @Bean(destroyMethod = "")
    public Disruptor<DisruptorEvent> priceDisruptor(DisruptorProperties properties,
                                                    Disruptor1EventDomainHandler domainHandler,
                                                    Disruptor2EventCachingHandler eventCachingHandler,
                                                    Disruptor3EventPublishingHandler publishingHandler,
                                                    Disruptor4CleanupHandler cleanupHandler,
                                                    DisruptorExceptionHandler exceptionHandler) {
        Disruptor<DisruptorEvent> disruptor = new Disruptor<>(
                new DisruptorEventFactory(),
                1 << properties.getRingBufferPowSize(),
                DaemonThreadFactory.INSTANCE,
                ProducerType.MULTI,
                properties.getWaitStrategy().create(properties.getWaitTimeout()));
        disruptor.setDefaultExceptionHandler(exceptionHandler);
        disruptor.handleEventsWith(domainHandler)
                .then(eventCachingHandler)
                .then(publishingHandler)
                .then(cleanupHandler);
        return disruptor;
    }
}
