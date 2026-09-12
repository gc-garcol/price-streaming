package gc.garcol.pricestreaming.domainlogic;

import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties({DisruptorProperties.class, DisruptorHandlerProperties.class, PriceStateProperties.class})
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
