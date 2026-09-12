package gc.garcol.pricestreaming.domainlogic;

import com.lmax.disruptor.ExceptionHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class DisruptorExceptionHandler implements ExceptionHandler<DisruptorEvent> {

    @Override
    public void handleEventException(Throwable throwable, long sequence, DisruptorEvent event) {
        log.error("Disruptor failed to handle event {} at sequence {}",
                event == null ? null : event.getEventType(), sequence, throwable);
    }

    @Override
    public void handleOnStartException(Throwable throwable) {
        log.error("Disruptor event handler failed to start", throwable);
    }

    @Override
    public void handleOnShutdownException(Throwable throwable) {
        log.error("Disruptor event handler failed to shut down", throwable);
    }
}
