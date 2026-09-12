package gc.garcol.pricestreaming.domainlogic;

import com.lmax.disruptor.EventHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class Disruptor4CleanupHandler implements EventHandler<DisruptorEvent> {
    @Override
    public void onEvent(DisruptorEvent event, long sequence, boolean endOfBatch) throws Exception {
        event.reset();
    }
}
