package gc.garcol.pricestreaming.domainlogic;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.LiteBlockingWaitStrategy;
import com.lmax.disruptor.LiteTimeoutBlockingWaitStrategy;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.TimeoutBlockingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public enum WaitStrategyType {

    BLOCKING,
    BUSY_SPIN,
    LITE_BLOCKING,
    LITE_TIMEOUT_BLOCKING,
    SLEEPING,
    TIMEOUT_BLOCKING,
    YIELDING;

    public WaitStrategy create(Duration waitTimeout) {
        return switch (this) {
            case BLOCKING -> new BlockingWaitStrategy();
            case BUSY_SPIN -> new BusySpinWaitStrategy();
            case LITE_BLOCKING -> new LiteBlockingWaitStrategy();
            case LITE_TIMEOUT_BLOCKING ->
                    new LiteTimeoutBlockingWaitStrategy(waitTimeout.toNanos(), TimeUnit.NANOSECONDS);
            case SLEEPING -> new SleepingWaitStrategy();
            case TIMEOUT_BLOCKING ->
                    new TimeoutBlockingWaitStrategy(waitTimeout.toNanos(), TimeUnit.NANOSECONDS);
            case YIELDING -> new YieldingWaitStrategy();
        };
    }
}
