package software.spool.mounter.api.builder;

import software.spool.core.port.bus.EventPublisher;
import software.spool.core.port.decorator.SafeEventPublisher;
import software.spool.core.utils.polling.PollingPolicy;
import software.spool.core.utils.polling.PollingScheduler;
import software.spool.core.utils.polling.ThreadedPollingScheduler;

public class SchedulingFacet<B> extends MounterFacet<B> {
    PollingPolicy pollingPolicy;
    PollingScheduler scheduler;
    EventPublisher publisher;

    SchedulingFacet(B parent) {
        super(parent);
        this.scheduler = new ThreadedPollingScheduler();
    }

    public SchedulingFacet<B> pollingWith(PollingPolicy pollingPolicy) {
        this.pollingPolicy = pollingPolicy;
        return this;
    }

    public SchedulingFacet<B> scheduledWith(PollingScheduler scheduler) {
        this.scheduler = scheduler;
        return this;
    }

    public SchedulingFacet<B> emittingWith(EventPublisher publisher) {
        this.publisher = SafeEventPublisher.of(publisher);
        return this;
    }
}
