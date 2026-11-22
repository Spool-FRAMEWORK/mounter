package software.spool.mounter.api.builder;

import software.spool.core.model.schedule.Schedule;
import software.spool.core.port.bus.EventPublisher;
import software.spool.core.port.decorator.SafeEventPublisher;
import software.spool.core.utils.polling.PollingScheduler;
import software.spool.core.utils.polling.ThreadedPollingScheduler;

public class SchedulingFacet<B> extends MounterFacet<B> {
    Schedule schedule;
    PollingScheduler scheduler;
    EventPublisher publisher;

    SchedulingFacet(B parent) {
        super(parent);
        this.scheduler = new ThreadedPollingScheduler();
    }

    public SchedulingFacet<B> pollingWith(Schedule schedule) {
        this.schedule = schedule;
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
