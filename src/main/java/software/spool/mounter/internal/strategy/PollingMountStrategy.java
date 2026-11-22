package software.spool.mounter.internal.strategy;

import software.spool.core.model.schedule.Schedule;
import software.spool.core.port.bus.Handler;
import software.spool.core.utils.polling.CancellationToken;
import software.spool.core.utils.polling.PollingScheduler;
import software.spool.core.utils.routing.ErrorRouter;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.strategy.MountStrategy;

public class PollingMountStrategy extends BaseMountStrategy implements MountStrategy {
    private final PollingScheduler scheduler;
    private final MountTarget target;
    private final Schedule schedule;

    public PollingMountStrategy(MountTarget target, Handler<MountTarget> handler, ErrorRouter errorRouter, PollingScheduler scheduler, Schedule schedule) {
        super(handler, errorRouter);
        this.scheduler = scheduler;
        this.target = target;
        this.schedule = schedule;
    }

    @Override
    public void execute(CancellationToken token) {
        scheduler.schedule(
                () -> mountSafely(target),
                schedule,
                token
        );
    }
}
