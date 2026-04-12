package software.spool.mounter.internal.strategy;

import software.spool.core.model.vo.PartitionKey;
import software.spool.core.port.bus.Handler;
import software.spool.core.utils.polling.CancellationToken;
import software.spool.core.utils.polling.PollingPolicy;
import software.spool.core.utils.polling.PollingScheduler;
import software.spool.core.utils.routing.ErrorRouter;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.strategy.MountStrategy;

public class PollingMountStrategy extends BaseMountStrategy implements MountStrategy {
    private final PollingScheduler scheduler;
    private final MountTarget target;
    private final PollingPolicy policy;

    public PollingMountStrategy(MountTarget target, Handler<MountTarget> handler, ErrorRouter errorRouter,
                                PollingScheduler scheduler, PollingPolicy policy) {
        super(handler, errorRouter);
        this.scheduler = scheduler;
        this.target = target;
        this.policy = policy;
    }

    @Override
    public void execute(CancellationToken token) {
        scheduler.schedule(
                () -> mountSafely(target),
                policy,
                token
        );
    }
}
