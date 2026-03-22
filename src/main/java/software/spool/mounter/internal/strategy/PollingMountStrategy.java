package software.spool.mounter.internal.strategy;

import software.spool.core.control.Handler;
import software.spool.core.model.PartitionKey;
import software.spool.core.utils.CancellationToken;
import software.spool.core.utils.ErrorRouter;
import software.spool.mounter.api.strategy.MountStrategy;
import software.spool.mounter.api.utils.PollingPolicy;
import software.spool.mounter.internal.scheduler.PollingScheduler;

public class PollingMountStrategy extends BaseMountStrategy implements MountStrategy {
    private final PollingScheduler scheduler;
    private final PartitionKey partitionKey;
    private final PollingPolicy policy;

    public PollingMountStrategy(PartitionKey partitionKey, Handler<PartitionKey> handler, ErrorRouter errorRouter,
                                PollingScheduler scheduler, PollingPolicy policy) {
        super(handler, errorRouter);
        this.scheduler = scheduler;
        this.partitionKey = partitionKey;
        this.policy = policy;
    }

    @Override
    public void execute(CancellationToken token) {
        scheduler.schedule(
                () -> mountSafely(partitionKey),
                policy,
                token
        );
    }
}
