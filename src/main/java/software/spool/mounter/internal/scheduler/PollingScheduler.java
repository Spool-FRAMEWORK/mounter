package software.spool.mounter.internal.scheduler;

import software.spool.core.utils.CancellationToken;
import software.spool.mounter.api.utils.PollingPolicy;

public interface PollingScheduler {
    void schedule(Runnable task, PollingPolicy policy, CancellationToken token);
}
