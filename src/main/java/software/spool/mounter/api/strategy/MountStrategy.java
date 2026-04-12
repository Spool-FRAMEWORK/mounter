package software.spool.mounter.api.strategy;

import software.spool.core.utils.polling.CancellationToken;

public interface MountStrategy {
    void execute(CancellationToken token);
}
