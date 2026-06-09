package software.spool.mounter.api.fixture;

import software.spool.core.utils.polling.CancellationToken;
import software.spool.mounter.api.strategy.MountStrategy;

public class CapturingMountStrategy implements MountStrategy {
    private CancellationToken lastToken;
    private int executeCount;

    @Override
    public void execute(CancellationToken token) {
        this.lastToken = token;
        this.executeCount++;
    }

    public CancellationToken lastToken() {
        return lastToken;
    }

    public boolean wasExecuted() {
        return lastToken != null;
    }

    public int executeCount() {
        return executeCount;
    }
}
