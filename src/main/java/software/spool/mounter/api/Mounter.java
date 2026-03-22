package software.spool.mounter.api;

import software.spool.core.utils.CancellationToken;
import software.spool.core.utils.ErrorRouter;
import software.spool.mounter.api.strategy.MountStrategy;

public class Mounter implements AutoCloseable {
    private final MountStrategy strategy;
    private volatile CancellationToken token;
    private final ErrorRouter errorRouter;

    public Mounter(MountStrategy strategy, ErrorRouter errorRouter) {
        this.strategy = strategy;
        this.token = CancellationToken.NONE;
        this.errorRouter = errorRouter;
    }

    public void startMounting() {
        if (token.isActive()) return;
        token = CancellationToken.create();
        try {
            strategy.execute(token);
        } catch (Exception e) {
            errorRouter.dispatch(e);
        }
    }

    public void stopMounting() {
        if (!token.isActive()) return;
        token.cancel();
        token = CancellationToken.NONE;
    }

    @Override
    public void close() throws Exception {
        stopMounting();
    }
}
