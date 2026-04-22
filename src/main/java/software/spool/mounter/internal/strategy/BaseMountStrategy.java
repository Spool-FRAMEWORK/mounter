package software.spool.mounter.internal.strategy;

import software.spool.core.port.bus.Handler;
import software.spool.core.utils.routing.ErrorRouter;
import software.spool.mounter.api.port.MountTarget;

public abstract class BaseMountStrategy {
    protected final Handler<MountTarget> handler;
    private final ErrorRouter errorRouter;

    protected BaseMountStrategy(Handler<MountTarget> handler, ErrorRouter errorRouter) {
        this.handler = handler;
        this.errorRouter = errorRouter;
    }

    public void mountSafely(MountTarget target) {
        try {
            handler.handle(target);
        } catch (Exception e) {
            errorRouter.dispatch(e);
        }
    }
}
