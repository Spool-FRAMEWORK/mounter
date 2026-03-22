package software.spool.mounter.internal.strategy;

import software.spool.core.control.Handler;
import software.spool.core.model.PartitionKey;
import software.spool.core.utils.ErrorRouter;

public abstract class BaseMountStrategy {
    protected final Handler<PartitionKey> handler;
    private final ErrorRouter errorRouter;

    protected BaseMountStrategy(Handler<PartitionKey> handler, ErrorRouter errorRouter) {
        this.handler = handler;
        this.errorRouter = errorRouter;
    }

    public void mountSafely(PartitionKey partitionKey) {
        try {
            handler.handle(partitionKey);
        } catch (Exception e) {
            errorRouter.dispatch(e);
        }
    }
}
