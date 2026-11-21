package software.spool.mounter.internal.control.steps;

import software.spool.core.utils.routing.ErrorRouter;
import software.spool.mounter.api.exception.MissingPartitionAttributeException;
import software.spool.mounter.api.port.PartitionedRecord;

public class RoutingMissingPartitionAttributeHandler implements MissingPartitionAttributeHandler {
    private final ErrorRouter errorRouter;

    public RoutingMissingPartitionAttributeHandler(ErrorRouter errorRouter) {
        this.errorRouter = errorRouter;
    }

    @Override
    public void handle(PartitionedRecord<?> result, MissingPartitionAttributeException exception) {
        errorRouter.dispatch(exception);
    }
}
