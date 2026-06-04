package software.spool.mounter.internal.decorator;

import software.spool.core.exception.PartitionDispatchException;
import software.spool.core.exception.SpoolException;
import software.spool.core.port.bus.Handler;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.scaling.PartitionDispatcher;

import java.util.List;

public class SafePartitionDispatcher implements PartitionDispatcher {
    private final PartitionDispatcher delegate;

    private SafePartitionDispatcher(PartitionDispatcher delegate) {
        this.delegate = delegate;
    }

    public static SafePartitionDispatcher of(PartitionDispatcher delegate) {
        return new SafePartitionDispatcher(delegate);
    }

    @Override
    public void dispatch(List<MountTarget> units, Handler<MountTarget> worker) {
        try {
            delegate.dispatch(units, worker);
        } catch (SpoolException e) {
            throw e;
        } catch (Exception e) {
            throw new PartitionDispatchException(e.getMessage(), e);
        }
    }
}
