package software.spool.mounter.api.adapter.health;

import software.spool.core.port.health.PieceMonitor;
import software.spool.mounter.api.port.DataMartRemover;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedRecord;

/**
 * Removes results of a mount through another remover and tells its {@link PieceMonitor} how each call went.
 */
public final class MonitoredDataMartRemover implements DataMartRemover {
    private final DataMartRemover delegate;
    private final PieceMonitor monitor;

    public MonitoredDataMartRemover(DataMartRemover delegate, PieceMonitor monitor) {
        this.delegate = delegate;
        this.monitor = monitor;
    }

    @Override
    public void remove(MountTarget target, PartitionedRecord<?> result) {
        monitor.run(() -> delegate.remove(target, result));
    }
}
