package software.spool.mounter.api.adapter.health;

import software.spool.core.port.health.PieceMonitor;
import software.spool.mounter.api.port.DataMartWriter;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedRecord;

import java.util.stream.Stream;

/**
 * Writes the results of a mount through another writer and tells its {@link PieceMonitor} how each write went. A
 * failure while the results are produced, such as a read of the source, is not blamed on the data mart.
 */
public final class MonitoredDataMartWriter implements DataMartWriter {
    private final DataMartWriter delegate;
    private final PieceMonitor monitor;

    public MonitoredDataMartWriter(DataMartWriter delegate, PieceMonitor monitor) {
        this.delegate = delegate;
        this.monitor = monitor;
    }

    @Override
    public void write(MountTarget target, Stream<PartitionedRecord<?>> result) {
        monitor.run(() -> delegate.write(target, monitor.given(result)));
    }

    @Override
    public void beginRound(MountTarget target) {
        monitor.run(() -> delegate.beginRound(target));
    }

    @Override
    public void completeRound(MountTarget target) {
        monitor.run(() -> delegate.completeRound(target));
    }

    @Override
    public void abandonRound(MountTarget target) {
        monitor.run(() -> delegate.abandonRound(target));
    }
}
