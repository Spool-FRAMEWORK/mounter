package software.spool.mounter.api.adapter.health;

import software.spool.core.port.health.PieceMonitor;
import software.spool.mounter.api.model.GenericRecord;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedReader;
import software.spool.mounter.api.port.PartitionedRecord;

import java.util.stream.Stream;

/**
 * Reads partitions through another reader and tells its {@link PieceMonitor} how each record was read.
 */
public final class MonitoredPartitionedReader implements PartitionedReader {
    private final PartitionedReader delegate;
    private final PieceMonitor monitor;

    public MonitoredPartitionedReader(PartitionedReader delegate, PieceMonitor monitor) {
        this.delegate = delegate;
        this.monitor = monitor;
    }

    @Override
    public Stream<PartitionedRecord<GenericRecord>> read(MountTarget mountTarget) {
        return monitor.stream(() -> delegate.read(mountTarget));
    }
}
