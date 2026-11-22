package software.spool.mounter.api.adapter.health;

import software.spool.core.model.vo.PartitionKey;
import software.spool.core.port.health.PieceMonitor;
import software.spool.mounter.api.port.scaling.PartitionDiscovery;
import software.spool.mounter.api.port.scaling.PartitionInfo;

import java.util.List;

/**
 * Discovers partitions through another discovery and tells its {@link PieceMonitor} how each call went.
 */
public final class MonitoredPartitionDiscovery implements PartitionDiscovery {
    private final PartitionDiscovery delegate;
    private final PieceMonitor monitor;

    public MonitoredPartitionDiscovery(PartitionDiscovery delegate, PieceMonitor monitor) {
        this.delegate = delegate;
        this.monitor = monitor;
    }

    @Override
    public List<PartitionInfo> discover(PartitionKey scope) {
        return monitor.call(() -> delegate.discover(scope));
    }
}
