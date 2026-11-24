package software.spool.mounter.api.adapter.health;

import software.spool.core.model.vo.PartitionKey;
import software.spool.core.port.health.PieceMonitor;
import software.spool.mounter.api.model.AggregateMetadata;
import software.spool.mounter.api.model.ConsumedPartition;
import software.spool.mounter.api.port.AggregateMetadataRepository;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Keeps the metadata of incremental aggregates through another repository and tells its {@link PieceMonitor} how
 * each call went.
 */
public final class MonitoredAggregateMetadataRepository implements AggregateMetadataRepository {
    private final AggregateMetadataRepository delegate;
    private final PieceMonitor monitor;

    public MonitoredAggregateMetadataRepository(AggregateMetadataRepository delegate, PieceMonitor monitor) {
        this.delegate = delegate;
        this.monitor = monitor;
    }

    @Override
    public Optional<AggregateMetadata> find(String dataMart, PartitionKey aggregate) {
        return monitor.call(() -> delegate.find(dataMart, aggregate));
    }

    @Override
    public List<AggregateMetadata> findAll(String dataMart) {
        return monitor.call(() -> delegate.findAll(dataMart));
    }

    @Override
    public void save(AggregateMetadata metadata) {
        monitor.run(() -> delegate.save(metadata));
    }

    @Override
    public Set<ConsumedPartition> consumedSources(String dataMart) {
        return monitor.call(() -> delegate.consumedSources(dataMart));
    }

    @Override
    public void markConsumed(String dataMart, Set<ConsumedPartition> sources) {
        monitor.run(() -> delegate.markConsumed(dataMart, sources));
    }
}
