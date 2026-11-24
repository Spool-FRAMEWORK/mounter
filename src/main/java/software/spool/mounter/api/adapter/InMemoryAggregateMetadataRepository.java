package software.spool.mounter.api.adapter;

import software.spool.core.model.vo.PartitionKey;
import software.spool.mounter.api.model.AggregateMetadata;
import software.spool.mounter.api.model.ConsumedPartition;
import software.spool.mounter.api.port.AggregateMetadataRepository;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Keeps the metadata of the aggregates in memory, for tests and for nodes that do not need it after a restart. */
public class InMemoryAggregateMetadataRepository implements AggregateMetadataRepository {
    private final Map<String, Map<PartitionKey, AggregateMetadata>> aggregates = new ConcurrentHashMap<>();
    private final Map<String, Map<PartitionKey, ConsumedPartition>> consumed = new ConcurrentHashMap<>();

    @Override
    public Optional<AggregateMetadata> find(String dataMart, PartitionKey aggregate) {
        return Optional.ofNullable(aggregatesOf(dataMart).get(aggregate));
    }

    @Override
    public List<AggregateMetadata> findAll(String dataMart) {
        return List.copyOf(aggregatesOf(dataMart).values());
    }

    @Override
    public void save(AggregateMetadata metadata) {
        aggregatesOf(metadata.dataMart()).put(metadata.aggregate(), metadata);
    }

    @Override
    public Set<ConsumedPartition> consumedSources(String dataMart) {
        return Set.copyOf(consumed.getOrDefault(dataMart, Map.of()).values());
    }

    @Override
    public void markConsumed(String dataMart, Set<ConsumedPartition> sources) {
        Map<PartitionKey, ConsumedPartition> ofDataMart = consumed.computeIfAbsent(dataMart, ignored -> new ConcurrentHashMap<>());
        sources.forEach(source -> ofDataMart.put(source.partition(), source));
    }

    private Map<PartitionKey, AggregateMetadata> aggregatesOf(String dataMart) {
        return aggregates.computeIfAbsent(dataMart, ignored -> new ConcurrentHashMap<>());
    }
}
