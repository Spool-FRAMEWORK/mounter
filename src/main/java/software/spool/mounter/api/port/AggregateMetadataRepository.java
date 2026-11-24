package software.spool.mounter.api.port;

import software.spool.core.model.vo.PartitionKey;
import software.spool.mounter.api.model.AggregateMetadata;
import software.spool.mounter.api.model.ConsumedPartition;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Keeps the metadata of the aggregates of each data mart, and the partitions of silver each data mart has consumed
 * (ADR-027). Data marts are named with their layer ({@code gold/payments}).
 */
public interface AggregateMetadataRepository {
    Optional<AggregateMetadata> find(String dataMart, PartitionKey aggregate);

    List<AggregateMetadata> findAll(String dataMart);

    /** Keeps the metadata in place of the one of the same aggregate, if there is one. */
    void save(AggregateMetadata metadata);

    /** The partitions of silver the data mart has consumed, with how many files each had when it was read (ADR-037). */
    Set<ConsumedPartition> consumedSources(String dataMart);

    /** Keeps the partitions as consumed by the data mart, each in place of what was kept for the same partition. */
    void markConsumed(String dataMart, Set<ConsumedPartition> sources);
}
