package software.spool.mounter.api.port;

import software.spool.core.model.vo.PartitionKey;
import software.spool.mounter.api.model.AggregateMetadata;

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

    Set<PartitionKey> consumedSources(String dataMart);

    /** Adds the partitions to the ones the data mart has consumed. */
    void markConsumed(String dataMart, Set<PartitionKey> sources);
}
