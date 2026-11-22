package software.spool.mounter.api.builder;

import software.spool.mounter.api.adapter.DatedPartitionWindowPolicy;
import software.spool.mounter.api.port.*;
import software.spool.mounter.api.port.scaling.PartitionDiscovery;

import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

public class CheckpointFacet<B> extends MounterFacet<B> {
    /** How long after its last date entry ends a partition is closed, when no window policy is set. */
    public static final Duration DEFAULT_CLOSING_MARGIN = Duration.ofMinutes(15);

    PartitionWindowPolicy windowPolicy;
    MountCheckpoint checkpoint;
    private AggregateMetadataRepository aggregateMetadata;
    private PartitionDiscovery partitionDiscovery;

    CheckpointFacet(B parent) { super(parent); }

    public CheckpointFacet<B> windowPolicy(PartitionWindowPolicy windowPolicy) {
        this.windowPolicy = windowPolicy;
        return this;
    }

    public CheckpointFacet<B> checkpoint(MountCheckpoint checkpoint) {
        this.checkpoint = checkpoint;
        return this;
    }

    /**
     * Keeps the state and the sources of each aggregate in the repository, so each round reads only the closed
     * partitions of silver it has not read yet, listed with the discovery (ADR-027). It needs an
     * {@link IncrementalMountAggregator}, an {@code AGGREGATION} target and a remover set with
     * {@code mount().removingWith(...)}. Without a window policy, a partition is closed
     * {@link #DEFAULT_CLOSING_MARGIN} after its last date entry ends.
     */
    public CheckpointFacet<B> aggregatingIncrementally(AggregateMetadataRepository aggregateMetadata,
                                                       PartitionDiscovery partitionDiscovery) {
        this.aggregateMetadata = Objects.requireNonNull(aggregateMetadata, "AggregateMetadataRepository required");
        this.partitionDiscovery = Objects.requireNonNull(partitionDiscovery, "PartitionDiscovery required");
        return this;
    }

    Optional<AggregateMetadataRepository> aggregateMetadata() {
        return Optional.ofNullable(aggregateMetadata);
    }

    PartitionDiscovery partitionDiscovery() {
        return partitionDiscovery;
    }

    PartitionWindowPolicy incrementalWindowPolicy() {
        return windowPolicy != null ? windowPolicy : new DatedPartitionWindowPolicy(Clock.systemUTC(), DEFAULT_CLOSING_MARGIN);
    }
}
