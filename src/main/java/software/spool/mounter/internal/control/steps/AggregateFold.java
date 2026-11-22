package software.spool.mounter.internal.control.steps;

import software.spool.core.model.vo.PartitionKey;
import software.spool.mounter.api.model.AggregateMetadata;
import software.spool.mounter.api.model.GenericRecord;
import software.spool.mounter.api.port.IncrementalMountAggregator;
import software.spool.mounter.api.port.PartitionedRecord;

import java.time.Instant;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * The state of one aggregate while a round adds records to it. A record of a partition the aggregate already holds
 * is skipped, so a partition read again after a failed round is not counted twice.
 */
final class AggregateFold<S> {
    private final String dataMart;
    private final PartitionKey aggregate;
    private final IncrementalMountAggregator<S, ?> aggregator;
    private final AggregateStateCodec<S> codec;
    private final Set<PartitionKey> sourcesBefore;
    private final Set<PartitionKey> sources;
    private final Optional<String> publishedState;
    private S state;
    private long records;
    private boolean changed;

    private AggregateFold(String dataMart, PartitionKey aggregate, IncrementalMountAggregator<S, ?> aggregator,
                          AggregateStateCodec<S> codec, Set<PartitionKey> sources, S state, long records,
                          Optional<String> publishedState) {
        this.dataMart = dataMart;
        this.aggregate = aggregate;
        this.aggregator = aggregator;
        this.codec = codec;
        this.sourcesBefore = Set.copyOf(sources);
        this.sources = new HashSet<>(sources);
        this.state = state;
        this.records = records;
        this.publishedState = publishedState;
    }

    /** Goes on from what the metadata keeps, or starts from the initial state if there is none. */
    static <S> AggregateFold<S> continuing(String dataMart, PartitionKey aggregate, Optional<AggregateMetadata> metadata,
                                           IncrementalMountAggregator<S, ?> aggregator, AggregateStateCodec<S> codec) {
        return metadata
                .map(kept -> new AggregateFold<>(dataMart, aggregate, aggregator, codec, kept.sources(),
                        codec.decode(kept.state()), kept.records(), kept.publishedState()))
                .orElseGet(() -> startingOver(dataMart, aggregate, Optional.empty(), aggregator, codec));
    }

    /** Starts from the initial state, keeping only which state has its result in the data mart. */
    static <S> AggregateFold<S> startingOver(String dataMart, PartitionKey aggregate, Optional<AggregateMetadata> metadata,
                                             IncrementalMountAggregator<S, ?> aggregator, AggregateStateCodec<S> codec) {
        return new AggregateFold<>(dataMart, aggregate, aggregator, codec, Set.of(), aggregator.initial(), 0,
                metadata.flatMap(AggregateMetadata::publishedState));
    }

    void add(PartitionedRecord<GenericRecord> record) {
        if (sourcesBefore.contains(record.partitionKey())) return;
        state = aggregator.add(state, record.record());
        sources.add(record.partitionKey());
        records++;
        changed = true;
    }

    boolean hasChanged() {
        return changed;
    }

    AggregateMetadata metadata(Instant updatedAt) {
        return new AggregateMetadata(dataMart, aggregate, sources, records, updatedAt, codec.encode(state), publishedState);
    }
}
