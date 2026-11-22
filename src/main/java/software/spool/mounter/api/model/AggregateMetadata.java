package software.spool.mounter.api.model;

import software.spool.core.model.vo.PartitionKey;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * What a mount keeps about one aggregate of a data mart, apart from its result (ADR-027).
 *
 * @param dataMart       the data mart the aggregate belongs to, named with its layer ({@code gold/payments})
 * @param aggregate      the partition of the aggregate, as its aggregator groups it
 * @param sources        the partitions of silver its records come from
 * @param records        how many records it has added
 * @param updatedAt      when its state last changed
 * @param state          the state of its aggregator, in JSON
 * @param publishedState the state whose result is in the data mart, none while nothing has been published
 */
public record AggregateMetadata(String dataMart, PartitionKey aggregate, Set<PartitionKey> sources, long records,
                                Instant updatedAt, String state, Optional<String> publishedState) {
    public AggregateMetadata {
        Objects.requireNonNull(dataMart, "Aggregate metadata without data mart");
        Objects.requireNonNull(aggregate, "Aggregate metadata without aggregate");
        Objects.requireNonNull(updatedAt, "Aggregate metadata without update time");
        Objects.requireNonNull(state, "Aggregate metadata without state");
        Objects.requireNonNull(publishedState, "Aggregate metadata without published state");
        sources = Set.copyOf(sources);
    }

    /** Whether the result in the data mart is not the result of the current state. */
    public boolean isPublicationPending() {
        return !publishedState.equals(Optional.of(state));
    }

    /** The same metadata once the result of its current state is in the data mart. */
    public AggregateMetadata published() {
        return new AggregateMetadata(dataMart, aggregate, sources, records, updatedAt, state, Optional.of(state));
    }
}
