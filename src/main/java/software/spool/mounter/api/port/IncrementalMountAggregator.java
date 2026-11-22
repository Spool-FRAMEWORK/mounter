package software.spool.mounter.api.port;

import software.spool.core.model.vo.PartitionKey;
import software.spool.mounter.api.model.GenericRecord;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Combines records as a fold, so a mount can keep the state of each aggregate and add new records to it in a later
 * round instead of reading everything again (ADR-027).
 *
 * <p>{@link #aggregate} is the same fold over every record given, so adding records round by round gives the same
 * result as aggregating them all at once. The state is kept as JSON, so {@link #stateType()} must be a type Jackson
 * can write and read back, such as a record.</p>
 *
 * @param <S> the state of one aggregate
 * @param <O> the result of one aggregate
 */
public interface IncrementalMountAggregator<S, O> extends MountAggregator<O> {
    /** The aggregate a record belongs to, which is also the partition its result comes with. */
    PartitionKey groupOf(PartitionedRecord<GenericRecord> record);

    S initial();

    S add(S state, GenericRecord record);

    O result(S state);

    Class<S> stateType();

    @Override
    default Stream<PartitionedRecord<O>> aggregate(Stream<PartitionedRecord<GenericRecord>> records) {
        Map<PartitionKey, S> states = new LinkedHashMap<>();
        records.forEach(record -> {
            PartitionKey group = groupOf(record);
            states.put(group, add(states.containsKey(group) ? states.get(group) : initial(), record.record()));
        });
        return states.entrySet().stream().map(state -> new PartitionedRecord<>(state.getKey(), result(state.getValue())));
    }
}
