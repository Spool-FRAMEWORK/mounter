package software.spool.mounter.internal.control.steps;

import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.adapter.jackson.RecordSerializerFactory;
import software.spool.core.exception.MountAggregateException;
import software.spool.core.port.serde.PayloadDeserializer;
import software.spool.core.port.serde.RecordSerializer;
import software.spool.mounter.api.port.IncrementalMountAggregator;

import java.nio.charset.StandardCharsets;

/** Writes the state of an incremental aggregator as JSON and reads it back. */
final class AggregateStateCodec<S> {
    private final RecordSerializer<S> serializer = RecordSerializerFactory.record();
    private final PayloadDeserializer<S> deserializer;
    private final String aggregatorName;

    AggregateStateCodec(IncrementalMountAggregator<S, ?> aggregator) {
        this.deserializer = PayloadDeserializerFactory.json().as(aggregator.stateType());
        this.aggregatorName = aggregator.getClass().getName();
    }

    String encode(S state) {
        try {
            return new String(serializer.serialize(state), StandardCharsets.UTF_8);
        } catch (RuntimeException e) {
            throw new MountAggregateException("the state of " + aggregatorName + " cannot be written as JSON: " + e.getMessage(), e);
        }
    }

    S decode(String state) {
        try {
            return deserializer.deserialize(state.getBytes(StandardCharsets.UTF_8));
        } catch (RuntimeException e) {
            throw new MountAggregateException("the state of " + aggregatorName + " cannot be read from JSON: " + e.getMessage(), e);
        }
    }
}
