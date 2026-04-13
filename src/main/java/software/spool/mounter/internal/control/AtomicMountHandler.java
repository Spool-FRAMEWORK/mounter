package software.spool.mounter.internal.control;

import software.spool.core.exception.SpoolException;
import software.spool.core.model.event.ItemsMounted;
import software.spool.core.port.bus.EventBusEmitter;
import software.spool.core.port.bus.Handler;
import software.spool.mounter.api.port.*;

import java.util.stream.Stream;

public class AtomicMountHandler<I, O> implements Handler<MountTarget> {
    private final PartitionedReader<I> reader;
    private final MountAggregator<I, O> aggregator;
    private final DataMartWriter<O> writer;
    private final EventBusEmitter emitter;
    private final PartitionWindowPolicy windowPolicy;
    private final MountCheckpoint checkpoint;
    private final PartitionKeyExtractor<O> keyExtractor;

    public AtomicMountHandler(PartitionedReader<I> reader,
                              MountAggregator<I, O> aggregator,
                              DataMartWriter<O> writer,
                              EventBusEmitter emitter,
                              PartitionWindowPolicy windowPolicy,
                              MountCheckpoint checkpoint, PartitionKeyExtractor<O> keyExtractor) {
        this.reader = reader;
        this.aggregator = aggregator;
        this.writer = writer;
        this.emitter = emitter;
        this.windowPolicy = windowPolicy;
        this.checkpoint = checkpoint;
        this.keyExtractor = keyExtractor;
    }

    @Override
    public void handle(MountTarget target) throws SpoolException {
        if (shouldSkip(target)) return;
        writeResult(target, aggregate(target));
        emitEvent(target);
        markAsMounted(target);
    }

    private boolean shouldSkip(MountTarget target) {
        return !windowPolicy.isClosed(target.sourceKey()) || checkpoint.isMounted(target);
    }

    private Stream<O> aggregate(MountTarget target) {
        return aggregator.aggregate(reader.read(target.sourceKey()).stream());
    }

    private void writeResult(MountTarget target, Stream<O> aggregated) {
        Stream<PartitionedRecord<O>> partitionedStream = aggregated.map(payload ->
                new PartitionedRecord<>(keyExtractor.extract(payload), payload)
        );
        writer.write(target, partitionedStream);
    }

    private void emitEvent(MountTarget target) {
        emitter.emit(ItemsMounted.builder()
                .partitionKey(target.sourceKey())
                .build());
    }

    private void markAsMounted(MountTarget target) {
        checkpoint.markMounted(target);
    }
}