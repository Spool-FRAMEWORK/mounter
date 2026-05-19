package software.spool.mounter.internal.control;

import software.spool.core.exception.SpoolException;
import software.spool.core.port.bus.EventPublisher;
import software.spool.core.port.bus.Handler;
import software.spool.mounter.api.port.*;

import java.util.stream.Stream;

public class AtomicMountHandler<I, O> implements Handler<MountTarget> {
    private final PartitionedReader<I> reader;
    private final MountAggregator<I, O> aggregator;
    private final DataMartWriter<O> writer;
    private final EventPublisher publisher;
    private final PartitionWindowPolicy windowPolicy;
    private final MountCheckpoint checkpoint;
    private final PartitionKeyExtractor<O> keyExtractor;

    public AtomicMountHandler(PartitionedReader<I> reader,
                              MountAggregator<I, O> aggregator,
                              DataMartWriter<O> writer,
                              EventPublisher publisher,
                              PartitionWindowPolicy windowPolicy,
                              MountCheckpoint checkpoint, PartitionKeyExtractor<O> keyExtractor) {
        this.reader = reader;
        this.aggregator = aggregator;
        this.writer = writer;
        this.publisher = publisher;
        this.windowPolicy = windowPolicy;
        this.checkpoint = checkpoint;
        this.keyExtractor = keyExtractor;
    }

    @Override
    public void handle(MountTarget target) throws SpoolException {
        if (shouldSkip(target)) return;
        writeResult(target, aggregator.aggregate(reader.read(target.sourceKey()).stream()));
        markAsMounted(target);
    }

    private boolean shouldSkip(MountTarget target) {
        return !windowPolicy.isClosed(target.sourceKey()) || checkpoint.isMounted(target);
    }

    private void writeResult(MountTarget target, Stream<O> aggregated) {
        Stream<PartitionedRecord<O>> partitionedStream = aggregated.map(payload ->
                new PartitionedRecord<>(keyExtractor.extract(payload), payload)
        );
        writer.write(target, partitionedStream);
    }

    private void markAsMounted(MountTarget target) {
        checkpoint.markMounted(target);
    }
}