package software.spool.mounter.internal.control;

import software.spool.core.exception.SpoolException;
import software.spool.core.port.bus.EventPublisher;
import software.spool.core.port.bus.Handler;
import software.spool.mounter.api.model.AggregatedRecord;
import software.spool.mounter.api.model.GenericRecord;
import software.spool.mounter.api.port.*;

import java.util.stream.Stream;

public class AtomicMountHandler<O> implements Handler<MountTarget> {
    private final PartitionedReader reader;
    private final MountAggregator<O> aggregator;
    private final DataMartWriter writer;
    private final EventPublisher publisher;
    private final PartitionWindowPolicy windowPolicy;
    private final MountCheckpoint checkpoint;
    private final PartitionKeyExtractor<O> keyExtractor;

    public AtomicMountHandler(PartitionedReader reader,
                              MountAggregator<O> aggregator,
                              DataMartWriter writer,
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
        writeResult(target, aggregator.aggregate(resolveStream(target)));
        markAsMounted(target);
    }

    private Stream<PartitionedRecord<GenericRecord>> resolveStream(MountTarget target) {
        if (reader instanceof StreamingPartitionedReader sr) {
            return sr.stream(target);
        }
        return reader.read(target).stream();
    }

    private boolean shouldSkip(MountTarget target) {
        return !windowPolicy.isClosed(target.sourceKey()) || checkpoint.isMounted(target);
    }

    private void writeResult(MountTarget target, Stream<AggregatedRecord<O>> aggregated) {
        Stream<PartitionedRecord<?>> partitionedStream = aggregated.map(agg ->
                new PartitionedRecord<>(
                        keyExtractor.extract(agg.source(), agg.output(), target),
                        agg.output()
                )
        );
        writer.write(target, partitionedStream);
    }

    private void markAsMounted(MountTarget target) {
        checkpoint.markMounted(target);
    }
}