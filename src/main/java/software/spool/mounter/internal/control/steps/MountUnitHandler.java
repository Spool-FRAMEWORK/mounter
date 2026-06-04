package software.spool.mounter.internal.control.steps;

import software.spool.core.exception.SpoolException;
import software.spool.core.port.bus.Handler;
import software.spool.mounter.api.model.AggregatedRecord;
import software.spool.mounter.api.port.*;

import java.util.stream.Stream;

public class MountUnitHandler<O> implements Handler<MountTarget> {
    private final PartitionedReader reader;
    private final MountAggregator<O> aggregator;
    private final DataMartWriter writer;
    private final PartitionWindowPolicy windowPolicy;
    private final MountCheckpoint checkpoint;
    private final PartitionKeyExtractor<O> keyExtractor;

    public MountUnitHandler(PartitionedReader reader, MountAggregator<O> aggregator, DataMartWriter writer,
                            PartitionWindowPolicy windowPolicy, MountCheckpoint checkpoint, PartitionKeyExtractor<O> keyExtractor) {
        this.reader = reader;
        this.aggregator = aggregator;
        this.writer = writer;
        this.windowPolicy = windowPolicy;
        this.checkpoint = checkpoint;
        this.keyExtractor = keyExtractor;
    }

    @Override
    public void handle(MountTarget target) throws SpoolException {
        if (shouldSkip(target)) return;
        writeResult(target, aggregator.aggregate(reader.read(target)));
        checkpoint.markMounted(target);
    }

    private boolean shouldSkip(MountTarget target) {
        return !windowPolicy.isClosed(target.sourceKey()) || checkpoint.isMounted(target);
    }

    private void writeResult(MountTarget target, Stream<AggregatedRecord<O>> aggregated) {
        writer.write(target, aggregated.map(agg ->
                new PartitionedRecord<>(keyExtractor.extract(agg.source(), agg.output(), target), agg.output())
        ));
    }
}
