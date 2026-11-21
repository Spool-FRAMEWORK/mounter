package software.spool.mounter.internal.control.steps;

import software.spool.core.exception.SpoolException;
import software.spool.core.port.bus.Handler;
import software.spool.mounter.api.exception.MissingPartitionAttributeException;
import software.spool.mounter.api.port.*;

import java.util.stream.Stream;

public class MountUnitHandler<O> implements Handler<MountTarget> {
    private final PartitionedReader reader;
    private final MountAggregator<O> aggregator;
    private final DataMartWriter writer;
    private final PartitionWindowPolicy windowPolicy;
    private final MountCheckpoint checkpoint;
    private final MountPartitioning<? super O> partitioning;
    private final MissingPartitionAttributeHandler missingAttributeHandler;

    public MountUnitHandler(PartitionedReader reader, MountAggregator<O> aggregator, DataMartWriter writer,
                            PartitionWindowPolicy windowPolicy, MountCheckpoint checkpoint,
                            MountPartitioning<? super O> partitioning, MissingPartitionAttributeHandler missingAttributeHandler) {
        this.reader = reader;
        this.aggregator = aggregator;
        this.writer = writer;
        this.windowPolicy = windowPolicy;
        this.checkpoint = checkpoint;
        this.partitioning = partitioning;
        this.missingAttributeHandler = missingAttributeHandler;
    }

    @Override
    public void handle(MountTarget target) throws SpoolException {
        if (shouldSkip(target)) return;
        writer.write(target, aggregator.aggregate(reader.read(target)).flatMap(this::inTargetPartition));
        checkpoint.markMounted(target);
    }

    private boolean shouldSkip(MountTarget target) {
        return !windowPolicy.isClosed(target.sourceKey()) || checkpoint.isMounted(target);
    }

    private Stream<PartitionedRecord<?>> inTargetPartition(PartitionedRecord<O> result) {
        try {
            return Stream.of(new PartitionedRecord<>(partitioning.of(result), result.record()));
        } catch (MissingPartitionAttributeException e) {
            missingAttributeHandler.handle(result, e);
            return Stream.empty();
        }
    }
}
