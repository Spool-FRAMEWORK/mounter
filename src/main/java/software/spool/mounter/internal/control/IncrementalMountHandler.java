package software.spool.mounter.internal.control;

import software.spool.core.exception.SpoolException;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.port.bus.EventPublisher;
import software.spool.core.port.bus.Handler;
import software.spool.mounter.api.port.*;

import java.util.List;
import java.util.stream.Stream;

public class IncrementalMountHandler<I, O> implements Handler<MountTarget> {
    private final IncrementalDataMartReader<O> reader;
    private final MergeableMountAggregator<O> aggregator;
    private final DataMartWriter<O> writer;
    private final EventPublisher publisher;
    private final PartitionWindowPolicy windowPolicy;
    private final MountCursor cursor;
    private final PartitionKeyExtractor<O> keyExtractor;

    public IncrementalMountHandler(IncrementalDataMartReader<O> reader,
                                   MergeableMountAggregator<O> aggregator,
                                   DataMartWriter<O> writer,
                                   EventPublisher publisher,
                                   PartitionWindowPolicy windowPolicy,
                                   MountCursor cursor,
                                   PartitionKeyExtractor<O> keyExtractor) {
        this.reader = reader;
        this.aggregator = aggregator;
        this.writer = writer;
        this.publisher = publisher;
        this.windowPolicy = windowPolicy;
        this.cursor = cursor;
        this.keyExtractor = keyExtractor;
    }

    @Override
    public void handle(MountTarget target) throws SpoolException {
        List<PartitionKey> closedPending = getClosedPendingPartitions(target);
        if (closedPending.isEmpty()) return;
        O aggregatedResult = processPartitions(closedPending, getCurrent(target), target);
        writeResult(target, aggregatedResult);
        commitCursor(target, closedPending);
    }

    private O getCurrent(MountTarget target) {
        return reader.readCurrent(target.sourceKey());
    }

    private List<PartitionKey> getClosedPendingPartitions(MountTarget target) {
        return reader.pendingPartitions(target.sourceKey(), cursor.processedSources(target))
                .stream()
                .filter(windowPolicy::isClosed)
                .toList();
    }

    private O processPartitions(List<PartitionKey> partitions, O current, MountTarget mountTarget) {
        for (PartitionKey sourceKey : partitions) {
            current = mergePartition(current, mountTarget);
        }
        return current;
    }

    private O mergePartition(O current, MountTarget mountTarget) {
        return aggregator.merge(current, reader.read(mountTarget).stream())
                .findFirst()
                .orElse(current);
    }

    private void writeResult(MountTarget target, O current) {
        if (current == null) return;
        writer.write(target, Stream.of(new PartitionedRecord<>(target.sourceKey(), current)));
    }

    private void commitCursor(MountTarget target, List<PartitionKey> partitions) {
        for (PartitionKey sourceKey : partitions) {
            cursor.advance(target, sourceKey);
        }
    }
}