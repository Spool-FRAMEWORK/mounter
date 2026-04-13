package software.spool.mounter.internal.control;

import software.spool.core.exception.SpoolException;
import software.spool.core.model.event.ItemsMounted;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.port.bus.EventBusEmitter;
import software.spool.core.port.bus.Handler;
import software.spool.mounter.api.port.*;

import java.util.List;
import java.util.stream.Stream;

public class IncrementalMountHandler<I, O> implements Handler<MountTarget> {
    private final IncrementalDataMartReader<I, O> reader;
    private final MergeableMountAggregator<I, O> aggregator;
    private final DataMartWriter<O> writer;
    private final EventBusEmitter emitter;
    private final PartitionWindowPolicy windowPolicy;
    private final MountCursor cursor;

    // 1. Añadimos el extractor para mantener consistencia con el Atomic
    private final PartitionKeyExtractor<O> keyExtractor;

    public IncrementalMountHandler(IncrementalDataMartReader<I, O> reader,
                                   MergeableMountAggregator<I, O> aggregator,
                                   DataMartWriter<O> writer,
                                   EventBusEmitter emitter,
                                   PartitionWindowPolicy windowPolicy,
                                   MountCursor cursor,
                                   PartitionKeyExtractor<O> keyExtractor) {
        this.reader = reader;
        this.aggregator = aggregator;
        this.writer = writer;
        this.emitter = emitter;
        this.windowPolicy = windowPolicy;
        this.cursor = cursor;
        this.keyExtractor = keyExtractor;
    }

    @Override
    public void handle(MountTarget target) throws SpoolException {
        List<PartitionKey> closedPending = getClosedPendingPartitions(target);
        if (closedPending.isEmpty()) return;

        // 2. Separamos el procesamiento de la escritura y el commit del cursor
        O aggregatedResult = processPartitions(closedPending, getCurrent(target));

        writeResult(target, aggregatedResult);

        // 3. ¡CRÍTICO! Avanzamos el cursor SOLO después de escribir con éxito
        commitCursor(target, closedPending);

        emitEvent(target);
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

    private O processPartitions(List<PartitionKey> partitions, O current) {
        for (PartitionKey sourceKey : partitions) {
            current = mergePartition(current, sourceKey);
        }
        return current;
    }

    private O mergePartition(O current, PartitionKey sourceKey) {
        return aggregator.merge(current, reader.read(sourceKey).stream())
                .findFirst()
                .orElse(current);
    }

    private void writeResult(MountTarget target, O current) {
        if (current == null) return;

        PartitionKey destKey = keyExtractor.extract(current);
        writer.write(target, Stream.of(new PartitionedRecord<>(destKey, current)));
    }

    private void commitCursor(MountTarget target, List<PartitionKey> partitions) {
        for (PartitionKey sourceKey : partitions) {
            cursor.advance(target, sourceKey);
        }
    }

    private void emitEvent(MountTarget target) {
        emitter.emit(ItemsMounted.builder()
                .partitionKey(target.sourceKey())
                .build());
    }
}