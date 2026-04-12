package software.spool.mounter.internal.control;

import software.spool.core.exception.SpoolException;
import software.spool.core.model.event.ItemsMounted;
import software.spool.core.port.bus.EventBusEmitter;
import software.spool.core.port.bus.Handler;
import software.spool.mounter.api.port.*;

import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

public class PartitionMountHandler<I, O> implements Handler<MountTarget> {
    private final DataLakeReader<I> reader;
    private final MountAggregator<I, O> aggregator;
    private final EventBusEmitter emitter;
    private final DataMartWriter<O> writer;
    private final PartitionWindowPolicy partitionWindowPolicy;
    private final MountCheckpoint checkpoint;

    public PartitionMountHandler(DataLakeReader<I> reader, MountAggregator<I, O> aggregator,
                                 EventBusEmitter emitter, DataMartWriter<O> writer,
                                 PartitionWindowPolicy partitionWindowPolicy, MountCheckpoint checkpoint) {
        this.reader = reader;
        this.aggregator = aggregator;
        this.emitter = emitter;
        this.writer = writer;
        this.partitionWindowPolicy = partitionWindowPolicy;
        this.checkpoint = checkpoint;
    }

    @Override
    public void handle(MountTarget target) throws SpoolException {
        if (!partitionWindowPolicy.isClosed(target.partitionKey())) return;
        if (checkpoint.isMounted(target)) return;
        AtomicLong counter = new AtomicLong(0);
        Stream<I> records = reader.read(target.partitionKey()).peek(__ -> counter.incrementAndGet());
        Stream<O> aggregate = aggregator.aggregate(records);
        emitter.emit(ItemsMounted.builder().partitionKey(target.partitionKey()).build());
        writer.write(target, aggregate);
        checkpoint.markMounted(target);
    }
}
