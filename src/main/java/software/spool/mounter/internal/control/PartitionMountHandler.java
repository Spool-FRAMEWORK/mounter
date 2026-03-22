package software.spool.mounter.internal.control;

import software.spool.core.control.Handler;
import software.spool.core.exception.SpoolException;
import software.spool.core.model.ItemsMounted;
import software.spool.core.model.PartitionKey;
import software.spool.core.port.EventBusEmitter;
import software.spool.mounter.api.port.DataLakeReader;
import software.spool.mounter.api.port.DataMartWriter;
import software.spool.mounter.api.port.MountAggregator;

import java.util.stream.Stream;

public class PartitionMountHandler<I, O> implements Handler<PartitionKey> {
    private final DataLakeReader<I> reader;
    private final MountAggregator<I, O> aggregator;
    private final EventBusEmitter emitter;
    private final DataMartWriter<O> writer;

    public PartitionMountHandler(DataLakeReader<I> reader, MountAggregator<I, O> aggregator,
                                 EventBusEmitter emitter, DataMartWriter<O> writer) {
        this.reader = reader;
        this.aggregator = aggregator;
        this.emitter = emitter;
        this.writer = writer;
    }

    @Override
    public void handle(PartitionKey partitionKey) throws SpoolException {
        Stream<O> aggregate = aggregator.aggregate(reader.read(partitionKey));
        emitter.emit(ItemsMounted.builder().partitionKey(partitionKey).build());
        writer.write(partitionKey, aggregate);
    }
}
