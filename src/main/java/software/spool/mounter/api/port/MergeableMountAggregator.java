package software.spool.mounter.api.port;

import software.spool.mounter.api.model.GenericRecord;

import java.util.stream.Stream;

public interface MergeableMountAggregator<O> extends MountAggregator<O> {
    Stream<O> merge(O previous, Stream<PartitionedRecord<GenericRecord>> newRecords);
}