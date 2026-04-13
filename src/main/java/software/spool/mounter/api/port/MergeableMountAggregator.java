package software.spool.mounter.api.port;

import java.util.stream.Stream;

public interface MergeableMountAggregator<I, O> extends MountAggregator<I, O> {
    Stream<O> merge(O previous, Stream<PartitionedRecord<I>> newRecords);
}