package software.spool.mounter.api.port;

import java.util.stream.Stream;

public interface MountAggregator<I, O> {
    Stream<O> aggregate(Stream<PartitionedRecord<I>> records);
}
