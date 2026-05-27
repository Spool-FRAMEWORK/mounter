package software.spool.mounter.api.port;

import software.spool.mounter.api.model.AggregatedRecord;
import software.spool.mounter.api.model.GenericRecord;

import java.util.stream.Stream;

public interface MountAggregator<O> {
    Stream<AggregatedRecord<O>> aggregate(Stream<PartitionedRecord<GenericRecord>> records);
}
