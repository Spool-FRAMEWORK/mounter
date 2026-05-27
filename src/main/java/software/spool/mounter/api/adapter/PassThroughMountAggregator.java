package software.spool.mounter.api.adapter;

import software.spool.mounter.api.model.AggregatedRecord;
import software.spool.mounter.api.model.GenericRecord;
import software.spool.mounter.api.port.MountAggregator;
import software.spool.mounter.api.port.PartitionedRecord;

import java.util.stream.Stream;

public class PassThroughMountAggregator implements MountAggregator<GenericRecord> {
    @Override
    public Stream<AggregatedRecord<GenericRecord>> aggregate(
            Stream<PartitionedRecord<GenericRecord>> records) {
        return records.map(p -> new AggregatedRecord<>(p.record(), p.record()));
    }
}
