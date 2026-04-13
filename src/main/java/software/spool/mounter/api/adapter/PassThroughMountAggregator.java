package software.spool.mounter.api.adapter;

import software.spool.mounter.api.port.MountAggregator;
import software.spool.mounter.api.port.PartitionedRecord;

import java.util.stream.Stream;

public class PassThroughMountAggregator<I> implements MountAggregator<I, I> {

    @Override
    public Stream<I> aggregate(Stream<PartitionedRecord<I>> records) {
        return records.map(PartitionedRecord::record);
    }
}
