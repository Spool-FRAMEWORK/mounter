package software.spool.mounter.internal.decorator;

import software.spool.core.exception.MountAggregateException;
import software.spool.core.exception.SpoolException;
import software.spool.mounter.api.model.AggregatedRecord;
import software.spool.mounter.api.model.GenericRecord;
import software.spool.mounter.api.port.MountAggregator;
import software.spool.mounter.api.port.PartitionedRecord;

import java.util.stream.Stream;

public class SafeMountAggregator<O> implements MountAggregator<O> {
    private final MountAggregator<O> aggregator;

    public SafeMountAggregator(MountAggregator<O> aggregator) {
        this.aggregator = aggregator;
    }

    public static <O> SafeMountAggregator<O> of(MountAggregator<O> aggregator) {
        return new SafeMountAggregator<O>(aggregator);
    }

    @Override
    public Stream<AggregatedRecord<O>> aggregate(Stream<PartitionedRecord<GenericRecord>> records) {
        try {
            return aggregator.aggregate(records);
        } catch (SpoolException e) {
            throw e;
        } catch (Exception e) {
            throw new MountAggregateException(e.getMessage(), e);
        }
    }
}
