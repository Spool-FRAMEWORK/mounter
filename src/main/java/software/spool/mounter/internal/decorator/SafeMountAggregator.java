package software.spool.mounter.internal.decorator;

import software.spool.core.exception.MountAggregateException;
import software.spool.core.exception.SpoolException;
import software.spool.mounter.api.port.MountAggregator;

import java.util.stream.Stream;

public class SafeMountAggregator<I, O> implements MountAggregator<I, O> {
    private final MountAggregator<I, O> aggregator;

    public SafeMountAggregator(MountAggregator<I, O> aggregator) {
        this.aggregator = aggregator;
    }

    @Override
    public Stream<O> aggregate(Stream<I> records) {
        try {
            return aggregator.aggregate(records);
        } catch(SpoolException e) {
            throw e;
        } catch (Exception e) {
            throw new MountAggregateException(e.getMessage());
        }
    }
}
