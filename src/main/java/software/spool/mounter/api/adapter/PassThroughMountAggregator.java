package software.spool.mounter.api.adapter;

import software.spool.mounter.api.port.MountAggregator;

import java.util.stream.Stream;

public class PassThroughMountAggregator<I> implements MountAggregator<I, I> {
    @Override
    public Stream<I> aggregate(Stream<I> records) {
        return records;
    }
}
