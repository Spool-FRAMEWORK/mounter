package software.spool.mounter.internal.decorator;

import software.spool.core.exception.PartitionSplitException;
import software.spool.core.exception.SpoolException;
import software.spool.mounter.api.port.scaling.PartitionInfo;
import software.spool.mounter.api.port.scaling.PartitionSlice;
import software.spool.mounter.api.port.scaling.PartitionSplitter;
import software.spool.mounter.api.port.scaling.SplitHint;

import java.util.List;

public class SafePartitionSplitter implements PartitionSplitter {
    private final PartitionSplitter delegate;

    private SafePartitionSplitter(PartitionSplitter delegate) {
        this.delegate = delegate;
    }

    public static SafePartitionSplitter of(PartitionSplitter delegate) {
        return new SafePartitionSplitter(delegate);
    }

    @Override
    public List<PartitionSlice> split(PartitionInfo partition, SplitHint hint) {
        try {
            return delegate.split(partition, hint);
        } catch (SpoolException e) {
            throw e;
        } catch (Exception e) {
            throw new PartitionSplitException(partition.key(), e.getMessage(), e);
        }
    }
}
