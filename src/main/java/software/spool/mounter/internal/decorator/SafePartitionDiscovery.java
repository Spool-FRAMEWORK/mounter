package software.spool.mounter.internal.decorator;

import software.spool.core.exception.DataLakeReadException;
import software.spool.core.exception.SpoolException;
import software.spool.core.model.vo.PartitionKey;
import software.spool.mounter.api.port.scaling.PartitionDiscovery;
import software.spool.mounter.api.port.scaling.PartitionInfo;

import java.util.List;

public class SafePartitionDiscovery implements PartitionDiscovery {
    private final PartitionDiscovery delegate;

    private SafePartitionDiscovery(PartitionDiscovery delegate) {
        this.delegate = delegate;
    }

    public static SafePartitionDiscovery of(PartitionDiscovery delegate) {
        return new SafePartitionDiscovery(delegate);
    }

    @Override
    public List<PartitionInfo> discover(PartitionKey scope) {
        try {
            return delegate.discover(scope);
        } catch (SpoolException e) {
            throw e;
        } catch (Exception e) {
            throw new DataLakeReadException(scope, e.getMessage(), e);
        }
    }
}
