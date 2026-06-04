package software.spool.mounter.internal.decorator;

import software.spool.core.exception.ScalingPolicyException;
import software.spool.core.exception.SpoolException;
import software.spool.mounter.api.port.scaling.PartitionInfo;
import software.spool.mounter.api.port.scaling.ScalingPlan;
import software.spool.mounter.api.port.scaling.ScalingPolicy;

import java.util.List;

public class SafeScalingPolicy implements ScalingPolicy {
    private final ScalingPolicy delegate;

    private SafeScalingPolicy(ScalingPolicy delegate) {
        this.delegate = delegate;
    }

    public static SafeScalingPolicy of(ScalingPolicy delegate) {
        return new SafeScalingPolicy(delegate);
    }

    @Override
    public ScalingPlan resolve(List<PartitionInfo> partitions) {
        try {
            return delegate.resolve(partitions);
        } catch (SpoolException e) {
            throw e;
        } catch (Exception e) {
            throw new ScalingPolicyException(e.getMessage());
        }
    }
}
