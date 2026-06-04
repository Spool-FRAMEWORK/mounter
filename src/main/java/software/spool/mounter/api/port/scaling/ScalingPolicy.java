package software.spool.mounter.api.port.scaling;

import java.util.List;

public interface ScalingPolicy {
    ScalingPlan resolve(List<PartitionInfo> partitions);
}
