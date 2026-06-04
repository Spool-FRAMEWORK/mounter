package software.spool.mounter.api.port;

import java.util.List;

public interface ScalingPolicy {
    ScalingPlan resolve(List<PartitionInfo> partitions);
}
