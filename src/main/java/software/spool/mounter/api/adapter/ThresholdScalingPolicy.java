package software.spool.mounter.api.adapter;

import software.spool.mounter.api.port.scaling.PartitionInfo;
import software.spool.mounter.api.port.scaling.ScalingPlan;
import software.spool.mounter.api.port.scaling.ScalingPolicy;

import java.util.List;

public class ThresholdScalingPolicy implements ScalingPolicy {
    private final int localParallelAbovePartitions;
    private final int distributedAbovePartitions;
    private final long splitAboveRecords;

    private ThresholdScalingPolicy(int localParallelAbovePartitions, int distributedAbovePartitions, long splitAboveRecords) {
        this.localParallelAbovePartitions = localParallelAbovePartitions;
        this.distributedAbovePartitions = distributedAbovePartitions;
        this.splitAboveRecords = splitAboveRecords;
    }

    public static ThresholdScalingPolicy of(int localParallelAbovePartitions, int distributedAbovePartitions, long splitAboveRecords) {
        return new ThresholdScalingPolicy(localParallelAbovePartitions, distributedAbovePartitions, splitAboveRecords);
    }

    @Override
    public ScalingPlan resolve(List<PartitionInfo> partitions) {
        int count = partitions.size();
        if (count > distributedAbovePartitions) return ScalingPlan.distributed(splitAboveRecords);
        if (count > localParallelAbovePartitions) return ScalingPlan.localParallel(splitAboveRecords);
        boolean anyLarge = partitions.stream()
                .anyMatch(p -> p.estimatedRecords().isPresent() && p.estimatedRecords().getAsLong() > splitAboveRecords);
        if (anyLarge) return ScalingPlan.localParallel(splitAboveRecords);
        return ScalingPlan.sequential();
    }
}
