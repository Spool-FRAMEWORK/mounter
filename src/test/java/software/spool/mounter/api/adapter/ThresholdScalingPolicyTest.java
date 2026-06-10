package software.spool.mounter.api.adapter;

import org.junit.jupiter.api.Test;
import software.spool.core.model.vo.PartitionKey;
import software.spool.mounter.api.port.scaling.ExecutionMode;
import software.spool.mounter.api.port.scaling.PartitionInfo;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ThresholdScalingPolicyTest {

    private static final ThresholdScalingPolicy POLICY = ThresholdScalingPolicy.of(2, 5, 1000L);

    @Test
    void resolve_fewPartitions_returnsSequentialPlan() {
        List<PartitionInfo> partitions = List.of(
            PartitionInfo.of(new PartitionKey("p1")),
            PartitionInfo.of(new PartitionKey("p2"))
        );
        assertThat(POLICY.resolve(partitions).mode()).isEqualTo(ExecutionMode.SEQUENTIAL);
    }

    @Test
    void resolve_manyPartitions_returnsLocalParallelPlan() {
        List<PartitionInfo> partitions = List.of(
            PartitionInfo.of(new PartitionKey("p1")),
            PartitionInfo.of(new PartitionKey("p2")),
            PartitionInfo.of(new PartitionKey("p3"))
        );
        assertThat(POLICY.resolve(partitions).mode()).isEqualTo(ExecutionMode.LOCAL_PARALLEL);
    }

    @Test
    void resolve_largePartition_returnsLocalParallelPlan() {
        List<PartitionInfo> partitions = List.of(PartitionInfo.of(new PartitionKey("p1"), 2000L, 1000L));
        assertThat(POLICY.resolve(partitions).mode()).isEqualTo(ExecutionMode.LOCAL_PARALLEL);
    }

    @Test
    void resolve_exceedsDistributedThreshold_returnsDistributedPlan() {
        List<PartitionInfo> partitions = new ArrayList<>();
        for (int i = 0; i < 6; i++) partitions.add(PartitionInfo.of(new PartitionKey("p" + i)));
        assertThat(POLICY.resolve(partitions).mode()).isEqualTo(ExecutionMode.DISTRIBUTED);
    }
}
