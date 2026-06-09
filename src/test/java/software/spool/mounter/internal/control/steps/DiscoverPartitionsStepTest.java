package software.spool.mounter.internal.control.steps;

import org.junit.jupiter.api.Test;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.pipeline.PipelineContext;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.scaling.PartitionInfo;

import javax.management.AttributeNotFoundException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class DiscoverPartitionsStepTest {

    @Test
    void apply_discoveryHasPartitions_addsDiscoveredToContext() throws AttributeNotFoundException {
        PartitionKey scope = new PartitionKey("scope");
        MountTarget target = MountTarget.transformation("dm", scope);
        List<PartitionInfo> partitions = List.of(PartitionInfo.of(scope));
        PipelineContext ctx = PipelineContext.empty().with(MountContextKeys.TARGET, target);

        PipelineContext result = new DiscoverPartitionsStep(s -> partitions).apply(ctx);

        assertThat(result.require(MountContextKeys.DISCOVERED)).isEqualTo(partitions);
    }

    @Test
    void apply_discoveryReturnsEmpty_addsEmptyListToContext() throws AttributeNotFoundException {
        MountTarget target = MountTarget.transformation("dm", new PartitionKey("scope"));
        PipelineContext ctx = PipelineContext.empty().with(MountContextKeys.TARGET, target);

        PipelineContext result = new DiscoverPartitionsStep(s -> List.of()).apply(ctx);

        assertThat(result.require(MountContextKeys.DISCOVERED)).isEmpty();
    }
}
