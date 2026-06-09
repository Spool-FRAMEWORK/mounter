package software.spool.mounter.internal.control.steps;

import org.junit.jupiter.api.Test;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.pipeline.PipelineContext;
import software.spool.mounter.api.adapter.SequentialPartitionDispatcher;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.scaling.ScalingPlan;

import javax.management.AttributeNotFoundException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class DispatchUnitsStepTest {

    @Test
    void apply_sequentialPlan_eachUnitHandledInOrder() throws AttributeNotFoundException {
        List<MountTarget> handled = new ArrayList<>();
        PartitionKey scope = new PartitionKey("scope");
        List<MountTarget> units = List.of(
            MountTarget.transformation("dm1", scope),
            MountTarget.transformation("dm2", scope)
        );
        PipelineContext ctx = PipelineContext.empty()
            .with(MountContextKeys.PLAN, ScalingPlan.sequential())
            .with(MountContextKeys.UNITS, units);
        DispatchUnitsStep step = new DispatchUnitsStep(
            new SequentialPartitionDispatcher(),
            new SequentialPartitionDispatcher(),
            t -> handled.add(t)
        );

        step.apply(ctx);

        assertThat(handled).containsExactlyElementsOf(units);
    }

    @Test
    void apply_distributedPlan_usesDistributedDispatcher() throws AttributeNotFoundException {
        List<MountTarget> localHandled = new ArrayList<>();
        List<MountTarget> distributedHandled = new ArrayList<>();
        List<MountTarget> units = List.of(MountTarget.transformation("dm", new PartitionKey("scope")));
        PipelineContext ctx = PipelineContext.empty()
            .with(MountContextKeys.PLAN, ScalingPlan.distributed(1000L))
            .with(MountContextKeys.UNITS, units);
        DispatchUnitsStep step = new DispatchUnitsStep(
            (list, worker) -> list.forEach(t -> localHandled.add(t)),
            (list, worker) -> list.forEach(t -> distributedHandled.add(t)),
            t -> {}
        );

        step.apply(ctx);

        assertThat(localHandled).isEmpty();
        assertThat(distributedHandled).containsExactlyElementsOf(units);
    }
}
