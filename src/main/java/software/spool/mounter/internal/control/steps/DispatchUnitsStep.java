package software.spool.mounter.internal.control.steps;

import software.spool.core.pipeline.PipelineContext;
import software.spool.core.pipeline.Step;
import software.spool.core.port.bus.Handler;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.scaling.ExecutionMode;
import software.spool.mounter.api.port.scaling.PartitionDispatcher;
import software.spool.mounter.api.port.scaling.ScalingPlan;

import javax.management.AttributeNotFoundException;
import java.util.List;

public class DispatchUnitsStep implements Step<PipelineContext, PipelineContext> {
    private final PartitionDispatcher localDispatcher;
    private final PartitionDispatcher distributedDispatcher;
    private final Handler<MountTarget> worker;

    public DispatchUnitsStep(PartitionDispatcher localDispatcher, PartitionDispatcher distributedDispatcher, Handler<MountTarget> worker) {
        this.localDispatcher = localDispatcher;
        this.distributedDispatcher = distributedDispatcher;
        this.worker = worker;
    }

    @Override
    public PipelineContext apply(PipelineContext ctx) throws AttributeNotFoundException {
        ScalingPlan plan = ctx.require(MountContextKeys.PLAN);
        List<MountTarget> units = ctx.require(MountContextKeys.UNITS);
        selectDispatcher(plan).dispatch(units, worker);
        return ctx;
    }

    private PartitionDispatcher selectDispatcher(ScalingPlan plan) {
        return plan.mode() == ExecutionMode.DISTRIBUTED ? distributedDispatcher : localDispatcher;
    }
}
