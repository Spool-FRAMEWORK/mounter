package software.spool.mounter.internal.control.steps;

import software.spool.core.pipeline.PipelineContext;
import software.spool.core.pipeline.Step;
import software.spool.mounter.api.port.scaling.ScalingPlan;
import software.spool.mounter.api.port.scaling.ScalingPolicy;

import javax.management.AttributeNotFoundException;

public class ResolveScalingPlanStep implements Step<PipelineContext, PipelineContext> {
    private final ScalingPolicy policy;

    public ResolveScalingPlanStep(ScalingPolicy policy) {
        this.policy = policy;
    }

    @Override
    public PipelineContext apply(PipelineContext ctx) throws AttributeNotFoundException {
        ScalingPlan plan = policy.resolve(ctx.require(MountContextKeys.DISCOVERED));
        return ctx.with(MountContextKeys.PLAN, plan);
    }
}
