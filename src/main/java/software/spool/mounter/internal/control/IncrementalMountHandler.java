package software.spool.mounter.internal.control;

import software.spool.core.pipeline.Pipeline;
import software.spool.core.pipeline.PipelineContext;
import software.spool.core.port.bus.Handler;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.internal.control.steps.IncrementalContextKeys;

public class IncrementalMountHandler<I, O> implements Handler<MountTarget> {
    private final Pipeline<PipelineContext, PipelineContext> pipeline;

    public IncrementalMountHandler(Pipeline<PipelineContext, PipelineContext> pipeline) {
        this.pipeline = pipeline;
    }

    @Override
    public void handle(MountTarget target) {
        pipeline.execute(PipelineContext.empty().with(IncrementalContextKeys.TARGET, target));
    }
}
