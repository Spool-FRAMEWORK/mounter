package software.spool.mounter.internal.control.steps;

import software.spool.core.model.vo.PartitionKey;
import software.spool.core.pipeline.PipelineContext;
import software.spool.core.pipeline.Step;
import software.spool.mounter.api.port.MountCursor;
import software.spool.mounter.api.port.MountTarget;

import javax.management.AttributeNotFoundException;

public class AdvanceCursorStep implements Step<PipelineContext, PipelineContext> {
    private final MountCursor cursor;

    public AdvanceCursorStep(MountCursor cursor) {
        this.cursor = cursor;
    }

    @Override
    public PipelineContext apply(PipelineContext ctx) throws AttributeNotFoundException {
        MountTarget target = ctx.require(IncrementalContextKeys.TARGET);
        for (PartitionKey sourceKey : ctx.require(IncrementalContextKeys.CLOSED_PENDING)) {
            cursor.advance(target, sourceKey);
        }
        return ctx;
    }
}
