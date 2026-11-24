package software.spool.mounter.internal.control.steps;

import software.spool.core.pipeline.PipelineContext;
import software.spool.core.pipeline.Step;
import software.spool.mounter.api.port.DataMartWriter;
import software.spool.mounter.api.port.MountTarget;

import javax.management.AttributeNotFoundException;

/**
 * Runs the dispatch of the units of a full mount as one round of its writer, so that what the mount wrote before and
 * no longer writes, in any unit of its scope, is removed when the round ends well. A round that fails removes nothing.
 */
public class FullMountRoundStep implements Step<PipelineContext, PipelineContext> {
    private final Step<PipelineContext, PipelineContext> dispatch;
    private final DataMartWriter writer;

    public FullMountRoundStep(Step<PipelineContext, PipelineContext> dispatch, DataMartWriter writer) {
        this.dispatch = dispatch;
        this.writer = writer;
    }

    @Override
    public PipelineContext apply(PipelineContext context) throws AttributeNotFoundException {
        MountTarget target = context.require(MountContextKeys.TARGET);
        writer.beginRound(target);
        PipelineContext dispatched;
        try {
            dispatched = dispatch.apply(context);
        } catch (AttributeNotFoundException | RuntimeException e) {
            writer.abandonRound(target);
            throw e;
        }
        writer.completeRound(target);
        return dispatched;
    }
}
