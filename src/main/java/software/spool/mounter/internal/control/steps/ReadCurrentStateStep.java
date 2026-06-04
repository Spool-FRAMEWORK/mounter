package software.spool.mounter.internal.control.steps;

import software.spool.core.pipeline.PipelineContext;
import software.spool.core.pipeline.Step;
import software.spool.mounter.api.port.IncrementalDataMartReader;
import software.spool.mounter.api.port.MountTarget;

import javax.management.AttributeNotFoundException;
import java.util.Optional;

public class ReadCurrentStateStep<O> implements Step<PipelineContext, PipelineContext> {
    private final IncrementalDataMartReader<O> reader;

    public ReadCurrentStateStep(IncrementalDataMartReader<O> reader) {
        this.reader = reader;
    }

    @Override
    public PipelineContext apply(PipelineContext ctx) throws AttributeNotFoundException {
        MountTarget target = ctx.require(IncrementalContextKeys.TARGET);
        return ctx.with(IncrementalContextKeys.CURRENT, Optional.ofNullable(reader.readCurrent(target.sourceKey())));
    }
}
