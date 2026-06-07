package software.spool.mounter.internal.control.steps;

import software.spool.core.model.vo.PartitionKey;
import software.spool.core.pipeline.PipelineContext;
import software.spool.core.pipeline.Step;
import software.spool.mounter.api.port.DataMartWriter;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedRecord;

import javax.management.AttributeNotFoundException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

public class WriteIncrementalResultStep<O> implements Step<PipelineContext, PipelineContext> {
    private final DataMartWriter writer;

    public WriteIncrementalResultStep(DataMartWriter writer) {
        this.writer = writer;
    }

    @SuppressWarnings("unchecked")
    @Override
    public PipelineContext apply(PipelineContext ctx) throws AttributeNotFoundException {
        List<PartitionKey> closedPending = ctx.require(IncrementalContextKeys.CLOSED_PENDING);
        Optional<O> current = (Optional<O>) ctx.require(IncrementalContextKeys.CURRENT);
        if (closedPending.isEmpty() || current.isEmpty()) return ctx;
        MountTarget target = ctx.require(IncrementalContextKeys.TARGET);
        writer.write(target, Stream.<PartitionedRecord<?>>of(new PartitionedRecord<>(target.sourceKey(), current.get())));
        return ctx;
    }
}
