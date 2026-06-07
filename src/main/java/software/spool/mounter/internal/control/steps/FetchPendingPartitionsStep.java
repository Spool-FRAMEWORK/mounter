package software.spool.mounter.internal.control.steps;

import software.spool.core.model.vo.PartitionKey;
import software.spool.core.pipeline.PipelineContext;
import software.spool.core.pipeline.Step;
import software.spool.mounter.api.port.IncrementalDataMartReader;
import software.spool.mounter.api.port.MountCursor;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionWindowPolicy;

import javax.management.AttributeNotFoundException;
import java.util.List;

public class FetchPendingPartitionsStep implements Step<PipelineContext, PipelineContext> {
    private final IncrementalDataMartReader<?> reader;
    private final PartitionWindowPolicy windowPolicy;
    private final MountCursor cursor;

    public FetchPendingPartitionsStep(IncrementalDataMartReader<?> reader, PartitionWindowPolicy windowPolicy, MountCursor cursor) {
        this.reader = reader;
        this.windowPolicy = windowPolicy;
        this.cursor = cursor;
    }

    @Override
    public PipelineContext apply(PipelineContext ctx) throws AttributeNotFoundException {
        MountTarget target = ctx.require(IncrementalContextKeys.TARGET);
        List<PartitionKey> closedPending = reader.pendingPartitions(target.sourceKey(), cursor.processedSources(target))
                .stream()
                .filter(windowPolicy::isClosed)
                .toList();
        return ctx.with(IncrementalContextKeys.CLOSED_PENDING, closedPending);
    }
}
