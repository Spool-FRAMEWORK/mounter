package software.spool.mounter.internal.control.steps;

import software.spool.core.model.vo.PartitionKey;
import software.spool.core.pipeline.PipelineContext;
import software.spool.core.pipeline.Step;
import software.spool.mounter.api.port.IncrementalDataMartReader;
import software.spool.mounter.api.port.MergeableMountAggregator;
import software.spool.mounter.api.port.MountTarget;

import javax.management.AttributeNotFoundException;
import java.util.List;
import java.util.Optional;

public class MergePartitionsStep<O> implements Step<PipelineContext, PipelineContext> {
    private final IncrementalDataMartReader<O> reader;
    private final MergeableMountAggregator<O> aggregator;

    public MergePartitionsStep(IncrementalDataMartReader<O> reader, MergeableMountAggregator<O> aggregator) {
        this.reader = reader;
        this.aggregator = aggregator;
    }

    @SuppressWarnings("unchecked")
    @Override
    public PipelineContext apply(PipelineContext ctx) throws AttributeNotFoundException {
        MountTarget target = ctx.require(IncrementalContextKeys.TARGET);
        List<PartitionKey> partitions = ctx.require(IncrementalContextKeys.CLOSED_PENDING);
        O current = ((Optional<O>) ctx.require(IncrementalContextKeys.CURRENT)).orElse(null);
        for (PartitionKey sourceKey : partitions) {
            current = aggregator.merge(current, reader.read(target.withSourceKey(sourceKey)))
                    .findFirst()
                    .orElse(current);
        }
        return ctx.with(IncrementalContextKeys.CURRENT, Optional.ofNullable(current));
    }
}
