package software.spool.mounter.internal.control.steps;

import software.spool.core.pipeline.PipelineContext;
import software.spool.core.pipeline.Step;
import software.spool.mounter.api.port.scaling.PartitionDiscovery;
import software.spool.mounter.api.port.scaling.PartitionInfo;

import javax.management.AttributeNotFoundException;
import java.util.List;

public class DiscoverPartitionsStep implements Step<PipelineContext, PipelineContext> {
    private final PartitionDiscovery discovery;

    public DiscoverPartitionsStep(PartitionDiscovery discovery) {
        this.discovery = discovery;
    }

    @Override
    public PipelineContext apply(PipelineContext ctx) throws AttributeNotFoundException {
        List<PartitionInfo> discovered = discovery.discover(ctx.require(MountContextKeys.TARGET).sourceKey());
        return ctx.with(MountContextKeys.DISCOVERED, discovered);
    }
}
