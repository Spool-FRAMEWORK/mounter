package software.spool.mounter.api.builder;

import software.spool.mounter.api.port.scaling.*;

public class ScalingFacet<B> extends MounterFacet<B> {
    PartitionDiscovery discovery;
    PartitionSplitter splitter;
    ScalingPolicy scalingPolicy;
    PartitionDispatcher localDispatcher;
    PartitionDispatcher distributedDispatcher;

    ScalingFacet(B parent) { super(parent); }

    public ScalingFacet<B> discoveringWith(PartitionDiscovery discovery) {
        this.discovery = discovery;
        return this;
    }

    public ScalingFacet<B> splittingWith(PartitionSplitter splitter) {
        this.splitter = splitter;
        return this;
    }

    public ScalingFacet<B> scalingWith(ScalingPolicy scalingPolicy) {
        this.scalingPolicy = scalingPolicy;
        return this;
    }

    public ScalingFacet<B> dispatchingWith(PartitionDispatcher localDispatcher) {
        this.localDispatcher = localDispatcher;
        return this;
    }

    public ScalingFacet<B> dispatchingWith(PartitionDispatcher localDispatcher, PartitionDispatcher distributedDispatcher) {
        this.localDispatcher = localDispatcher;
        this.distributedDispatcher = distributedDispatcher;
        return this;
    }
}
