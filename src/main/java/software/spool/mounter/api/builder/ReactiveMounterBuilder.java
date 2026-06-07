package software.spool.mounter.api.builder;

import software.spool.core.port.bus.EventBus;
import software.spool.core.port.watchdog.ModuleHeartBeat;
import software.spool.mounter.api.Mounter;
import software.spool.mounter.api.port.MountAggregator;
import software.spool.mounter.api.port.PartitionedReader;
import software.spool.mounter.internal.decorator.SafePartitionedReader;

public class ReactiveMounterBuilder<I, O> {
    final EventBus bus;
    final ModuleHeartBeat moduleHeartBeat;
    MountAggregator<O> aggregator;
    PartitionedReader reader;
    final MountFacet<ReactiveMounterBuilder<I, O>> mount;
    final CheckpointFacet<ReactiveMounterBuilder<I, O>> checkpoint;
    final ObservabilityFacet<ReactiveMounterBuilder<I, O>> observability;

    public ReactiveMounterBuilder(EventBus bus, ModuleHeartBeat moduleHeartBeat) {
        this.bus = bus;
        this.moduleHeartBeat = moduleHeartBeat;
        this.mount = new MountFacet<>(this);
        this.checkpoint = new CheckpointFacet<>(this);
        this.observability = new ObservabilityFacet<>(this);
    }

    public ReactiveMounterBuilder<I, O> aggregatingWith(MountAggregator<O> aggregator) {
        this.aggregator = aggregator;
        return this;
    }

    public ReactiveMounterBuilder<I, O> readingWith(PartitionedReader reader) {
        this.reader = SafePartitionedReader.of(reader);
        return this;
    }

    public MountFacet<ReactiveMounterBuilder<I, O>> mount() { return mount; }
    public CheckpointFacet<ReactiveMounterBuilder<I, O>> checkpoint() { return checkpoint; }
    public ObservabilityFacet<ReactiveMounterBuilder<I, O>> observability() { return observability; }

    public Mounter build() {
        return ReactiveMounterAssembler.assemble(this);
    }
}
