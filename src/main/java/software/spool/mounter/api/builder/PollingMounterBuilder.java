package software.spool.mounter.api.builder;

import software.spool.core.port.watchdog.ModuleHeartBeat;
import software.spool.mounter.api.Mounter;
import software.spool.mounter.api.port.MountAggregator;
import software.spool.mounter.api.port.PartitionedReader;

public class PollingMounterBuilder<T, R> {
    final PartitionedReader reader;
    final ModuleHeartBeat heartBeat;
    MountAggregator<R> aggregator;
    final MountFacet<PollingMounterBuilder<T, R>> mount;
    final CheckpointFacet<PollingMounterBuilder<T, R>> checkpoint;
    final SchedulingFacet<PollingMounterBuilder<T, R>> scheduling;
    final ObservabilityFacet<PollingMounterBuilder<T, R>> observability;
    final ScalingFacet<PollingMounterBuilder<T, R>> scaling;

    public PollingMounterBuilder(PartitionedReader reader, ModuleHeartBeat heartBeat) {
        this.reader = reader;
        this.heartBeat = heartBeat;
        this.mount = new MountFacet<>(this);
        this.checkpoint = new CheckpointFacet<>(this);
        this.scheduling = new SchedulingFacet<>(this);
        this.observability = new ObservabilityFacet<>(this);
        this.scaling = new ScalingFacet<>(this);
    }

    public PollingMounterBuilder<T, R> aggregatingWith(MountAggregator<R> aggregator) {
        this.aggregator = aggregator;
        return this;
    }

    public MountFacet<PollingMounterBuilder<T, R>> mount() { return mount; }
    public CheckpointFacet<PollingMounterBuilder<T, R>> checkpoint() { return checkpoint; }
    public SchedulingFacet<PollingMounterBuilder<T, R>> scheduling() { return scheduling; }
    public ObservabilityFacet<PollingMounterBuilder<T, R>> observability() { return observability; }
    public ScalingFacet<PollingMounterBuilder<T, R>> scaling() { return scaling; }

    public Mounter build() {
        return PollingMounterAssembler.assemble(this);
    }
}
