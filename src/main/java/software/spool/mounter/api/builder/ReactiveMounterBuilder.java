package software.spool.mounter.api.builder;

import software.spool.core.port.bus.EventBus;
import software.spool.core.port.bus.Handler;
import software.spool.core.port.watchdog.ModuleHeartBeat;
import software.spool.core.utils.routing.ErrorRouter;
import software.spool.mounter.api.Mounter;
import software.spool.mounter.api.port.*;
import software.spool.mounter.api.strategy.MountStrategy;
import software.spool.mounter.api.utils.MounterErrorRouter;
import software.spool.mounter.internal.control.AtomicMountHandler;
import software.spool.mounter.internal.decorator.SafeDataMartWriter;
import software.spool.mounter.internal.decorator.SafePartitionedReader;
import software.spool.mounter.internal.strategy.ReactiveMountStrategy;
import software.spool.mounter.internal.utils.RecordPartitionKeyExtractor;

import java.util.Objects;

public class ReactiveMounterBuilder<I, O> {
    private final EventBus bus;
    private final ModuleHeartBeat moduleHeartBeat;
    private MountTarget target;
    private PartitionedReader<I> reader;
    private MountAggregator<I, O> aggregator;
    private DataMartWriter<O> writer;
    private ErrorRouter errorRouter;
    private PartitionWindowPolicy partitionWindowPolicy;
    private MountCheckpoint checkpoint;
    private MountPartitionSchema<O> mountPartitionSchema;


    public ReactiveMounterBuilder(EventBus bus, ModuleHeartBeat moduleHeartBeat) {
        this.bus = bus;
        this.moduleHeartBeat = moduleHeartBeat;
    }

    public ReactiveMounterBuilder<I, O> aggregatingWith(MountAggregator<I, O> aggregator) {
        this.aggregator = aggregator;
        return this;
    }

    public ReactiveMounterBuilder<I, O> writingWith(DataMartWriter<O> writer) {
        this.writer = SafeDataMartWriter.of(writer);
        return this;
    }

    public ReactiveMounterBuilder<I, O> readingWith(PartitionedReader<I> reader) {
        this.reader = SafePartitionedReader.of(reader);
        return this;
    }

    public ReactiveMounterBuilder<I, O> errorRouting(ErrorRouter errorRouter) {
        this.errorRouter = errorRouter;
        return this;
    }

    public ReactiveMounterBuilder<I, O> onTarget(MountTarget target) {
        this.target = target;
        return this;
    }

    public ReactiveMounterBuilder<I, O> partitionWindowPolicy(PartitionWindowPolicy partitionWindowPolicy) {
        this.partitionWindowPolicy = partitionWindowPolicy;
        return this;
    }

    public ReactiveMounterBuilder<I, O> checkpoint(MountCheckpoint checkpoint) {
        this.checkpoint = checkpoint;
        return this;
    }

    public ReactiveMounterBuilder<I, O> partitioningWith(MountPartitionSchema<O> mountPartitionSchema) {
        this.mountPartitionSchema = mountPartitionSchema;
        return this;
    }

    public Mounter build() {
        Handler<MountTarget> handler = new AtomicMountHandler<>(
                reader, aggregator, writer, bus, partitionWindowPolicy, checkpoint, new RecordPartitionKeyExtractor<>(mountPartitionSchema)
        );

        MountStrategy strategy = new ReactiveMountStrategy(target, bus, handler, getErrorRouter());
        return new Mounter(strategy, getErrorRouter(), moduleHeartBeat);
    }

    private ErrorRouter getErrorRouter() {
        return Objects.requireNonNullElse(errorRouter, MounterErrorRouter.defaults(bus));
    }
}

