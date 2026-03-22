package software.spool.mounter.api.builder;

import software.spool.core.port.EventBus;
import software.spool.core.utils.ErrorRouter;
import software.spool.mounter.api.Mounter;
import software.spool.mounter.api.port.DataLakeReader;
import software.spool.mounter.api.port.DataMartWriter;
import software.spool.mounter.api.port.MountAggregator;
import software.spool.mounter.api.strategy.MountStrategy;
import software.spool.mounter.internal.control.PartitionMountHandler;
import software.spool.mounter.internal.decorator.SafeDataLakeReader;
import software.spool.mounter.internal.decorator.SafeDataMartWriter;
import software.spool.mounter.internal.strategy.ReactiveMountStrategy;

public class ReactiveMounterBuilder<I, O> {
    private final EventBus bus;
    private DataLakeReader<I> reader;
    private MountAggregator<I, O> aggregator;
    private DataMartWriter<O> writer;
    private ErrorRouter errorRouter;

    public ReactiveMounterBuilder(EventBus bus, ErrorRouter errorRouter) {
        this.bus = bus;
        this.errorRouter = errorRouter;
    }

    public <NO> ReactiveMounterBuilder<I, NO> aggregatingWith(MountAggregator<I, NO> aggregator) {
        ReactiveMounterBuilder<I, NO> next = new ReactiveMounterBuilder<>(reader, this.bus, this.errorRouter);
        next.aggregator = aggregator;
        return next;
    }

    private ReactiveMounterBuilder(DataLakeReader<I> reader, EventBus bus, ErrorRouter errorRouter) {
        this.reader = reader;
        this.bus = bus;
        this.errorRouter = errorRouter;
    }

    public ReactiveMounterBuilder<I, O> writingWith(DataMartWriter<O> writer) {
        this.writer = SafeDataMartWriter.of(writer);
        return this;
    }

    public ReactiveMounterBuilder<I, O> readingWith(DataLakeReader<I> reader) {
        this.reader = SafeDataLakeReader.of(reader);
        return this;
    }

    public ReactiveMounterBuilder<I, O> errorRouting(ErrorRouter errorRouter) {
        this.errorRouter = errorRouter;
        return this;
    }

    public Mounter build() {
        PartitionMountHandler<I, O> handler = new PartitionMountHandler<>(reader, aggregator, bus, writer);
        MountStrategy strategy = new ReactiveMountStrategy(bus, handler, errorRouter);
        return new Mounter(strategy, errorRouter);
    }
}

