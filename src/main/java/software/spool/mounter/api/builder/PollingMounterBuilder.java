package software.spool.mounter.api.builder;

import software.spool.core.model.PartitionKey;
import software.spool.core.port.EventBusEmitter;
import software.spool.core.utils.ErrorRouter;
import software.spool.mounter.api.Mounter;
import software.spool.mounter.api.port.DataLakeReader;
import software.spool.mounter.api.port.DataMartWriter;
import software.spool.mounter.api.port.MountAggregator;
import software.spool.mounter.api.strategy.MountStrategy;
import software.spool.mounter.api.utils.PollingPolicy;
import software.spool.mounter.internal.control.PartitionMountHandler;
import software.spool.mounter.internal.strategy.PollingMountStrategy;
import software.spool.mounter.internal.scheduler.PollingScheduler;

public class PollingMounterBuilder<T, R> {
    private final DataLakeReader<T> reader;
    private MountAggregator<T, R> aggregator;
    private DataMartWriter<R> writer;
    private PollingPolicy policy;
    private EventBusEmitter emitter;
    private ErrorRouter errorRouter;
    private PartitionKey partitionKey;
    private PollingScheduler scheduler;

    public PollingMounterBuilder(DataLakeReader<T> reader, PollingScheduler scheduler, ErrorRouter errorRouter) {
        this.reader = reader;
        this.scheduler = scheduler;
        this.errorRouter = errorRouter;
    }

    private PollingMounterBuilder(DataLakeReader<T> reader, PollingPolicy policy,
                                  EventBusEmitter emitter, ErrorRouter errorRouter,
                                  PartitionKey partitionKey, PollingScheduler scheduler) {
        this.reader = reader;
        this.policy = policy;
        this.emitter = emitter;
        this.errorRouter = errorRouter;
        this.partitionKey = partitionKey;
        this.scheduler = scheduler;
    }

    public <NR> PollingMounterBuilder<T, NR> aggregatingWith(MountAggregator<T, NR> aggregator) {
        PollingMounterBuilder<T, NR> next = new PollingMounterBuilder<>(reader, policy, emitter, errorRouter, partitionKey, scheduler);
        next.aggregator = aggregator;
        return next;
    }

    public PollingMounterBuilder<T, R> writingWith(DataMartWriter<R> writer) {
        this.writer = writer;
        return this;
    }

    public PollingMounterBuilder<T, R> pollingWith(PollingPolicy policy) {
        this.policy = policy;
        return this;
    }

    public PollingMounterBuilder<T, R> emittingWith(EventBusEmitter emitter) {
        this.emitter = emitter;
        return this;
    }

    public PollingMounterBuilder<T, R> errorRouting(ErrorRouter errorRouter) {
        this.errorRouter = errorRouter;
        return this;
    }

    public PollingMounterBuilder<T, R> forPartition(PartitionKey partitionKey) {
        this.partitionKey = partitionKey;
        return this;
    }

    public PollingMounterBuilder<T, R> scheduledWith(PollingScheduler scheduler) {
        this.scheduler = scheduler;
        return this;
    }

    public Mounter build() {
        PartitionMountHandler<T, R> handler = new PartitionMountHandler<>(reader, aggregator, emitter, writer);
        MountStrategy strategy = new PollingMountStrategy(partitionKey, handler, errorRouter, scheduler, policy);
        return new Mounter(strategy, errorRouter);
    }
}
