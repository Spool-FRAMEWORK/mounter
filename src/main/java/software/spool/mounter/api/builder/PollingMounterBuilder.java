package software.spool.mounter.api.builder;

import software.spool.core.port.bus.EventBusEmitter;
import software.spool.core.port.decorator.SafeEventBusEmitter;
import software.spool.core.port.watchdog.ModuleHeartBeat;
import software.spool.core.utils.polling.PollingPolicy;
import software.spool.core.utils.polling.PollingScheduler;
import software.spool.core.utils.polling.ThreadedPollingScheduler;
import software.spool.core.utils.routing.ErrorRouter;
import software.spool.mounter.api.Mounter;
import software.spool.mounter.api.port.*;
import software.spool.mounter.api.strategy.MountStrategy;
import software.spool.mounter.api.utils.MounterErrorRouter;
import software.spool.mounter.internal.control.PartitionMountHandler;
import software.spool.mounter.internal.decorator.SafeDataMartWriter;
import software.spool.mounter.internal.strategy.PollingMountStrategy;

import java.util.Objects;

public class PollingMounterBuilder<T> {

    private final DataLakeReader<T> reader;
    private final ModuleHeartBeat moduleHeartBeat;

    public PollingMounterBuilder(DataLakeReader<T> reader, ModuleHeartBeat moduleHeartBeat) {
        this.reader = reader;
        this.moduleHeartBeat = moduleHeartBeat;
    }

    public <R> Configured<T, R> aggregatingWith(MountAggregator<T, R> aggregator) {
        return new Configured<>(reader, moduleHeartBeat, aggregator);
    }

    public static class Configured<T, R> {

        private final DataLakeReader<T> reader;
        private final ModuleHeartBeat moduleHeartBeat;
        private final MountAggregator<T, R> aggregator;

        private DataMartWriter<R> writer;
        private PollingPolicy policy;
        private EventBusEmitter emitter;
        private ErrorRouter errorRouter;
        private MountTarget target;
        private PollingScheduler scheduler;
        private PartitionWindowPolicy partitionWindowPolicy;
        private MountCheckpoint checkpoint;

        private Configured(DataLakeReader<T> reader, ModuleHeartBeat moduleHeartBeat, MountAggregator<T, R> aggregator) {
            this.reader = reader;
            this.moduleHeartBeat = moduleHeartBeat;
            this.aggregator = aggregator;
            this.scheduler = new ThreadedPollingScheduler();
        }

        public Configured<T, R> writingWith(DataMartWriter<R> writer) {
            this.writer = SafeDataMartWriter.of(writer);
            return this;
        }

        public Configured<T, R> pollingWith(PollingPolicy policy) {
            this.policy = policy;
            return this;
        }

        public Configured<T, R> emittingWith(EventBusEmitter emitter) {
            this.emitter = SafeEventBusEmitter.of(emitter);
            return this;
        }

        public Configured<T, R> errorRouting(ErrorRouter errorRouter) {
            this.errorRouter = errorRouter;
            return this;
        }

        public Configured<T, R> onTarget(MountTarget target) {
            this.target = target;
            return this;
        }

        public Configured<T, R> partitionWindowPolicy(PartitionWindowPolicy partitionWindowPolicy) {
            this.partitionWindowPolicy = partitionWindowPolicy;
            return this;
        }

        public Configured<T, R> checkpoint(MountCheckpoint checkpoint) {
            this.checkpoint = checkpoint;
            return this;
        }

        public Configured<T, R> scheduledWith(PollingScheduler scheduler) {
            this.scheduler = scheduler;
            return this;
        }

        public Mounter build() {
            PartitionMountHandler<T, R> handler = new PartitionMountHandler<>(
                    reader, aggregator, emitter, writer, partitionWindowPolicy, checkpoint
            );
            ErrorRouter router = getErrorRouter();
            MountStrategy strategy = new PollingMountStrategy(target, handler, router, scheduler, policy);
            return new Mounter(strategy, router, moduleHeartBeat);
        }

        private ErrorRouter getErrorRouter() {
            return Objects.requireNonNullElse(errorRouter, MounterErrorRouter.defaults(emitter));
        }
    }
}