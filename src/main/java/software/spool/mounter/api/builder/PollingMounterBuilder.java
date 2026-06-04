package software.spool.mounter.api.builder;

import software.spool.core.model.vo.PartitionKey;
import software.spool.core.port.bus.EventPublisher;
import software.spool.core.port.bus.Handler;
import software.spool.core.port.decorator.SafeEventPublisher;
import software.spool.core.port.watchdog.ModuleHeartBeat;
import software.spool.core.utils.polling.PollingPolicy;
import software.spool.core.utils.polling.PollingScheduler;
import software.spool.core.utils.polling.ThreadedPollingScheduler;
import software.spool.core.utils.routing.ErrorRouter;
import software.spool.mounter.api.Mounter;
import software.spool.mounter.api.port.*;
import software.spool.mounter.api.port.scaling.*;
import software.spool.mounter.api.strategy.MountStrategy;
import software.spool.mounter.api.utils.MounterErrorRouter;
import software.spool.mounter.internal.control.AtomicMountHandler;
import software.spool.mounter.internal.decorator.SafeDataMartWriter;
import software.spool.mounter.internal.strategy.PollingMountStrategy;
import software.spool.mounter.internal.utils.RecordPartitionKeyExtractor;

import java.util.Objects;

public class PollingMounterBuilder<T> {
    private final PartitionedReader reader;
    private final ModuleHeartBeat moduleHeartBeat;

    public PollingMounterBuilder(PartitionedReader reader, ModuleHeartBeat moduleHeartBeat) {
        this.reader = reader;
        this.moduleHeartBeat = moduleHeartBeat;
    }

    public <R> Configured<T, R> aggregatingWith(MountAggregator<R> aggregator) {
        return new Configured<>(reader, moduleHeartBeat, aggregator);
    }

    public static class Configured<T, R> {
        private final PartitionedReader reader;
        private final ModuleHeartBeat moduleHeartBeat;
        private final MountAggregator<R> aggregator;
        private DataMartWriter writer;
        private PollingPolicy policy;
        private EventPublisher publisher;
        private ErrorRouter errorRouter;
        private MountTarget target;
        private PollingScheduler scheduler;
        private PartitionWindowPolicy partitionWindowPolicy;
        private MountCheckpoint checkpoint;
        private MountCursor cursor;
        private PartitionKeyExtractor<R> keyExtractor;
        private PartitionKey scope;
        private PartitionDiscovery discovery;
        private PartitionSplitter splitter;
        private ScalingPolicy scalingPolicy;
        private PartitionDispatcher localDispatcher;
        private PartitionDispatcher distributedDispatcher;

        private Configured(PartitionedReader reader, ModuleHeartBeat moduleHeartBeat, MountAggregator<R> aggregator) {
            this.reader = reader;
            this.moduleHeartBeat = moduleHeartBeat;
            this.aggregator = aggregator;
            this.scheduler = new ThreadedPollingScheduler();
        }

        public Configured<T, R> writingWith(DataMartWriter writer) {
            this.writer = SafeDataMartWriter.of(writer);
            return this;
        }

        public Configured<T, R> pollingWith(PollingPolicy policy) {
            this.policy = policy;
            return this;
        }

        public Configured<T, R> emittingWith(EventPublisher publisher) {
            this.publisher = SafeEventPublisher.of(publisher);
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

        public Configured<T, R> onScope(PartitionKey scope) {
            this.scope = scope;
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

        public Configured<T, R> cursor(MountCursor cursor) {
            this.cursor = cursor;
            return this;
        }

        public Configured<T, R> scheduledWith(PollingScheduler scheduler) {
            this.scheduler = scheduler;
            return this;
        }

        public Configured<T, R> partitioningWith(MountPartitionSchema<R> schema) {
            this.keyExtractor = new RecordPartitionKeyExtractor<>(schema);
            return this;
        }

        public Configured<T, R> partitioningWith(PartitionKeyExtractor<R> keyExtractor) {
            this.keyExtractor = keyExtractor;
            return this;
        }

        public Configured<T, R> discoveringWith(PartitionDiscovery discovery) {
            this.discovery = discovery;
            return this;
        }

        public Configured<T, R> splittingWith(PartitionSplitter splitter) {
            this.splitter = splitter;
            return this;
        }

        public Configured<T, R> scalingWith(ScalingPolicy scalingPolicy) {
            this.scalingPolicy = scalingPolicy;
            return this;
        }

        public Configured<T, R> dispatchingWith(PartitionDispatcher localDispatcher) {
            this.localDispatcher = localDispatcher;
            return this;
        }

        public Configured<T, R> dispatchingWith(PartitionDispatcher localDispatcher, PartitionDispatcher distributedDispatcher) {
            this.localDispatcher = localDispatcher;
            this.distributedDispatcher = distributedDispatcher;
            return this;
        }

        public Mounter build() {
            ErrorRouter router = getErrorRouter();
            MountTarget effectiveTarget = scope != null ? target.withSourceKey(scope) : target;
            Handler<MountTarget> handler = new AtomicMountHandler<>(
                    reader, aggregator, writer, publisher, partitionWindowPolicy, checkpoint, keyExtractor,
                    discovery, splitter, scalingPolicy, localDispatcher, distributedDispatcher
            );
            MountStrategy strategy = new PollingMountStrategy(effectiveTarget, handler, router, scheduler, policy);
            return new Mounter(strategy, router, moduleHeartBeat);
        }

        private ErrorRouter getErrorRouter() {
            return Objects.requireNonNullElse(errorRouter, MounterErrorRouter.defaults(publisher));
        }
    }
}
