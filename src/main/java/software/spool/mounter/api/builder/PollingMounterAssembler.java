package software.spool.mounter.api.builder;

import software.spool.core.port.bus.Handler;
import software.spool.core.utils.routing.ErrorRouter;
import software.spool.mounter.api.Mounter;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionKeyExtractor;
import software.spool.mounter.api.strategy.MountStrategy;
import software.spool.mounter.api.utils.MounterErrorRouter;
import software.spool.mounter.internal.control.AtomicMountHandler;
import software.spool.mounter.internal.strategy.PollingMountStrategy;

import java.util.Objects;

class PollingMounterAssembler {
    @SuppressWarnings("unchecked")
    static <T, R> Mounter assemble(PollingMounterBuilder<T, R> b) {
        ErrorRouter router = Objects.requireNonNullElse(b.observability.errorRouter,
                MounterErrorRouter.defaults(b.scheduling.publisher));
        MountTarget effectiveTarget = b.mount.scope != null
                ? b.mount.target.withSourceKey(b.mount.scope)
                : b.mount.target;
        Handler<MountTarget> handler = new AtomicMountHandler<>(
                b.reader, b.aggregator, b.mount.writer, b.scheduling.publisher,
                b.checkpoint.windowPolicy, b.checkpoint.checkpoint,
                (PartitionKeyExtractor<R>) b.mount.keyExtractor,
                b.scaling.discovery, b.scaling.splitter, b.scaling.scalingPolicy,
                b.scaling.localDispatcher, b.scaling.distributedDispatcher
        );
        MountStrategy strategy = new PollingMountStrategy(effectiveTarget, handler, router,
                b.scheduling.scheduler, b.scheduling.pollingPolicy);
        return new Mounter(strategy, router, b.heartBeat);
    }
}
