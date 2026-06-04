package software.spool.mounter.api.builder;

import software.spool.core.port.bus.Handler;
import software.spool.core.utils.routing.ErrorRouter;
import software.spool.mounter.api.Mounter;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionKeyExtractor;
import software.spool.mounter.api.strategy.MountStrategy;
import software.spool.mounter.api.utils.MounterErrorRouter;
import software.spool.mounter.internal.control.AtomicMountHandler;
import software.spool.mounter.internal.strategy.ReactiveMountStrategy;

import java.util.Objects;

class ReactiveMounterAssembler {
    @SuppressWarnings("unchecked")
    static <I, O> Mounter assemble(ReactiveMounterBuilder<I, O> b) {
        ErrorRouter router = Objects.requireNonNullElse(b.observability.errorRouter,
                MounterErrorRouter.defaults(b.bus));
        Handler<MountTarget> handler = new AtomicMountHandler<>(
                b.reader, b.aggregator, b.mount.writer, b.bus,
                b.checkpoint.windowPolicy, b.checkpoint.checkpoint,
                (PartitionKeyExtractor<O>) b.mount.keyExtractor
        );
        MountStrategy strategy = new ReactiveMountStrategy(b.mount.target, b.bus, handler, router);
        return new Mounter(strategy, router, b.moduleHeartBeat);
    }
}
