package software.spool.mounter.internal.strategy;

import software.spool.core.model.Event;
import software.spool.core.model.event.EnvelopePersisted;
import software.spool.core.model.event.EnvelopesMounted;
import software.spool.core.port.bus.EventBus;
import software.spool.core.port.bus.Handler;
import software.spool.core.port.bus.Subscription;
import software.spool.core.utils.polling.CancellationToken;
import software.spool.core.utils.routing.ErrorRouter;
import software.spool.mounter.api.MountMode;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.strategy.MountStrategy;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

/**
 * Mounts when what the mount reads has just been written: a {@code TRANSFORMATION} mount when an envelope is persisted
 * in bronze, and an {@code AGGREGATION} mount when a partition of silver that holds its scope is mounted. Once the
 * mounter stops, it mounts nothing more and drops its subscription with the next event.
 */
public class ReactiveMountStrategy extends BaseMountStrategy implements MountStrategy {
    private final EventBus bus;
    private final MountTarget target;

    public ReactiveMountStrategy(MountTarget target, EventBus eventBus, Handler<MountTarget> handler, ErrorRouter errorRouter) {
        super(handler, errorRouter);
        this.target = target;
        this.bus = eventBus;
    }

    @Override
    public void execute(CancellationToken token) {
        if (target.mode() == MountMode.TRANSFORMATION) {
            mountOn(EnvelopePersisted.class, persisted -> true, token);
        } else {
            mountOn(EnvelopesMounted.class, this::isWhatTheMountReads, token);
        }
    }

    private <E extends Event> void mountOn(Class<E> eventType, Predicate<E> relevant, CancellationToken token) {
        AtomicReference<Subscription> subscription = new AtomicReference<>();
        subscription.set(bus.subscribe(eventType, event -> {
            if (token.isCancelled()) {
                subscription.get().cancel();
            } else if (relevant.test(event)) {
                mountSafely(target);
            }
        }));
    }

    private boolean isWhatTheMountReads(EnvelopesMounted mounted) {
        return MountMode.TRANSFORMATION.writesTo(mounted.dataMart()) && mounted.partitionKey().contains(target.sourceKey());
    }
}
