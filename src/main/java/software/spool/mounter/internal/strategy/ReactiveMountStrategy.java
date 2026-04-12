package software.spool.mounter.internal.strategy;

import software.spool.core.model.event.ItemPersisted;
import software.spool.core.port.bus.EventBus;
import software.spool.core.port.bus.Handler;
import software.spool.core.utils.polling.CancellationToken;
import software.spool.core.utils.routing.ErrorRouter;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.strategy.MountStrategy;

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
        bus.on(ItemPersisted.class, i -> mountSafely(target));
    }
}
