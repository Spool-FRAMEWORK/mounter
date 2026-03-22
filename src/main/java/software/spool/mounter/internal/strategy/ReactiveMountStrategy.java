package software.spool.mounter.internal.strategy;

import software.spool.core.control.Handler;
import software.spool.core.model.ItemPersisted;
import software.spool.core.model.PartitionKey;
import software.spool.core.port.EventBus;
import software.spool.core.utils.CancellationToken;
import software.spool.core.utils.ErrorRouter;
import software.spool.mounter.api.strategy.MountStrategy;

public class ReactiveMountStrategy extends BaseMountStrategy implements MountStrategy {
    private final EventBus bus;

    public ReactiveMountStrategy(EventBus eventBus, Handler<PartitionKey> handler, ErrorRouter errorRouter) {
        super(handler, errorRouter);
        this.bus = eventBus;
    }

    @Override
    public void execute(CancellationToken token) {
            bus.on(ItemPersisted.class, e -> mountSafely(e.partitionKey()));
    }
}
