package software.spool.mounter.api;

import software.spool.core.model.spool.SpoolModule;
import software.spool.core.model.spool.SpoolNode;
import software.spool.core.port.health.HealthPayload;
import software.spool.core.port.watchdog.ModuleHeartBeat;
import software.spool.core.utils.polling.CancellationToken;
import software.spool.core.utils.routing.ErrorRouter;
import software.spool.mounter.api.strategy.MountStrategy;

import java.util.Objects;

public class Mounter implements SpoolModule {
    private final MountStrategy strategy;
    private volatile CancellationToken token;
    private final ErrorRouter errorRouter;
    private final ModuleHeartBeat heartBeat;

    public Mounter(MountStrategy strategy, ErrorRouter errorRouter, ModuleHeartBeat heartBeat) {
        this.strategy = strategy;
        this.errorRouter = errorRouter;
        this.heartBeat = heartBeat;
        this.token = CancellationToken.NOOP;
    }

    @Override
    public void start(SpoolNode.StartPermit permit) {
        if (token.isActive()) return;
        Objects.requireNonNull(permit);
        token = CancellationToken.create();
        try {
            heartBeat.start();
            strategy.execute(token);
        } catch (Exception e) {
            errorRouter.dispatch(e);
        }
    }

    @Override
    public void stop(SpoolNode.StartPermit permit) {
        if (!token.isActive()) return;
        Objects.requireNonNull(permit);
        token.cancel();
        heartBeat.stop();
        token = CancellationToken.NOOP;
    }

    @Override
    public HealthPayload checkHealth() {
        return token.isActive() ? HealthPayload.healthy(heartBeat.identity().moduleId()) : HealthPayload.degraded(heartBeat.identity().moduleId(), null);
    }
}
