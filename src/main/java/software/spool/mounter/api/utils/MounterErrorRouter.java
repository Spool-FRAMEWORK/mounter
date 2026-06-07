package software.spool.mounter.api.utils;

import software.spool.core.adapter.logging.LoggerFactory;
import software.spool.core.port.bus.EventPublisher;
import software.spool.core.utils.routing.ErrorRouter;

public class MounterErrorRouter {

    private MounterErrorRouter() {
    }

    public static ErrorRouter defaults(EventPublisher publisher) {
        return new ErrorRouter()
                .orElse((a, b) -> LoggerFactory.getLogger(MounterErrorRouter.class).error(a.getMessage()));
    }
}
