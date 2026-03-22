package software.spool.mounter.api.utils;

import software.spool.core.port.EventBusEmitter;
import software.spool.core.utils.ErrorRouter;

public class MounterErrorRouter {

    private MounterErrorRouter() {
    }

    public static ErrorRouter defaults(EventBusEmitter bus) {
        return new ErrorRouter();
    }
}
