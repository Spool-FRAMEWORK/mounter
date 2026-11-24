package software.spool.mounter.internal.control;

import software.spool.core.exception.SpoolException;
import software.spool.core.port.bus.Handler;
import software.spool.mounter.api.port.DataMartWriter;
import software.spool.mounter.api.port.MountTarget;

/**
 * Mounts a unit with another handler and then makes the writer forget what it wrote, without removing anything. An
 * incremental mount replaces its results on its own, so what its writer notes is not needed after the unit.
 */
public class WrittenResultsForgettingMountUnitHandler implements Handler<MountTarget> {
    private final Handler<MountTarget> handler;
    private final DataMartWriter writer;

    public WrittenResultsForgettingMountUnitHandler(Handler<MountTarget> handler, DataMartWriter writer) {
        this.handler = handler;
        this.writer = writer;
    }

    @Override
    public void handle(MountTarget target) throws SpoolException {
        try {
            handler.handle(target);
        } finally {
            writer.forgetWhatWasWritten(target);
        }
    }
}
