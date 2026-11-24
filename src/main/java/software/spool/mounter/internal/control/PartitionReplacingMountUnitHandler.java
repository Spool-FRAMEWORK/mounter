package software.spool.mounter.internal.control;

import software.spool.core.exception.SpoolException;
import software.spool.core.port.bus.Handler;
import software.spool.mounter.api.port.DataMartWriter;
import software.spool.mounter.api.port.MountTarget;

/**
 * Mounts a unit of a full mount with another handler and then leaves each partition it wrote with only what it wrote,
 * so the results of earlier rounds that the mount no longer produces do not stay in the data mart. A unit that fails
 * removes nothing.
 */
public class PartitionReplacingMountUnitHandler implements Handler<MountTarget> {
    private final Handler<MountTarget> handler;
    private final DataMartWriter writer;

    public PartitionReplacingMountUnitHandler(Handler<MountTarget> handler, DataMartWriter writer) {
        this.handler = handler;
        this.writer = writer;
    }

    @Override
    public void handle(MountTarget target) throws SpoolException {
        try {
            handler.handle(target);
        } catch (RuntimeException e) {
            writer.forgetWhatWasWritten(target);
            throw e;
        }
        writer.keepOnlyWhatWasWritten(target);
    }
}
