package software.spool.mounter.api.adapter;

import software.spool.core.exception.SpoolException;
import software.spool.core.port.bus.Handler;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionDispatcher;

import java.util.List;

public class SequentialPartitionDispatcher implements PartitionDispatcher {
    @Override
    public void dispatch(List<MountTarget> units, Handler<MountTarget> worker) {
        for (MountTarget unit : units) {
            try {
                worker.handle(unit);
            } catch (SpoolException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
