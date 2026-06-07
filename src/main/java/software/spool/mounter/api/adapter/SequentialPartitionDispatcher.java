package software.spool.mounter.api.adapter;

import software.spool.core.port.bus.Handler;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.scaling.PartitionDispatcher;

import java.util.List;

public class SequentialPartitionDispatcher implements PartitionDispatcher {
    @Override
    public void dispatch(List<MountTarget> units, Handler<MountTarget> worker) {
        for (MountTarget unit : units) {
            worker.handle(unit);
        }
    }
}
