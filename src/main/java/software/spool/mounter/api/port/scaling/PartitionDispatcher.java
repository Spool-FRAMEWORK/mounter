package software.spool.mounter.api.port.scaling;

import software.spool.core.port.bus.Handler;
import software.spool.mounter.api.port.MountTarget;

import java.util.List;

public interface PartitionDispatcher {
    void dispatch(List<MountTarget> units, Handler<MountTarget> worker);
}
