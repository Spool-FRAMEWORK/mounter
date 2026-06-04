package software.spool.mounter.api.port;

import software.spool.core.port.bus.Handler;

import java.util.List;

public interface PartitionDispatcher {
    void dispatch(List<MountTarget> units, Handler<MountTarget> worker);
}
