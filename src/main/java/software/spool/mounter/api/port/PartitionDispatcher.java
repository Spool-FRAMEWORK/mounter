package software.spool.mounter.api.port;

import software.spool.core.model.vo.PartitionKey;
import software.spool.core.port.bus.Handler;

import java.util.List;

public interface PartitionDispatcher {
    void dispatch(List<PartitionKey> partitions, MountTarget scope, Handler<MountTarget> worker);
}
