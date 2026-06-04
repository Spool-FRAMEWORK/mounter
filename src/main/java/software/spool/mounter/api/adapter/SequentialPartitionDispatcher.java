package software.spool.mounter.api.adapter;

import software.spool.core.exception.SpoolException;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.port.bus.Handler;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionDispatcher;

import java.util.List;

public class SequentialPartitionDispatcher implements PartitionDispatcher {
    @Override
    public void dispatch(List<PartitionKey> partitions, MountTarget scope, Handler<MountTarget> worker) {
        for (PartitionKey key : partitions) {
            try {
                worker.handle(scope.withSourceKey(key));
            } catch (SpoolException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
