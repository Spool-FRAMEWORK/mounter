package software.spool.mounter.api.adapter;

import software.spool.core.model.vo.PartitionKey;
import software.spool.mounter.api.port.MountCursor;
import software.spool.mounter.api.port.MountTarget;

import java.util.Set;

public class NoOpMountCursor implements MountCursor {
    @Override
    public Set<PartitionKey> processedSources(MountTarget target) {
        return Set.of();
    }

    @Override
    public void advance(MountTarget target, PartitionKey sourceKey) {
        System.out.println("Advancing " + sourceKey + " to " + target);
    }
}
