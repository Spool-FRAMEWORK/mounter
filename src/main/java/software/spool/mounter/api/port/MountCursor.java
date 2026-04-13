package software.spool.mounter.api.port;

import software.spool.core.model.vo.PartitionKey;

import java.util.Set;

public interface MountCursor {
    Set<PartitionKey> processedSources(MountTarget target);
    void advance(MountTarget target, PartitionKey sourceKey);
}