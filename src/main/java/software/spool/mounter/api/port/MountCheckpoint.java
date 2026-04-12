package software.spool.mounter.api.port;

import software.spool.core.model.vo.PartitionKey;

public interface MountCheckpoint {
    boolean isMounted(MountTarget target);
    void markMounted(MountTarget target);
    void markFailed(MountTarget target, Exception exception);
}
