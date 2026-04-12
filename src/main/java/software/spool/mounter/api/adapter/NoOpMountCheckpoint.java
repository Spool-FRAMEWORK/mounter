package software.spool.mounter.api.adapter;

import software.spool.mounter.api.port.MountCheckpoint;
import software.spool.mounter.api.port.MountTarget;

public class NoOpMountCheckpoint implements MountCheckpoint {
    public boolean isMounted(MountTarget target) { return false; }
    public void markMounted(MountTarget target) { }
    public void markFailed(MountTarget target, Exception e) { }
}