package software.spool.mounter.api.port;

public interface MountCheckpoint {
    boolean isMounted(MountTarget target);
    void markMounted(MountTarget target);
    void markFailed(MountTarget target, Exception exception);
}
