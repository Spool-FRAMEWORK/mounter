package software.spool.mounter.api.port;

import java.util.stream.Stream;

/**
 * Writes the results of a mount to its data mart. A writer that notes what it writes can also leave each partition it
 * wrote with only that, so a full mount does not keep the results of earlier rounds that it no longer produces.
 */
public interface DataMartWriter {
    void write(MountTarget target, Stream<PartitionedRecord<?>> result);

    /**
     * Removes from every partition written for the target since the last call to this method or to
     * {@link #forgetWhatWasWritten} the results that were not written in that time, and forgets what was written. A
     * writer that does not note what it writes keeps everything.
     */
    default void keepOnlyWhatWasWritten(MountTarget target) {
    }

    /** Forgets what was written for the target since the last call, without removing anything. */
    default void forgetWhatWasWritten(MountTarget target) {
    }
}
