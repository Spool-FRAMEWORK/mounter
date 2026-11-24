package software.spool.mounter.api.port;

import java.util.stream.Stream;

/**
 * Writes the results of a mount to its data mart. A full mount writes all its results again in each round, so a
 * writer that keeps a list of what each mount wrote can remove, when a round ends well, what the mount wrote before
 * and no longer writes, without touching what other mounts wrote. A writer that keeps no list removes nothing.
 */
public interface DataMartWriter {
    void write(MountTarget target, Stream<PartitionedRecord<?>> result);

    /**
     * Starts a round of the mount of the target: from now on, what is written for the target, or for a part of its
     * scope, is noted as written by that mount.
     */
    default void beginRound(MountTarget target) {
    }

    /**
     * Ends well the round of the mount of the target: removes what the mount wrote in earlier rounds and did not write
     * in this one, and keeps the list of what it wrote in this one.
     */
    default void completeRound(MountTarget target) {
    }

    /**
     * Ends a round of the mount of the target that failed: removes nothing, and adds what it wrote to the list of the
     * mount, so the next round that ends well removes it if it is no longer written.
     */
    default void abandonRound(MountTarget target) {
    }
}
