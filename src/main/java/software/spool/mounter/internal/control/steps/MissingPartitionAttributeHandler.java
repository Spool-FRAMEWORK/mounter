package software.spool.mounter.internal.control.steps;

import software.spool.mounter.api.exception.MissingPartitionAttributeException;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedRecord;

@FunctionalInterface
public interface MissingPartitionAttributeHandler {
    void handle(MountTarget target, PartitionedRecord<?> result, MissingPartitionAttributeException exception);
}
