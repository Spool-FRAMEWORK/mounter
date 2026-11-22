package software.spool.mounter.internal.quarantine;

import software.spool.core.model.quarantine.QuarantineReason;
import software.spool.mounter.api.exception.MissingPartitionAttributeException;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedRecord;
import software.spool.mounter.internal.control.steps.MissingPartitionAttributeHandler;

import java.util.List;

/**
 * Sends to the quarantine a mount result that lacks a partition attribute, as a result of the data mart of its target.
 */
public class QuarantiningMountResultHandler implements MissingPartitionAttributeHandler {
    private final MountResultQuarantine quarantine;

    public QuarantiningMountResultHandler(MountResultQuarantine quarantine) {
        this.quarantine = quarantine;
    }

    @Override
    public void handle(MountTarget target, PartitionedRecord<?> result, MissingPartitionAttributeException exception) {
        quarantine.quarantine(target.qualifiedDataMart(), result, QuarantineReason.MISSING_PARTITION_ATTRIBUTE,
                List.of(exception.getMessage()));
    }
}
