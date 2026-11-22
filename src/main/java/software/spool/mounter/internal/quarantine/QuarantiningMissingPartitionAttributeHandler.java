package software.spool.mounter.internal.quarantine;

import software.spool.core.exception.DeserializationException;
import software.spool.core.model.quarantine.QuarantineReason;
import software.spool.core.model.vo.Envelope;
import software.spool.mounter.api.exception.MissingPartitionAttributeException;
import software.spool.mounter.api.model.GenericRecord;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedRecord;
import software.spool.mounter.internal.control.steps.MissingPartitionAttributeHandler;

import java.util.List;
import java.util.Optional;

/**
 * Sends to the quarantine a silver record that lacks a partition attribute. Only a record that is still the envelope
 * read from the data lake can be quarantined; the others are left to the fallback.
 */
public class QuarantiningMissingPartitionAttributeHandler implements MissingPartitionAttributeHandler {
    private final DataLakeRecordQuarantine quarantine;
    private final MissingPartitionAttributeHandler fallback;

    public QuarantiningMissingPartitionAttributeHandler(DataLakeRecordQuarantine quarantine, MissingPartitionAttributeHandler fallback) {
        this.quarantine = quarantine;
        this.fallback = fallback;
    }

    @Override
    public void handle(MountTarget target, PartitionedRecord<?> result, MissingPartitionAttributeException exception) {
        envelopeOf(result).ifPresentOrElse(
                envelope -> quarantine.quarantine(envelope, result.partitionKey(), QuarantineReason.MISSING_PARTITION_ATTRIBUTE,
                        List.of(exception.getMessage())),
                () -> fallback.handle(target, result, exception));
    }

    private static Optional<Envelope> envelopeOf(PartitionedRecord<?> result) {
        if (!(result.record() instanceof GenericRecord dataLakeRecord)) return Optional.empty();
        try {
            return Optional.of(DataLakeRecordQuarantine.envelopeOf(dataLakeRecord));
        } catch (DeserializationException e) {
            return Optional.empty();
        }
    }
}
