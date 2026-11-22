package software.spool.mounter.internal.validation;

import software.spool.core.model.quarantine.QuarantineReason;
import software.spool.core.model.vo.Envelope;
import software.spool.mounter.api.MountMode;
import software.spool.mounter.api.model.GenericRecord;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedReader;
import software.spool.mounter.api.port.PartitionedRecord;
import software.spool.mounter.internal.quarantine.DataLakeRecordQuarantine;
import software.spool.validator.api.EnvelopeValidator;
import software.spool.validator.api.ValidationResult;

import java.util.stream.Stream;

/**
 * Validates what a {@code TRANSFORMATION} mount reads from bronze, where each record is a whole envelope.
 *
 * <p>
 * A record that fails a critical validator does not reach the aggregator: it goes to the
 * {@link DataLakeRecordQuarantine} with its violations. Records of other mounts pass through untouched.
 * </p>
 */
public class ValidatingPartitionedReader implements PartitionedReader {
    private final PartitionedReader reader;
    private final EnvelopeValidator validator;
    private final DataLakeRecordQuarantine quarantine;

    public ValidatingPartitionedReader(PartitionedReader reader, EnvelopeValidator validator, DataLakeRecordQuarantine quarantine) {
        this.reader = reader;
        this.validator = validator;
        this.quarantine = quarantine;
    }

    @Override
    public Stream<PartitionedRecord<GenericRecord>> read(MountTarget mountTarget) {
        Stream<PartitionedRecord<GenericRecord>> records = reader.read(mountTarget);
        if (mountTarget.mode() != MountMode.TRANSFORMATION) return records;
        return records.filter(this::passesValidation);
    }

    private boolean passesValidation(PartitionedRecord<GenericRecord> partitionedRecord) {
        Envelope envelope = DataLakeRecordQuarantine.envelopeOf(partitionedRecord.record());
        ValidationResult result = validator.validate(envelope);
        if (!result.isQuarantine()) return true;
        quarantine.quarantine(envelope, partitionedRecord.partitionKey(), QuarantineReason.VALIDATION_FAILED,
                result.getViolations().stream().map(Throwable::getMessage).toList());
        return false;
    }
}
