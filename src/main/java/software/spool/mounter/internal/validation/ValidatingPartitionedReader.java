package software.spool.mounter.internal.validation;

import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.adapter.jackson.RecordSerializerFactory;
import software.spool.core.adapter.logging.LoggerFactory;
import software.spool.core.model.failure.EnvelopeQuarantined;
import software.spool.core.model.quarantine.QuarantineReason;
import software.spool.core.model.quarantine.QuarantinedRecord;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.EventMetadataKey;
import software.spool.core.port.bus.EventPublisher;
import software.spool.core.port.logging.Logger;
import software.spool.core.port.metrics.MetricsRegistry;
import software.spool.core.port.metrics.SpoolMetrics;
import software.spool.core.port.quarantine.QuarantineRepository;
import software.spool.mounter.api.MountMode;
import software.spool.mounter.api.model.GenericRecord;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedReader;
import software.spool.mounter.api.port.PartitionedRecord;
import software.spool.validator.api.EnvelopeValidator;
import software.spool.validator.api.ValidationResult;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Validates what a {@code TRANSFORMATION} mount reads from bronze, where each record is a whole envelope.
 *
 * <p>
 * A record that fails a critical validator does not reach the aggregator: it is kept in the quarantine with its
 * envelope and violations, {@link EnvelopeQuarantined} is published and it counts as rejected. It stays in bronze.
 * Records of other mounts pass through untouched.
 * </p>
 */
public class ValidatingPartitionedReader implements PartitionedReader {
    private static final Logger LOG = LoggerFactory.getLogger(ValidatingPartitionedReader.class);
    private static final Map<String, String> REJECTED_ATTRIBUTES =
            Map.of(SpoolMetrics.Attributes.REASON, "invalid", SpoolMetrics.Attributes.MODULE, "mounter");

    private final PartitionedReader reader;
    private final EnvelopeValidator validator;
    private final QuarantineRepository quarantineRepository;
    private final EventPublisher publisher;
    private final MetricsRegistry.CounterMetric recordsRejected;

    public ValidatingPartitionedReader(PartitionedReader reader, EnvelopeValidator validator,
                                       QuarantineRepository quarantineRepository, EventPublisher publisher,
                                       MetricsRegistry.CounterMetric recordsRejected) {
        this.reader = reader;
        this.validator = validator;
        this.quarantineRepository = quarantineRepository;
        this.publisher = publisher;
        this.recordsRejected = recordsRejected;
    }

    @Override
    public Stream<PartitionedRecord<GenericRecord>> read(MountTarget mountTarget) {
        Stream<PartitionedRecord<GenericRecord>> records = reader.read(mountTarget);
        if (mountTarget.mode() != MountMode.TRANSFORMATION) return records;
        return records.filter(this::passesValidation);
    }

    private boolean passesValidation(PartitionedRecord<GenericRecord> partitionedRecord) {
        Envelope envelope = envelopeOf(partitionedRecord.record());
        ValidationResult result = validator.validate(envelope);
        if (!result.isQuarantine()) return true;
        quarantine(envelope, result.getViolations().stream().map(Throwable::getMessage).toList());
        return false;
    }

    private void quarantine(Envelope envelope, List<String> violations) {
        quarantineRepository.save(new QuarantinedRecord(envelope, QuarantineReason.VALIDATION_FAILED, violations, Instant.now()));
        publisher.publish(EnvelopeQuarantined.builder()
                .correlationId(envelope.metadata().get(EventMetadataKey.CORRELATION_ID))
                .idempotencyKey(envelope.idempotencyKey())
                .reason(QuarantineReason.VALIDATION_FAILED)
                .violations(violations)
                .build());
        recordsRejected.add(1, REJECTED_ATTRIBUTES);
        LOG.warn("Kept {} out of silver and sent it to quarantine: {}", envelope.idempotencyKey().value(), violations);
    }

    private static Envelope envelopeOf(GenericRecord bronzeRecord) {
        return PayloadDeserializerFactory.json().as(Envelope.class)
                .deserialize(RecordSerializerFactory.record().serialize(bronzeRecord.toMap()));
    }
}
