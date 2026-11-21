package software.spool.mounter.internal.validation;

import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.adapter.jackson.RecordSerializerFactory;
import software.spool.core.adapter.logging.LoggerFactory;
import software.spool.core.exception.BronzeRemovalException;
import software.spool.core.model.failure.EnvelopeQuarantined;
import software.spool.core.model.quarantine.QuarantineLocation;
import software.spool.core.model.quarantine.QuarantineReason;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.EventMetadataKey;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.port.bus.EventPublisher;
import software.spool.core.port.datalake.BronzeEnvelopeRemover;
import software.spool.core.port.logging.Logger;
import software.spool.core.port.metrics.MetricsRegistry;
import software.spool.core.port.metrics.SpoolMetrics;
import software.spool.core.port.quarantine.QuarantineRepository;
import software.spool.core.utils.quarantine.QuarantineRecorder;
import software.spool.mounter.api.MountMode;
import software.spool.mounter.api.model.GenericRecord;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedReader;
import software.spool.mounter.api.port.PartitionedRecord;
import software.spool.validator.api.EnvelopeValidator;
import software.spool.validator.api.ValidationResult;

import java.time.Instant;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Validates what a {@code TRANSFORMATION} mount reads from bronze, where each record is a whole envelope.
 *
 * <p>
 * A record that fails a critical validator does not reach the aggregator: it is kept in the quarantine with its
 * envelope, its bronze partition and its violations, then removed from bronze so the next mount does not read it
 * again. {@link EnvelopeQuarantined} is published and it counts as rejected. Records of other mounts pass through
 * untouched.
 * </p>
 */
public class ValidatingPartitionedReader implements PartitionedReader {
    private static final Logger LOG = LoggerFactory.getLogger(ValidatingPartitionedReader.class);
    private static final String PAYLOAD = "payload";
    private static final Map<String, String> REJECTED_ATTRIBUTES =
            Map.of(SpoolMetrics.Attributes.REASON, "invalid", SpoolMetrics.Attributes.MODULE, "mounter");

    private final PartitionedReader reader;
    private final EnvelopeValidator validator;
    private final QuarantineRecorder recorder;
    private final BronzeEnvelopeRemover bronzeEnvelopeRemover;
    private final EventPublisher publisher;
    private final MetricsRegistry.CounterMetric recordsRejected;

    public ValidatingPartitionedReader(PartitionedReader reader, EnvelopeValidator validator,
                                       QuarantineRepository quarantineRepository, BronzeEnvelopeRemover bronzeEnvelopeRemover,
                                       EventPublisher publisher, MetricsRegistry.CounterMetric recordsRejected) {
        this.reader = reader;
        this.validator = validator;
        this.recorder = new QuarantineRecorder(quarantineRepository);
        this.bronzeEnvelopeRemover = bronzeEnvelopeRemover;
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
        quarantine(envelope, partitionedRecord.partitionKey(), result.getViolations().stream().map(Throwable::getMessage).toList());
        return false;
    }

    private void quarantine(Envelope envelope, PartitionKey partition, List<String> violations) {
        QuarantineLocation location = QuarantineLocation.bronze(partition);
        recorder.record(envelope, location, QuarantineReason.VALIDATION_FAILED, violations, Instant.now());
        removeFromBronze(envelope, partition);
        publisher.publish(EnvelopeQuarantined.builder()
                .correlationId(envelope.metadata().get(EventMetadataKey.CORRELATION_ID))
                .idempotencyKey(envelope.idempotencyKey())
                .reason(QuarantineReason.VALIDATION_FAILED)
                .violations(violations)
                .location(location)
                .build());
        recordsRejected.add(1, REJECTED_ATTRIBUTES);
        LOG.warn("Kept {} out of silver and sent it to quarantine: {}", envelope.idempotencyKey().value(), violations);
    }

    private void removeFromBronze(Envelope envelope, PartitionKey partition) {
        try {
            bronzeEnvelopeRemover.remove(partition, envelope.idempotencyKey());
        } catch (BronzeRemovalException e) {
            LOG.error("Envelope {} stays in bronze partition {} although it is in quarantine: {}",
                    envelope.idempotencyKey().value(), partition.value(), e.getMessage());
        }
    }

    private static Envelope envelopeOf(GenericRecord bronzeRecord) {
        Map<String, Object> fields = new LinkedHashMap<>(bronzeRecord.toMap());
        fields.computeIfPresent(PAYLOAD, (key, payload) -> payload instanceof String encoded ? encoded : encoded(payload));
        return PayloadDeserializerFactory.json().as(Envelope.class)
                .deserialize(RecordSerializerFactory.record().serialize(fields));
    }

    private static String encoded(Object decodedPayload) {
        return Base64.getEncoder().encodeToString(RecordSerializerFactory.record().serialize(decodedPayload));
    }
}
