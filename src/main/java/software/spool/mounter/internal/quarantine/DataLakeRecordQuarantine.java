package software.spool.mounter.internal.quarantine;

import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.adapter.jackson.RecordSerializerFactory;
import software.spool.core.adapter.logging.LoggerFactory;
import software.spool.core.exception.DataLakeRemovalException;
import software.spool.core.model.failure.EnvelopeQuarantined;
import software.spool.core.model.quarantine.QuarantineLocation;
import software.spool.core.model.quarantine.QuarantineReason;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.EventMetadataKey;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.port.bus.EventPublisher;
import software.spool.core.port.datalake.DataLakeEnvelopeRemover;
import software.spool.core.port.logging.Logger;
import software.spool.core.port.metrics.MetricsRegistry;
import software.spool.core.port.metrics.SpoolMetrics;
import software.spool.core.port.quarantine.QuarantineRepository;
import software.spool.core.utils.quarantine.QuarantineRecorder;
import software.spool.mounter.api.model.GenericRecord;

import java.time.Instant;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Keeps out of silver a record read from the data lake, where each record is a whole envelope.
 *
 * <p>
 * The envelope is kept in the quarantine with its data lake partition and its violations, then removed from the data
 * lake so the next mount does not read it again. {@link EnvelopeQuarantined} is published and it counts as rejected.
 * </p>
 */
public class DataLakeRecordQuarantine {
    private static final Logger LOG = LoggerFactory.getLogger(DataLakeRecordQuarantine.class);
    private static final String PAYLOAD = "payload";
    private static final Map<String, String> REJECTED_ATTRIBUTES =
            Map.of(SpoolMetrics.Attributes.REASON, "invalid", SpoolMetrics.Attributes.MODULE, "mounter");

    private final QuarantineRecorder recorder;
    private final DataLakeEnvelopeRemover dataLakeEnvelopeRemover;
    private final EventPublisher publisher;
    private final MetricsRegistry.CounterMetric recordsRejected;

    public DataLakeRecordQuarantine(QuarantineRepository quarantineRepository, DataLakeEnvelopeRemover dataLakeEnvelopeRemover,
                                    EventPublisher publisher, MetricsRegistry.CounterMetric recordsRejected) {
        this.recorder = new QuarantineRecorder(quarantineRepository);
        this.dataLakeEnvelopeRemover = dataLakeEnvelopeRemover;
        this.publisher = publisher;
        this.recordsRejected = recordsRejected;
    }

    public static Envelope envelopeOf(GenericRecord dataLakeRecord) {
        Map<String, Object> fields = new LinkedHashMap<>(dataLakeRecord.toMap());
        fields.computeIfPresent(PAYLOAD, (key, payload) -> payload instanceof String encoded ? encoded : encoded(payload));
        return PayloadDeserializerFactory.json().as(Envelope.class)
                .deserialize(RecordSerializerFactory.record().serialize(fields));
    }

    public void quarantine(Envelope envelope, PartitionKey partition, List<String> violations) {
        QuarantineLocation location = QuarantineLocation.bronze(partition);
        recorder.record(envelope, location, QuarantineReason.VALIDATION_FAILED, violations, Instant.now());
        removeFromDataLake(envelope, partition);
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

    private void removeFromDataLake(Envelope envelope, PartitionKey partition) {
        try {
            dataLakeEnvelopeRemover.remove(partition, envelope.idempotencyKey());
        } catch (DataLakeRemovalException e) {
            LOG.error("Envelope {} stays in data lake partition {} although it is in quarantine: {}",
                    envelope.idempotencyKey().value(), partition.value(), e.getMessage());
        }
    }

    private static String encoded(Object decodedPayload) {
        byte[] bytes = decodedPayload instanceof byte[] raw ? raw : RecordSerializerFactory.record().serialize(decodedPayload);
        return Base64.getEncoder().encodeToString(bytes);
    }
}
