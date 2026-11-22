package software.spool.mounter.internal.quarantine;

import software.spool.core.adapter.jackson.RecordSerializerFactory;
import software.spool.core.adapter.logging.LoggerFactory;
import software.spool.core.model.EnvelopeStatus;
import software.spool.core.model.failure.EnvelopeQuarantined;
import software.spool.core.model.quarantine.QuarantineLocation;
import software.spool.core.model.quarantine.QuarantineReason;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.EventMetadata;
import software.spool.core.model.vo.EventMetadataKey;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.port.bus.EventPublisher;
import software.spool.core.port.logging.Logger;
import software.spool.core.port.metrics.MetricsRegistry;
import software.spool.core.port.quarantine.QuarantineRepository;
import software.spool.core.utils.media.MediaTypes;
import software.spool.core.utils.quarantine.QuarantineRecorder;
import software.spool.mounter.api.model.GenericRecord;
import software.spool.mounter.api.port.PartitionedRecord;

import java.time.Instant;
import java.util.List;

/**
 * Keeps out of its data mart a mount result that cannot be written there.
 *
 * <p>
 * The result is kept in the quarantine as an envelope whose payload is the result in JSON and whose key is calculated
 * from the data mart and that JSON, so the same result rejected again reopens the same record. The record says which
 * data mart the result belongs to and the partition it came with. {@link EnvelopeQuarantined} is published and it
 * counts as rejected.
 * </p>
 */
public class MountResultQuarantine {
    private static final Logger LOG = LoggerFactory.getLogger(MountResultQuarantine.class);

    private final QuarantineRecorder recorder;
    private final EventPublisher publisher;
    private final MetricsRegistry.CounterMetric recordsRejected;

    public MountResultQuarantine(QuarantineRepository quarantineRepository, EventPublisher publisher,
                                 MetricsRegistry.CounterMetric recordsRejected) {
        this.recorder = new QuarantineRecorder(quarantineRepository);
        this.publisher = publisher;
        this.recordsRejected = recordsRejected;
    }

    /**
     * @param dataMart the data mart the result belongs to, named with its layer ({@code gold/payments})
     */
    public void quarantine(String dataMart, PartitionedRecord<?> result, QuarantineReason reason, List<String> violations) {
        byte[] content = RecordSerializerFactory.record().serialize(contentOf(result.record()));
        IdempotencyKey idempotencyKey = IdempotencyKey.of(dataMart, content);
        Instant now = Instant.now();
        Envelope envelope = new Envelope(idempotencyKey, new EventMetadata()
                .set(EventMetadataKey.SOURCE, dataMart)
                .set(EventMetadataKey.CORRELATION_ID, idempotencyKey.value()),
                MediaTypes.JSON, content, EnvelopeStatus.QUARANTINED, 0, now, now);
        QuarantineLocation location = QuarantineLocation.dataMart(dataMart, result.partitionKey());
        recorder.record(envelope, location, reason, violations, now);
        publisher.publish(EnvelopeQuarantined.builder()
                .correlationId(idempotencyKey.value())
                .idempotencyKey(idempotencyKey)
                .reason(reason)
                .violations(violations)
                .location(location)
                .build());
        recordsRejected.add(1, RejectionMetricAttributes.of(reason));
        LOG.warn("Kept a result out of {} and sent it to quarantine as {}: {}", dataMart, idempotencyKey.value(), violations);
    }

    private static Object contentOf(Object result) {
        return result instanceof GenericRecord genericRecord ? genericRecord.toMap() : result;
    }
}
