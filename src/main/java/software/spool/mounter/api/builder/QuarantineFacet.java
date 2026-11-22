package software.spool.mounter.api.builder;

import software.spool.core.adapter.otel.OpenTelemetryMetricsRegistry;
import software.spool.core.port.bus.EventPublisher;
import software.spool.core.port.datalake.DataLakeEnvelopeRemover;
import software.spool.core.port.metrics.MetricsRegistry;
import software.spool.core.port.metrics.SpoolMetrics;
import software.spool.core.port.quarantine.QuarantineRepository;
import software.spool.mounter.internal.quarantine.DataLakeRecordQuarantine;
import software.spool.mounter.internal.quarantine.MountResultQuarantine;

import java.util.Objects;
import java.util.Optional;

/**
 * Sets where a mount keeps what it cannot promote: the records of bronze that fail validation on read or lack a
 * partition attribute, which go back to bronze when resolved, and the results that lack a partition attribute, which
 * go to their data mart when resolved. A record of bronze sent to the quarantine is removed from the data lake with
 * the remover given in {@link #removingFromDataLakeWith}.
 */
public class QuarantineFacet<B> extends MounterFacet<B> {
    private QuarantineRepository quarantineRepository;
    private DataLakeEnvelopeRemover dataLakeEnvelopeRemover;

    QuarantineFacet(B parent) { super(parent); }

    public QuarantineFacet<B> in(QuarantineRepository quarantineRepository) {
        this.quarantineRepository = quarantineRepository;
        return this;
    }

    /**
     * Sets where the quarantined records of bronze are removed from, which must be the data lake the mount reads.
     * Required with a quarantine on a {@code TRANSFORMATION} mount.
     */
    public QuarantineFacet<B> removingFromDataLakeWith(DataLakeEnvelopeRemover dataLakeEnvelopeRemover) {
        this.dataLakeEnvelopeRemover = dataLakeEnvelopeRemover;
        return this;
    }

    Optional<DataLakeRecordQuarantine> dataLakeQuarantineFor(EventPublisher publisher) {
        if (quarantineRepository == null) return Optional.empty();
        requirePublisher(publisher);
        Objects.requireNonNull(dataLakeEnvelopeRemover, "DataLakeEnvelopeRemover required to quarantine records; set it with quarantine().removingFromDataLakeWith(...)");
        return Optional.of(new DataLakeRecordQuarantine(quarantineRepository, dataLakeEnvelopeRemover, publisher, recordsRejected()));
    }

    Optional<MountResultQuarantine> resultQuarantineFor(EventPublisher publisher) {
        if (quarantineRepository == null) return Optional.empty();
        requirePublisher(publisher);
        return Optional.of(new MountResultQuarantine(quarantineRepository, publisher, recordsRejected()));
    }

    private static void requirePublisher(EventPublisher publisher) {
        Objects.requireNonNull(publisher, "EventPublisher required to quarantine records; set it with scheduling().emittingWith(...)");
    }

    private static MetricsRegistry.CounterMetric recordsRejected() {
        MetricsRegistry metrics = new OpenTelemetryMetricsRegistry();
        return metrics.counter(SpoolMetrics.Validation.RECORDS_REJECTED_TOTAL, SpoolMetrics.Validation.RECORDS_REJECTED_TOTAL_DESC, "1");
    }
}
