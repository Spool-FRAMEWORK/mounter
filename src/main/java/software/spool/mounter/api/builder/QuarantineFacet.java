package software.spool.mounter.api.builder;

import software.spool.core.adapter.otel.OpenTelemetryMetricsRegistry;
import software.spool.core.port.bus.EventPublisher;
import software.spool.core.port.datalake.DataLakeEnvelopeRemover;
import software.spool.core.port.metrics.MetricsRegistry;
import software.spool.core.port.metrics.SpoolMetrics;
import software.spool.core.port.quarantine.QuarantineRepository;
import software.spool.mounter.internal.quarantine.DataLakeRecordQuarantine;

import java.util.Objects;
import java.util.Optional;

/**
 * Sets where a {@code TRANSFORMATION} mount keeps the records it does not promote from bronze to silver: those that
 * fail validation on read, and those that lack a partition attribute. A record sent to the quarantine is removed from
 * the data lake with the remover given in {@link #removingFromDataLakeWith}.
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
     * Sets where the quarantined records are removed from, which must be the data lake the mount reads. Required with
     * a quarantine.
     */
    public QuarantineFacet<B> removingFromDataLakeWith(DataLakeEnvelopeRemover dataLakeEnvelopeRemover) {
        this.dataLakeEnvelopeRemover = dataLakeEnvelopeRemover;
        return this;
    }

    Optional<DataLakeRecordQuarantine> quarantineFor(EventPublisher publisher) {
        if (quarantineRepository == null) return Optional.empty();
        Objects.requireNonNull(publisher, "EventPublisher required to quarantine records; set it with scheduling().emittingWith(...)");
        Objects.requireNonNull(dataLakeEnvelopeRemover, "DataLakeEnvelopeRemover required to quarantine records; set it with quarantine().removingFromDataLakeWith(...)");
        MetricsRegistry metrics = new OpenTelemetryMetricsRegistry();
        return Optional.of(new DataLakeRecordQuarantine(quarantineRepository, dataLakeEnvelopeRemover, publisher,
                metrics.counter(SpoolMetrics.Validation.RECORDS_REJECTED_TOTAL, SpoolMetrics.Validation.RECORDS_REJECTED_TOTAL_DESC, "1")));
    }
}
