package software.spool.mounter.api.builder;

import software.spool.core.adapter.otel.OpenTelemetryMetricsRegistry;
import software.spool.core.port.bus.EventPublisher;
import software.spool.core.port.datalake.BronzeEnvelopeRemover;
import software.spool.core.port.metrics.MetricsRegistry;
import software.spool.core.port.metrics.SpoolMetrics;
import software.spool.core.port.quarantine.QuarantineRepository;
import software.spool.mounter.api.MountMode;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedReader;
import software.spool.mounter.internal.validation.ValidatingPartitionedReader;
import software.spool.validator.api.EnvelopeValidator;
import software.spool.validator.engine.ValidatorRegistry;

import java.util.Objects;
import java.util.Optional;

/**
 * Validates on read what a {@code TRANSFORMATION} mount promotes from bronze to silver, with the validators of each
 * source, the same ones the ingester uses. Off until a quarantine is given with {@link #quarantineIn}; then the
 * bronze records it rejects are removed with the remover given in {@link #removingFromBronzeWith}.
 */
public class ValidationFacet<B> extends MounterFacet<B> {
    private QuarantineRepository quarantineRepository;
    private BronzeEnvelopeRemover bronzeEnvelopeRemover;
    private ValidatorRegistry registry;

    ValidationFacet(B parent) { super(parent); }

    /**
     * Turns validation on and sets where the records that fail a critical validator are kept.
     */
    public ValidationFacet<B> quarantineIn(QuarantineRepository quarantineRepository) {
        this.quarantineRepository = quarantineRepository;
        return this;
    }

    /**
     * Sets where the rejected records are removed from, which must be the bronze layer the mount reads. Required when
     * validation is on.
     */
    public ValidationFacet<B> removingFromBronzeWith(BronzeEnvelopeRemover bronzeEnvelopeRemover) {
        this.bronzeEnvelopeRemover = bronzeEnvelopeRemover;
        return this;
    }

    /**
     * Replaces the validators found on the class path. Meant for tests.
     */
    public ValidationFacet<B> validatingWith(ValidatorRegistry registry) {
        this.registry = registry;
        return this;
    }

    PartitionedReader readerFor(PartitionedReader reader, MountTarget target, EventPublisher publisher) {
        if (quarantineRepository == null) return reader;
        if (target.mode() != MountMode.TRANSFORMATION) {
            throw new IllegalStateException("Validation on read only applies to TRANSFORMATION mounts, from bronze to silver; this mount is "
                    + target.mode());
        }
        Objects.requireNonNull(publisher, "EventPublisher required to validate on read; set it with scheduling().emittingWith(...)");
        Objects.requireNonNull(bronzeEnvelopeRemover, "BronzeEnvelopeRemover required to validate on read; set it with validation().removingFromBronzeWith(...)");
        MetricsRegistry metrics = new OpenTelemetryMetricsRegistry();
        return new ValidatingPartitionedReader(reader,
                new EnvelopeValidator(Optional.ofNullable(registry).orElseGet(ValidatorRegistry::new)),
                quarantineRepository,
                bronzeEnvelopeRemover,
                publisher,
                metrics.counter(SpoolMetrics.Validation.RECORDS_REJECTED_TOTAL, SpoolMetrics.Validation.RECORDS_REJECTED_TOTAL_DESC, "1"));
    }
}
