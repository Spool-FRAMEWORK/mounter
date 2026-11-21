package software.spool.mounter.api.builder;

import software.spool.mounter.api.MountMode;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedReader;
import software.spool.mounter.internal.quarantine.DataLakeRecordQuarantine;
import software.spool.mounter.internal.validation.ValidatingPartitionedReader;
import software.spool.validator.api.EnvelopeValidator;
import software.spool.validator.engine.ValidatorRegistry;

import java.util.Optional;

/**
 * Validates on read what a {@code TRANSFORMATION} mount promotes from bronze to silver, with the validators of each
 * source, the same ones the ingester uses. Off until {@link #enabled()}; the records it rejects go to the quarantine
 * set in {@code quarantine()}.
 */
public class ValidationFacet<B> extends MounterFacet<B> {
    private boolean enabled;
    private ValidatorRegistry registry;

    ValidationFacet(B parent) { super(parent); }

    public ValidationFacet<B> enabled() {
        this.enabled = true;
        return this;
    }

    /**
     * Replaces the validators found on the class path. Meant for tests.
     */
    public ValidationFacet<B> validatingWith(ValidatorRegistry registry) {
        this.registry = registry;
        return this;
    }

    PartitionedReader readerFor(PartitionedReader reader, MountTarget target, Optional<DataLakeRecordQuarantine> quarantine) {
        if (!enabled) return reader;
        if (target.mode() != MountMode.TRANSFORMATION) {
            throw new IllegalStateException("Validation on read only applies to TRANSFORMATION mounts, from bronze to silver; this mount is "
                    + target.mode());
        }
        return new ValidatingPartitionedReader(reader,
                new EnvelopeValidator(Optional.ofNullable(registry).orElseGet(ValidatorRegistry::new)),
                quarantine.orElseThrow(() -> new IllegalStateException(
                        "Quarantine required to validate on read; set it with quarantine().in(...)")));
    }
}
