package software.spool.mounter.internal.quarantine;

import software.spool.core.model.quarantine.QuarantineReason;
import software.spool.core.port.metrics.SpoolMetrics;

import java.util.Map;

final class RejectionMetricAttributes {
    private static final String MODULE = "mounter";

    private RejectionMetricAttributes() {
    }

    static Map<String, String> of(QuarantineReason reason) {
        return Map.of(SpoolMetrics.Attributes.REASON, reasonOf(reason), SpoolMetrics.Attributes.MODULE, MODULE);
    }

    private static String reasonOf(QuarantineReason reason) {
        return switch (reason) {
            case VALIDATION_FAILED -> "invalid";
            case MISSING_PARTITION_ATTRIBUTE -> "missing-partition-attribute";
            case RETRIES_EXHAUSTED -> "retries-exhausted";
        };
    }
}
