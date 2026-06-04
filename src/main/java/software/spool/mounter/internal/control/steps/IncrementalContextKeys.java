package software.spool.mounter.internal.control.steps;

import software.spool.core.model.vo.PartitionKey;
import software.spool.core.pipeline.ContextKey;
import software.spool.mounter.api.port.MountTarget;

import java.util.List;
import java.util.Optional;

public final class IncrementalContextKeys {
    public static final ContextKey<MountTarget> TARGET = ContextKey.of("incremental.target");
    public static final ContextKey<List<PartitionKey>> CLOSED_PENDING = ContextKey.of("incremental.closed_pending");
    public static final ContextKey<Optional<?>> CURRENT = ContextKey.of("incremental.current");
}
