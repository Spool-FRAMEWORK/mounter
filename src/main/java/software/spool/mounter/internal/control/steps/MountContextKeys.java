package software.spool.mounter.internal.control.steps;

import software.spool.core.pipeline.ContextKey;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.scaling.PartitionInfo;
import software.spool.mounter.api.port.scaling.ScalingPlan;

import java.util.List;

public final class MountContextKeys {
    public static final ContextKey<MountTarget> TARGET = ContextKey.of("mount.target");
    public static final ContextKey<List<PartitionInfo>> DISCOVERED = ContextKey.of("mount.discovered");
    public static final ContextKey<ScalingPlan> PLAN = ContextKey.of("mount.plan");
    public static final ContextKey<List<MountTarget>> UNITS = ContextKey.of("mount.units");
}
