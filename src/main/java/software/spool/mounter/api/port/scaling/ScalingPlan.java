package software.spool.mounter.api.port.scaling;

public record ScalingPlan(ExecutionMode mode, SplitHint splitHint) {
    public static ScalingPlan sequential() {
        return new ScalingPlan(ExecutionMode.SEQUENTIAL, null);
    }

    public static ScalingPlan localParallel(long targetRecordsPerSlice) {
        return new ScalingPlan(ExecutionMode.LOCAL_PARALLEL, SplitHint.of(targetRecordsPerSlice));
    }

    public static ScalingPlan distributed(long targetRecordsPerSlice) {
        return new ScalingPlan(ExecutionMode.DISTRIBUTED, SplitHint.of(targetRecordsPerSlice));
    }
}
