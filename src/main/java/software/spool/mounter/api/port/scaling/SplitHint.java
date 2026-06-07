package software.spool.mounter.api.port.scaling;

public record SplitHint(long targetRecordsPerSlice) {
    public static SplitHint of(long targetRecordsPerSlice) {
        return new SplitHint(targetRecordsPerSlice);
    }
}
