package software.spool.mounter.api.port;

public record SplitHint(long targetRecordsPerSlice) {
    public static SplitHint of(long targetRecordsPerSlice) {
        return new SplitHint(targetRecordsPerSlice);
    }
}
