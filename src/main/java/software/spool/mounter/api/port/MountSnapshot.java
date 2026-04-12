package software.spool.mounter.api.port;

import software.spool.core.model.vo.PartitionKey;

import java.time.Instant;

public record MountSnapshot(
        long samples,
        Instant mountedAt
) {
    public static MountSnapshot of(long samples, PartitionKey partitionKey) {
        return new MountSnapshot(samples, Instant.now());
    }
}
