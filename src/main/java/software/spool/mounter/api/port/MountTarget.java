package software.spool.mounter.api.port;

import software.spool.core.model.vo.PartitionKey;

public record MountTarget(
        String dataMart,
        PartitionKey sourceKey) {

    public static MountTarget of(String dataMart, PartitionKey sourceKey) {
        return new MountTarget(dataMart, sourceKey);
    }
}