package software.spool.mounter.api.port;

import software.spool.core.model.vo.PartitionKey;

public record MountTarget(String dataMart, PartitionKey partitionKey) {
    public static MountTarget of(String dataMart, PartitionKey partitionKey) {
        return new MountTarget(dataMart, partitionKey);
    }

    public String toPath() {
        return dataMart + "/" + partitionKey.value().replace("::", "/");
    }
}
