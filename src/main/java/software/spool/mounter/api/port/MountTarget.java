package software.spool.mounter.api.port;

import software.spool.core.model.vo.PartitionKey;
import software.spool.mounter.api.MountMode;

public record MountTarget(String dataMart, PartitionKey sourceKey, MountMode mode) {
    public String qualifiedDataMart() {
        return mode.qualify(dataMart);
    }

    public static MountTarget transformation(String dataMart, PartitionKey sourceKey) {
        return new MountTarget(dataMart, sourceKey, MountMode.TRANSFORMATION);
    }

    public static MountTarget aggregation(String dataMart, PartitionKey sourceKey) {
        return new MountTarget(dataMart, sourceKey, MountMode.AGGREGATION);
    }
}