package software.spool.mounter.api.port;

import software.spool.core.model.vo.PartitionKey;
import software.spool.mounter.api.MountMode;

public record MountTarget(String dataMart, PartitionKey sourceKey, MountMode mode, ExtensionResolver extensionResolver) {
    public MountTarget withSourceKey(PartitionKey key) {
        return new MountTarget(dataMart, key, mode, extensionResolver);
    }

    public String qualifiedDataMart() {
        return mode.qualify(dataMart);
    }

    public String resolveExtension(byte[] content) {
        return extensionResolver == null ? "json" : extensionResolver.resolve(content);
    }

    public static MountTarget transformation(String dataMart, PartitionKey sourceKey) {
        return new MountTarget(dataMart, sourceKey, MountMode.TRANSFORMATION, null);
    }

    public static MountTarget transformation(String dataMart, PartitionKey sourceKey, String extension) {
        return new MountTarget(dataMart, sourceKey, MountMode.TRANSFORMATION, ExtensionResolver.fixed(extension));
    }

    public static MountTarget transformation(String dataMart, PartitionKey sourceKey, ExtensionResolver resolver) {
        return new MountTarget(dataMart, sourceKey, MountMode.TRANSFORMATION, resolver);
    }

    public static MountTarget aggregation(String dataMart, PartitionKey sourceKey) {
        return new MountTarget(dataMart, sourceKey, MountMode.AGGREGATION, null);
    }

    public static MountTarget aggregation(String dataMart, PartitionKey sourceKey, String extension) {
        return new MountTarget(dataMart, sourceKey, MountMode.AGGREGATION, ExtensionResolver.fixed(extension));
    }

    public static MountTarget aggregation(String dataMart, PartitionKey sourceKey, ExtensionResolver resolver) {
        return new MountTarget(dataMart, sourceKey, MountMode.AGGREGATION, resolver);
    }
}