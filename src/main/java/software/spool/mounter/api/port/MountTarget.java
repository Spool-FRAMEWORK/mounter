package software.spool.mounter.api.port;

import software.spool.core.model.vo.PartitionKey;
import software.spool.mounter.api.MountMode;

public record MountTarget(String dataMart, PartitionKey sourceKey, MountMode mode, ExtensionResolver extensionResolver, PartitionSlice slice) {
    public MountTarget withSourceKey(PartitionKey key) {
        return new MountTarget(dataMart, key, mode, extensionResolver, slice);
    }

    public MountTarget withSlice(PartitionSlice slice) {
        return new MountTarget(dataMart, sourceKey, mode, extensionResolver, slice);
    }

    public String qualifiedDataMart() {
        return mode.qualify(dataMart);
    }

    public String resolveExtension(byte[] content) {
        return extensionResolver == null ? "json" : extensionResolver.resolve(content);
    }

    public static MountTarget transformation(String dataMart, PartitionKey sourceKey) {
        return new MountTarget(dataMart, sourceKey, MountMode.TRANSFORMATION, null, null);
    }

    public static MountTarget transformation(String dataMart, PartitionKey sourceKey, String extension) {
        return new MountTarget(dataMart, sourceKey, MountMode.TRANSFORMATION, ExtensionResolver.fixed(extension), null);
    }

    public static MountTarget transformation(String dataMart, PartitionKey sourceKey, ExtensionResolver resolver) {
        return new MountTarget(dataMart, sourceKey, MountMode.TRANSFORMATION, resolver, null);
    }

    public static MountTarget aggregation(String dataMart, PartitionKey sourceKey) {
        return new MountTarget(dataMart, sourceKey, MountMode.AGGREGATION, null, null);
    }

    public static MountTarget aggregation(String dataMart, PartitionKey sourceKey, String extension) {
        return new MountTarget(dataMart, sourceKey, MountMode.AGGREGATION, ExtensionResolver.fixed(extension), null);
    }

    public static MountTarget aggregation(String dataMart, PartitionKey sourceKey, ExtensionResolver resolver) {
        return new MountTarget(dataMart, sourceKey, MountMode.AGGREGATION, resolver, null);
    }
}
