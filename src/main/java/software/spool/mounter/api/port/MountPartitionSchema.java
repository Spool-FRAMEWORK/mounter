package software.spool.mounter.api.port;

import java.util.List;

public record MountPartitionSchema<O>(Class<O> recordClass, List<String> attributes) {

    public static <T> MountPartitionSchema<T> of(Class<T> recordClass, String... attributes) {
        if (attributes.length == 0)
            throw new IllegalArgumentException("MountPartitionSchema requires at least one partition attribute");
        return new MountPartitionSchema<>(recordClass, List.of(attributes));
    }
}