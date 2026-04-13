package software.spool.mounter.internal.utils;

import software.spool.core.model.vo.PartitionKey;
import software.spool.mounter.api.port.MountPartitionSchema;
import software.spool.mounter.api.port.PartitionKeyExtractor;

import java.lang.reflect.RecordComponent;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

public class RecordPartitionKeyExtractor<O> implements PartitionKeyExtractor<O> {

    private final Map<String, Function<O, Object>> accessors = new LinkedHashMap<>();

    public RecordPartitionKeyExtractor(MountPartitionSchema<O> schema) {
        Class<O> recordClass = schema.recordClass();

        if (!recordClass.isRecord()) {
            throw new IllegalArgumentException("Output class must be a Java Record: " + recordClass.getName());
        }

        for (String key : schema.attributes()) {
            try {
                RecordComponent component = findComponent(recordClass, key);
                accessors.put(key, obj -> {
                    try {
                        return component.getAccessor().invoke(obj);
                    } catch (Throwable e) {
                        throw new IllegalStateException("Failed to read partition attribute: " + key, e);
                    }
                });
            } catch (NoSuchMethodException e) {
                throw new IllegalArgumentException(
                        String.format("Attribute '%s' not found in record %s", key, recordClass.getSimpleName())
                );
            }
        }
    }

    private RecordComponent findComponent(Class<?> clazz, String name) throws NoSuchMethodException {
        for (RecordComponent rc : clazz.getRecordComponents()) {
            if (rc.getName().equals(name)) return rc;
        }
        throw new NoSuchMethodException(name);
    }

    @Override
    public PartitionKey extract(O payload) {
        StringBuilder path = new StringBuilder();
        for (Map.Entry<String, Function<O, Object>> entry : accessors.entrySet()) {
            if (!path.isEmpty()) path.append("/");
            path.append(entry.getKey()).append("=").append(entry.getValue().apply(payload));
        }
        return new PartitionKey(path.toString());
    }
}