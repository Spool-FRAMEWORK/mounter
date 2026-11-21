package software.spool.mounter.api.adapter;

import software.spool.core.model.vo.ManualPartitionKeyBuilder;
import software.spool.core.model.vo.PartitionKey;
import software.spool.mounter.api.exception.MissingPartitionAttributeException;
import software.spool.mounter.api.model.GenericRecord;
import software.spool.mounter.api.port.MountPartitioning;
import software.spool.mounter.api.port.PartitionedRecord;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.RecordComponent;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Builds the partition from attributes of the result, a {@link GenericRecord} or a Java record, in the order given.
 * A nested attribute is written as its path ({@code payload.country}), and each entry is named after it.
 */
public class AttributesPartitioning<O> implements MountPartitioning<O> {
    private static final String PATH_SEPARATOR = "\\.";

    private final List<String> attributes;

    private AttributesPartitioning(List<String> attributes) {
        this.attributes = attributes;
    }

    public static <O> AttributesPartitioning<O> of(String... attributes) {
        return of(List.of(attributes));
    }

    public static <O> AttributesPartitioning<O> of(List<String> attributes) {
        if (attributes.isEmpty()) throw new IllegalArgumentException("AttributesPartitioning requires at least one attribute");
        return new AttributesPartitioning<>(List.copyOf(attributes));
    }

    @Override
    public PartitionKey of(PartitionedRecord<? extends O> result) {
        ManualPartitionKeyBuilder partition = PartitionKey.ofEntriesWithoutDate();
        for (String attribute : attributes) {
            partition.with(attribute, valueAt(result.record(), attribute)
                    .orElseThrow(() -> new MissingPartitionAttributeException(attribute)));
        }
        return partition.build();
    }

    private static Optional<Object> valueAt(Object record, String attribute) {
        Optional<Object> current = Optional.ofNullable(record);
        for (String segment : attribute.split(PATH_SEPARATOR)) {
            current = current.flatMap(value -> field(value, segment));
        }
        return current;
    }

    private static Optional<Object> field(Object value, String name) {
        if (value instanceof GenericRecord genericRecord) return field(genericRecord.toMap(), name);
        if (value instanceof Map<?, ?> map) return Optional.ofNullable(map.get(name));
        if (value.getClass().isRecord()) return component(value, name);
        return Optional.empty();
    }

    private static Optional<Object> component(Object record, String name) {
        return Arrays.stream(record.getClass().getRecordComponents())
                .filter(component -> component.getName().equals(name))
                .findFirst()
                .flatMap(component -> Optional.ofNullable(read(record, component)));
    }

    private static Object read(Object record, RecordComponent component) {
        try {
            component.getAccessor().setAccessible(true);
            return component.getAccessor().invoke(record);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new IllegalStateException("Failed to read partition attribute " + component.getName()
                    + " of " + record.getClass().getSimpleName(), e);
        }
    }
}
