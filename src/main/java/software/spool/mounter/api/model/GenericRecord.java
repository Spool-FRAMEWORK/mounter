package software.spool.mounter.api.model;

import java.util.*;

public final class GenericRecord {
    private final Map<String, Object> fields;

    private GenericRecord(Map<String, Object> fields) {
        this.fields = Collections.unmodifiableMap(fields);
    }

    public static GenericRecord of(Map<String, Object> fields) {
        Objects.requireNonNull(fields, "fields must not be null");
        return new GenericRecord(new LinkedHashMap<>(fields));
    }

    public Optional<Object> get(String field) {
        return Optional.ofNullable(fields.get(field));
    }

    public boolean has(String field) {
        return fields.containsKey(field);
    }

    public Set<String> fields() {
        return fields.keySet();
    }

    public Optional<String> getString(String field) {
        return get(field).map(Object::toString);
    }

    public Optional<Long> getLong(String field) {
        return get(field).map(v -> ((Number) v).longValue());
    }

    public Optional<Double> getDouble(String field) {
        return get(field).map(v -> ((Number) v).doubleValue());
    }

    public Optional<Boolean> getBoolean(String field) {
        return get(field).map(v -> (Boolean) v);
    }

    @SuppressWarnings("unchecked")
    public Optional<GenericRecord> getNested(String field) {
        return get(field)
            .filter(v -> v instanceof Map)
            .map(v -> GenericRecord.of((Map<String, Object>) v));
    }

    @SuppressWarnings("unchecked")
    public List<GenericRecord> getList(String field) {
        return get(field)
            .filter(v -> v instanceof List)
            .map(v -> ((List<Map<String, Object>>) v).stream()
                .map(GenericRecord::of)
                .toList())
            .orElse(List.of());
    }

    public Map<String, Object> toMap() {
        return fields;
    }

    @Override
    public String toString() {
        return "SpoolRecord" + fields;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof GenericRecord r)) return false;
        return fields.equals(r.fields);
    }

    @Override
    public int hashCode() {
        return fields.hashCode();
    }
}