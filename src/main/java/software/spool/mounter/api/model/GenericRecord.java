package software.spool.mounter.api.model;

import software.spool.core.adapter.jackson.RecordSerializerFactory;

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

    public boolean has(String field) {
        return fields.containsKey(field);
    }

    public Map<String, Object> content() {
        return Map.copyOf(fields);
    }

    public Set<String> fields() {
        return fields.keySet();
    }

    public Object get(String field) {
        if (!fields.containsKey(field)) {
            throw new NoSuchElementException("Field '" + field + "' not found");
        }
        return fields.get(field);
    }

    public String getString(String field) {
        Object value = get(field);
        return value != null ? value.toString() : null;
    }

    public Long getLong(String field) {
        Object value = get(field);
        if (!(value instanceof Number number)) {
            throw new ClassCastException("Field '" + field + "' is not a Number");
        }
        return number.longValue();
    }

    public Double getDouble(String field) {
        Object value = get(field);
        if (!(value instanceof Number number)) {
            throw new ClassCastException("Field '" + field + "' is not a Number");
        }
        return number.doubleValue();
    }

    public Boolean getBoolean(String field) {
        Object value = get(field);
        if (!(value instanceof Boolean b)) {
            throw new ClassCastException("Field '" + field + "' is not a Boolean");
        }
        return b;
    }

    @SuppressWarnings("unchecked")
    public GenericRecord getNested(String field) {
        Object value = get(field);
        if (!(value instanceof Map<?, ?> map)) {
            throw new ClassCastException("Field '" + field + "' is not a Map");
        }
        return GenericRecord.of((Map<String, Object>) map);
    }

    @SuppressWarnings("unchecked")
    public List<GenericRecord> getList(String field) {
        Object value = get(field);
        if (!(value instanceof List<?> list)) {
            throw new ClassCastException("Field '" + field + "' is not a List");
        }
        return ((List<Map<String, Object>>) list).stream()
                .map(GenericRecord::of)
                .toList();
    }

    public Map<String, Object> toMap() {
        return fields;
    }

    @Override
    public String toString() {
        return new String(RecordSerializerFactory.record().serialize(fields))   ;
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