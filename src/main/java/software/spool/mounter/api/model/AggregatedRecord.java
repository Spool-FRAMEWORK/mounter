package software.spool.mounter.api.model;

public record AggregatedRecord<O>(GenericRecord source, O output) {}