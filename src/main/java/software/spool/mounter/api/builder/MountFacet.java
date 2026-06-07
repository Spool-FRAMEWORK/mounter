package software.spool.mounter.api.builder;

import software.spool.core.model.vo.PartitionKey;
import software.spool.mounter.api.port.*;
import software.spool.mounter.internal.decorator.SafeDataMartWriter;
import software.spool.mounter.internal.utils.RecordPartitionKeyExtractor;

public class MountFacet<B> extends MounterFacet<B> {
    DataMartWriter writer;
    MountTarget target;
    PartitionKey scope;
    PartitionKeyExtractor keyExtractor;

    MountFacet(B parent) { super(parent); }

    public MountFacet<B> writingWith(DataMartWriter writer) {
        this.writer = SafeDataMartWriter.of(writer);
        return this;
    }

    public MountFacet<B> onTarget(MountTarget target) {
        this.target = target;
        return this;
    }

    public MountFacet<B> onScope(PartitionKey scope) {
        this.scope = scope;
        return this;
    }

    public <R> MountFacet<B> partitioningWith(MountPartitionSchema<R> schema) {
        this.keyExtractor = new RecordPartitionKeyExtractor<>(schema);
        return this;
    }

    public MountFacet<B> partitioningWith(PartitionKeyExtractor keyExtractor) {
        this.keyExtractor = keyExtractor;
        return this;
    }
}
