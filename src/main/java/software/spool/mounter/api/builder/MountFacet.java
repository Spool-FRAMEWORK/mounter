package software.spool.mounter.api.builder;

import software.spool.core.model.vo.PartitionKey;
import software.spool.mounter.api.adapter.SameAsSourcePartitioning;
import software.spool.mounter.api.port.*;
import software.spool.mounter.internal.decorator.SafeDataMartWriter;

public class MountFacet<B> extends MounterFacet<B> {
    DataMartWriter writer;
    MountTarget target;
    PartitionKey scope;
    MountPartitioning<?> partitioning = new SameAsSourcePartitioning<>();
    DataMartRemover remover;

    MountFacet(B parent) { super(parent); }

    /**
     * Sets how a result written before is removed from the data mart, which an incremental mount needs to replace the
     * result of an aggregate that changes.
     */
    public MountFacet<B> removingWith(DataMartRemover remover) {
        this.remover = remover;
        return this;
    }

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

    /**
     * Sets how the partition each result is written to is decided. By default, the partition the result comes with.
     */
    public MountFacet<B> partitioningWith(MountPartitioning<?> partitioning) {
        this.partitioning = partitioning;
        return this;
    }
}
