package software.spool.mounter.api.builder;

import software.spool.mounter.api.port.*;

public class CheckpointFacet<B> extends MounterFacet<B> {
    PartitionWindowPolicy windowPolicy;
    MountCheckpoint checkpoint;
    MountCursor cursor;

    CheckpointFacet(B parent) { super(parent); }

    public CheckpointFacet<B> windowPolicy(PartitionWindowPolicy windowPolicy) {
        this.windowPolicy = windowPolicy;
        return this;
    }

    public CheckpointFacet<B> checkpoint(MountCheckpoint checkpoint) {
        this.checkpoint = checkpoint;
        return this;
    }

    public CheckpointFacet<B> cursor(MountCursor cursor) {
        this.cursor = cursor;
        return this;
    }
}
