package software.spool.mounter.internal.decorator;

import software.spool.core.model.vo.PartitionKey;
import software.spool.mounter.api.port.DataMartWriter;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedRecord;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * Writes with another writer and notes the partitions each mount unit writes to, keyed by the target of the unit, so
 * units dispatched at the same time do not mix their partitions.
 */
public class PartitionRecordingDataMartWriter implements DataMartWriter {
    private final DataMartWriter writer;
    private final Map<MountTarget, Set<PartitionKey>> written = new ConcurrentHashMap<>();

    public PartitionRecordingDataMartWriter(DataMartWriter writer) {
        this.writer = writer;
    }

    @Override
    public void write(MountTarget target, Stream<PartitionedRecord<?>> results) {
        writer.write(target, results.peek(result -> written
                .computeIfAbsent(target, ignored -> ConcurrentHashMap.newKeySet())
                .add(result.partitionKey())));
    }

    /** The partitions the unit of the target has written since the last call, which it forgets. */
    public Set<PartitionKey> takeWrittenBy(MountTarget target) {
        Set<PartitionKey> partitions = written.remove(target);
        return partitions == null ? Set.of() : Set.copyOf(partitions);
    }

    @Override
    public void beginRound(MountTarget target) {
        writer.beginRound(target);
    }

    @Override
    public void completeRound(MountTarget target) {
        writer.completeRound(target);
    }

    @Override
    public void abandonRound(MountTarget target) {
        writer.abandonRound(target);
    }
}
