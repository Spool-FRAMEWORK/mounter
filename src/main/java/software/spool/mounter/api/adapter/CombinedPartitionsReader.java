package software.spool.mounter.api.adapter;

import software.spool.core.model.vo.PartitionKey;
import software.spool.mounter.api.model.GenericRecord;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedReader;
import software.spool.mounter.api.port.PartitionedRecord;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Reads several partitions with another reader, one after the other, in place of the partition the target names, so
 * the aggregator gets the records of all of them in one stream.
 *
 * <p>A reader gives each record with the partition of the folder it is kept in, and reads a folder whole. When two of
 * the partitions overlap, the records of a folder that an earlier partition already gave are skipped, so no record is
 * read twice.</p>
 */
public class CombinedPartitionsReader implements PartitionedReader {
    private final PartitionedReader reader;
    private final List<PartitionKey> partitions;

    public CombinedPartitionsReader(PartitionedReader reader, List<PartitionKey> partitions) {
        if (partitions.isEmpty()) {
            throw new IllegalArgumentException("A combined read needs at least one partition");
        }
        this.reader = reader;
        this.partitions = List.copyOf(partitions);
    }

    @Override
    public Stream<PartitionedRecord<GenericRecord>> read(MountTarget mountTarget) {
        Set<PartitionKey> foldersRead = new HashSet<>();
        return partitions.stream().flatMap(partition -> notReadBefore(mountTarget.withSourceKey(partition), foldersRead));
    }

    private Stream<PartitionedRecord<GenericRecord>> notReadBefore(MountTarget target, Set<PartitionKey> foldersRead) {
        Set<PartitionKey> foldersReadBefore = Set.copyOf(foldersRead);
        return reader.read(target)
                .filter(record -> !foldersReadBefore.contains(record.partitionKey()))
                .map(record -> remembered(record, foldersRead));
    }

    private static PartitionedRecord<GenericRecord> remembered(PartitionedRecord<GenericRecord> record, Set<PartitionKey> foldersRead) {
        foldersRead.add(record.partitionKey());
        return record;
    }
}
