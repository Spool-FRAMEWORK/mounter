package software.spool.mounter.internal.decorator;

import software.spool.core.exception.DataLakeReadException;
import software.spool.core.exception.SpoolException;
import software.spool.core.model.vo.PartitionKey;
import software.spool.mounter.api.port.PartitionedReader;
import software.spool.mounter.api.port.PartitionedRecord;

import java.util.List;

public class SafePartitionedReader<I> implements PartitionedReader<I> {
    private final PartitionedReader<I> reader;

    public SafePartitionedReader(PartitionedReader<I> reader) {
        this.reader = reader;
    }

    public static <I> SafePartitionedReader<I> of(PartitionedReader<I> reader) {
        return new SafePartitionedReader<>(reader);
    }

    @Override
    public List<PartitionedRecord<I>> read(PartitionKey partitionKey) {
        try {
            return reader.read(partitionKey);
        } catch (SpoolException e) {
            throw e;
        } catch (Exception e) {
            throw new DataLakeReadException(partitionKey, e.getMessage());
        }
    }
}
