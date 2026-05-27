package software.spool.mounter.internal.decorator;

import software.spool.core.exception.DataLakeReadException;
import software.spool.core.exception.SpoolException;
import software.spool.core.model.vo.PartitionKey;
import software.spool.mounter.api.model.GenericRecord;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedReader;
import software.spool.mounter.api.port.PartitionedRecord;

import java.util.List;

public class SafePartitionedReader implements PartitionedReader {
    private final PartitionedReader reader;

    public SafePartitionedReader(PartitionedReader reader) {
        this.reader = reader;
    }

    public static SafePartitionedReader of(PartitionedReader reader) {
        return new SafePartitionedReader(reader);
    }

    @Override
    public List<PartitionedRecord<GenericRecord>> read(MountTarget mountTarget) {
        try {
            return reader.read(mountTarget);
        } catch (SpoolException e) {
            throw e;
        } catch (Exception e) {
            throw new DataLakeReadException(mountTarget.sourceKey(), e.getMessage());
        }
    }
}
