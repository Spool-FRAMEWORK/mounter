package software.spool.mounter.internal.decorator;

import software.spool.core.exception.DataLakeReadException;
import software.spool.core.exception.SpoolException;
import software.spool.mounter.api.model.GenericRecord;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedReader;
import software.spool.mounter.api.port.PartitionedRecord;
import software.spool.mounter.api.port.StreamingPartitionedReader;

import java.util.List;
import java.util.stream.Stream;

public class SafePartitionedReader implements StreamingPartitionedReader {
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

    @Override
    public Stream<PartitionedRecord<GenericRecord>> stream(MountTarget mountTarget) {
        try {
            if (reader instanceof StreamingPartitionedReader sr) {
                return sr.stream(mountTarget);
            }
            return reader.read(mountTarget).stream();
        } catch (SpoolException e) {
            throw e;
        } catch (Exception e) {
            throw new DataLakeReadException(mountTarget.sourceKey(), e.getMessage());
        }
    }
}
