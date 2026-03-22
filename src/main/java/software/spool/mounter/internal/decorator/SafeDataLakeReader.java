package software.spool.mounter.internal.decorator;

import software.spool.core.exception.DataLakeReadException;
import software.spool.core.exception.SpoolException;
import software.spool.core.model.PartitionKey;
import software.spool.mounter.api.port.DataLakeReader;

import java.util.stream.Stream;

public class SafeDataLakeReader<I> implements DataLakeReader<I> {
    private final DataLakeReader<I> reader;

    public SafeDataLakeReader(DataLakeReader<I> reader) {
        this.reader = reader;
    }

    @Override
    public Stream<I> read(PartitionKey partitionKey) {
        try {
            return reader.read(partitionKey);
        } catch (SpoolException e) {
            throw e;
        } catch (Exception e) {
            throw new DataLakeReadException(partitionKey, e.getMessage());
        }
    }
}
