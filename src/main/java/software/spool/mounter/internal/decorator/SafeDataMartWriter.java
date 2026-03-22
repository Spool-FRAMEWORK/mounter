package software.spool.mounter.internal.decorator;

import software.spool.core.exception.DataMartWriteException;
import software.spool.core.exception.SpoolException;
import software.spool.core.model.PartitionKey;
import software.spool.mounter.api.port.DataMartWriter;

import java.util.stream.Stream;

public class SafeDataMartWriter<O> implements DataMartWriter<O> {
    private final DataMartWriter<O> writer;

    public SafeDataMartWriter(DataMartWriter<O> writer) {
        this.writer = writer;
    }

    public static <D> DataMartWriter<D> of(DataMartWriter<D> writer) {
        return new SafeDataMartWriter<>(writer);
    }

    @Override
    public void write(PartitionKey partitionKey, Stream<O> result) {
        try {
            writer.write(partitionKey, result);
        } catch (SpoolException e) {
            throw e;
        } catch (Exception e) {
            throw new DataMartWriteException(partitionKey, e.getMessage());
        }
    }
}
