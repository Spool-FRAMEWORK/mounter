package software.spool.mounter.internal.decorator;

import software.spool.core.exception.DataMartWriteException;
import software.spool.core.exception.SpoolException;
import software.spool.mounter.api.port.DataMartWriter;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedRecord;

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
    public void write(MountTarget target, Stream<PartitionedRecord<O>> result) {
        try {
            writer.write(target, result);
        } catch (SpoolException e) {
            throw e;
        } catch (Exception e) {
            throw new DataMartWriteException(target.sourceKey(), e.getMessage());
        }
    }
}
