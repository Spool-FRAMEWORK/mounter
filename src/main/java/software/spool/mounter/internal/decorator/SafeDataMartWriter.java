package software.spool.mounter.internal.decorator;

import software.spool.core.exception.DataMartWriteException;
import software.spool.core.exception.SpoolException;
import software.spool.mounter.api.port.DataMartWriter;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedRecord;

import java.util.stream.Stream;

public class SafeDataMartWriter<O> implements DataMartWriter {
    private final DataMartWriter writer;

    public SafeDataMartWriter(DataMartWriter writer) {
        this.writer = writer;
    }

    public static DataMartWriter of(DataMartWriter writer) {
        return new SafeDataMartWriter<>(writer);
    }

    @Override
    public void write(MountTarget target, Stream<PartitionedRecord<?>> result) {
        safely(target, () -> writer.write(target, result));
    }

    @Override
    public void beginRound(MountTarget target) {
        safely(target, () -> writer.beginRound(target));
    }

    @Override
    public void completeRound(MountTarget target) {
        safely(target, () -> writer.completeRound(target));
    }

    @Override
    public void abandonRound(MountTarget target) {
        safely(target, () -> writer.abandonRound(target));
    }

    private static void safely(MountTarget target, Runnable writing) {
        try {
            writing.run();
        } catch (SpoolException e) {
            throw e;
        } catch (Exception e) {
            throw new DataMartWriteException(target.sourceKey(), e.getMessage(), e);
        }
    }
}
