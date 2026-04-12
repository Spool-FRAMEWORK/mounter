package software.spool.mounter.api.port;

import java.util.stream.Stream;

public interface DataMartWriter<R> {
    void write(MountTarget target, Stream<R> result);
}
