package software.spool.mounter.internal.control;

import software.spool.core.exception.SpoolException;
import software.spool.core.model.event.EnvelopesMounted;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.port.bus.EventPublisher;
import software.spool.core.port.bus.Handler;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.internal.decorator.PartitionRecordingDataMartWriter;

import java.util.Set;

/**
 * Mounts a unit with another handler and then publishes {@link EnvelopesMounted} once for each partition of its data
 * mart the unit wrote to. A unit that fails publishes nothing.
 */
public class AnnouncingMountUnitHandler implements Handler<MountTarget> {
    private final Handler<MountTarget> handler;
    private final PartitionRecordingDataMartWriter writer;
    private final EventPublisher publisher;

    public AnnouncingMountUnitHandler(Handler<MountTarget> handler, PartitionRecordingDataMartWriter writer,
                                      EventPublisher publisher) {
        this.handler = handler;
        this.writer = writer;
        this.publisher = publisher;
    }

    @Override
    public void handle(MountTarget target) throws SpoolException {
        try {
            handler.handle(target);
        } catch (RuntimeException e) {
            writer.takeWrittenBy(target);
            throw e;
        }
        Set<PartitionKey> written = writer.takeWrittenBy(target);
        written.forEach(partition -> publisher.publish(EnvelopesMounted.builder()
                .dataMart(target.qualifiedDataMart())
                .partitionKey(partition)
                .build()));
    }
}
