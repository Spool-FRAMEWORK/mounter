package software.spool.mounter.api.adapter;

import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.exception.QuarantinedResultRejectedException;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.port.quarantine.QuarantinedResultWriter;
import software.spool.mounter.api.exception.MissingPartitionAttributeException;
import software.spool.mounter.api.model.GenericRecord;
import software.spool.mounter.api.port.DataMartWriter;
import software.spool.mounter.api.port.MountPartitioning;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedRecord;

import java.util.stream.Stream;

/**
 * Writes a mount result taken out of quarantine to the data mart of a mount, with the partitioning and the writer of
 * that mount, so the result lands where the mount would write it now.
 */
public class PartitioningQuarantinedResultWriter implements QuarantinedResultWriter {
    private final MountTarget target;
    private final MountPartitioning<Object> partitioning;
    private final DataMartWriter writer;

    @SuppressWarnings("unchecked")
    public PartitioningQuarantinedResultWriter(MountTarget target, MountPartitioning<?> partitioning, DataMartWriter writer) {
        this.target = target;
        this.partitioning = (MountPartitioning<Object>) partitioning;
        this.writer = writer;
    }

    @Override
    public String dataMart() {
        return target.qualifiedDataMart();
    }

    @Override
    public void write(PartitionKey partition, byte[] content) {
        PartitionedRecord<Object> result = new PartitionedRecord<>(partition,
                GenericRecord.of(PayloadDeserializerFactory.json().asMap().deserialize(content)));
        writer.write(target, Stream.of(new PartitionedRecord<>(destinationOf(result), result.record())));
    }

    private PartitionKey destinationOf(PartitionedRecord<Object> result) {
        try {
            return partitioning.of(result);
        } catch (MissingPartitionAttributeException e) {
            throw new QuarantinedResultRejectedException(
                    "The result cannot be written to " + dataMart() + ": " + e.getMessage(), e);
        }
    }
}
