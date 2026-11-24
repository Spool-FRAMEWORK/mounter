package software.spool.mounter.api.adapter.health;

import org.junit.jupiter.api.Test;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.port.health.HealthStatus;
import software.spool.core.port.health.PieceCheckPolicy;
import software.spool.core.port.health.PieceMonitor;
import software.spool.mounter.api.adapter.InMemoryAggregateMetadataRepository;
import software.spool.mounter.api.model.GenericRecord;
import software.spool.mounter.api.port.DataMartWriter;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedReader;
import software.spool.mounter.api.port.PartitionedRecord;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MonitoredMounterPortsTest {
    private static final PartitionKey PARTITION = new PartitionKey("source=payments");
    private static final MountTarget TARGET = MountTarget.transformation("payments", PARTITION);
    private static final PartitionedRecord<GenericRecord> RECORD = new PartitionedRecord<>(PARTITION, GenericRecord.of(Map.of("id", "payment-1")));

    private final PieceMonitor monitor = PieceMonitor.of("lake", "FILE_SYSTEM", Optional.empty(), PieceCheckPolicy.defaults());

    @Test
    void reader_whenARecordCannotBeRead_marksThePieceUnhealthy() {
        PartitionedReader reader = new MonitoredPartitionedReader(target -> Stream.of(RECORD).map(record -> {
            throw new IllegalStateException("the file payment-1.json could not be read");
        }), monitor);

        assertThatThrownBy(() -> reader.read(TARGET).toList()).hasMessage("the file payment-1.json could not be read");
        assertThat(monitor.health().status()).isEqualTo(HealthStatus.UNHEALTHY);
    }

    @Test
    void reader_givesTheRecordsItRead() {
        PartitionedReader reader = new MonitoredPartitionedReader(target -> Stream.of(RECORD), monitor);

        assertThat(reader.read(TARGET).toList()).containsExactly(RECORD);
        assertThat(monitor.health().status()).isEqualTo(HealthStatus.HEALTHY);
    }

    @Test
    void writer_whenItCannotWrite_marksThePieceUnhealthy() {
        DataMartWriter writer = new MonitoredDataMartWriter((target, result) -> result.forEach(record -> {
            throw new IllegalStateException("the folder of the data mart cannot be written");
        }), monitor);

        assertThatThrownBy(() -> writer.write(TARGET, Stream.of(RECORD))).hasMessage("the folder of the data mart cannot be written");
        assertThat(monitor.health().status()).isEqualTo(HealthStatus.UNHEALTHY);
    }

    @Test
    void writer_whenTheResultsItIsGivenFail_doesNotBlameTheDataMart() {
        DataMartWriter writer = new MonitoredDataMartWriter((target, result) -> result.forEach(record -> {
        }), monitor);
        Stream<PartitionedRecord<?>> failingResults = Stream.<PartitionedRecord<?>>of(RECORD).map(record -> {
            throw new IllegalStateException("the source could not be read");
        });

        assertThatThrownBy(() -> writer.write(TARGET, failingResults)).hasMessage("the source could not be read");
        assertThat(monitor.health().status()).isEqualTo(HealthStatus.HEALTHY);
    }

    @Test
    void writer_roundThatEndsAfterARecordCouldNotBeRead_leavesThePieceUnhealthy() {
        PartitionedReader reader = new MonitoredPartitionedReader(target -> Stream.of(RECORD).map(record -> {
            throw new IllegalStateException("the file payment-1.json could not be read");
        }), monitor);
        DataMartWriter writer = new MonitoredDataMartWriter((target, result) -> result.forEach(record -> {
        }), monitor);
        writer.beginRound(TARGET);

        assertThatThrownBy(() -> reader.read(TARGET).toList()).isInstanceOf(IllegalStateException.class);
        writer.abandonRound(TARGET);

        assertThat(monitor.health().status()).isEqualTo(HealthStatus.UNHEALTHY);
    }

    @Test
    void writer_whenItCannotEndARound_marksThePieceUnhealthy() {
        DataMartWriter writer = new MonitoredDataMartWriter(new DataMartWriter() {
            @Override
            public void write(MountTarget target, Stream<PartitionedRecord<?>> result) {
            }

            @Override
            public void completeRound(MountTarget target) {
                throw new IllegalStateException("the list of the mount could not be written");
            }
        }, monitor);

        assertThatThrownBy(() -> writer.completeRound(TARGET)).hasMessage("the list of the mount could not be written");
        assertThat(monitor.health().status()).isEqualTo(HealthStatus.UNHEALTHY);
    }

    @Test
    void remover_whenItCannotRemove_marksThePieceUnhealthy() {
        MonitoredDataMartRemover remover = new MonitoredDataMartRemover((target, result) -> {
            throw new IllegalStateException("the object could not be deleted");
        }, monitor);

        assertThatThrownBy(() -> remover.remove(TARGET, RECORD)).isInstanceOf(IllegalStateException.class);
        assertThat(monitor.health().status()).isEqualTo(HealthStatus.UNHEALTHY);
    }

    @Test
    void aggregateMetadata_givesWhatTheRepositoryKeeps() {
        MonitoredAggregateMetadataRepository repository = new MonitoredAggregateMetadataRepository(new InMemoryAggregateMetadataRepository(), monitor);

        assertThat(repository.findAll("payments")).isEmpty();
        assertThat(monitor.health().status()).isEqualTo(HealthStatus.HEALTHY);
    }
}
