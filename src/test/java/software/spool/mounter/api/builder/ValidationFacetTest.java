package software.spool.mounter.api.builder;

import org.junit.jupiter.api.Test;
import software.spool.core.adapter.memory.InMemoryDataLake;
import software.spool.core.adapter.memory.InMemoryQuarantineRepository;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.port.bus.EventBus;
import software.spool.core.port.bus.EventPublisher;
import software.spool.mounter.api.port.DataMartWriter;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedReader;
import software.spool.mounter.internal.quarantine.DataLakeRecordQuarantine;
import software.spool.mounter.internal.validation.ValidatingPartitionedReader;

import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

class ValidationFacetTest {

    private static final PartitionKey PARTITION = PartitionKey.ofEntriesWithoutDate().with("source", "products").build();
    private static final PartitionedReader BRONZE = target -> Stream.of();

    @Test
    void readerFor_withoutValidation_returnsTheSameReader() {
        ValidationFacet<Object> validation = new ValidationFacet<>(new Object());

        assertThat(validation.readerFor(BRONZE, MountTarget.transformation("products", PARTITION), quarantine()))
                .isSameAs(BRONZE);
    }

    @Test
    void readerFor_withValidationOnATransformation_validatesWhatItReads() {
        ValidationFacet<Object> validation = new ValidationFacet<>(new Object()).enabled();

        assertThat(validation.readerFor(BRONZE, MountTarget.transformation("products", PARTITION), quarantine()))
                .isInstanceOf(ValidatingPartitionedReader.class);
    }

    @Test
    void readerFor_validationWithoutQuarantine_failsWithAClearMessage() {
        ValidationFacet<Object> validation = new ValidationFacet<>(new Object()).enabled();

        assertThatThrownBy(() -> validation.readerFor(BRONZE, MountTarget.transformation("products", PARTITION), Optional.empty()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Quarantine required to validate on read; set it with quarantine().in(...)");
    }

    @Test
    void quarantineFor_withoutDataLakeEnvelopeRemover_failsWithAClearMessage() {
        QuarantineFacet<Object> quarantine = new QuarantineFacet<>(new Object()).in(new InMemoryQuarantineRepository());

        assertThatThrownBy(() -> quarantine.quarantineFor(mock(EventPublisher.class)))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("DataLakeEnvelopeRemover required to quarantine records; set it with quarantine().removingFromDataLakeWith(...)");
    }

    @Test
    void build_validationOnAnAggregationMount_failsSayingItOnlyAppliesFromBronzeToSilver() {
        ReactiveMounterBuilder<Object, Object> builder = MounterBuilderFactory.<Object, Object>reactive(mock(EventBus.class))
                .readingWith(BRONZE)
                .aggregatingWith(records -> Stream.of())
                .mount().onTarget(MountTarget.aggregation("products", PARTITION)).writingWith(mock(DataMartWriter.class)).and()
                .validation().enabled().and()
                .quarantine().in(new InMemoryQuarantineRepository()).removingFromDataLakeWith(new InMemoryDataLake()).and();

        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("TRANSFORMATION")
                .hasMessageContaining("AGGREGATION");
    }

    @Test
    void build_validationOnATransformationMount_builds() {
        ReactiveMounterBuilder<Object, Object> builder = MounterBuilderFactory.<Object, Object>reactive(mock(EventBus.class))
                .readingWith(BRONZE)
                .aggregatingWith(records -> Stream.of())
                .mount().onTarget(MountTarget.transformation("products", PARTITION)).writingWith(mock(DataMartWriter.class)).and()
                .validation().enabled().and()
                .quarantine().in(new InMemoryQuarantineRepository()).removingFromDataLakeWith(new InMemoryDataLake()).and();

        assertThatCode(builder::build).doesNotThrowAnyException();
    }

    @Test
    void build_pollingValidationWithoutAPublisher_failsWithAClearMessage() {
        PollingMounterBuilder<Object, Object> builder = MounterBuilderFactory.<Object, Object>polling(BRONZE)
                .aggregatingWith(records -> Stream.of())
                .mount().onTarget(MountTarget.transformation("products", PARTITION)).writingWith(mock(DataMartWriter.class)).and()
                .validation().enabled().and()
                .quarantine().in(new InMemoryQuarantineRepository()).removingFromDataLakeWith(new InMemoryDataLake()).and();

        assertThatThrownBy(builder::build)
                .isInstanceOf(NullPointerException.class)
                .hasMessage("EventPublisher required to quarantine records; set it with scheduling().emittingWith(...)");
    }

    private static Optional<DataLakeRecordQuarantine> quarantine() {
        return new QuarantineFacet<>(new Object()).in(new InMemoryQuarantineRepository())
                .removingFromDataLakeWith(new InMemoryDataLake()).quarantineFor(mock(EventPublisher.class));
    }
}
