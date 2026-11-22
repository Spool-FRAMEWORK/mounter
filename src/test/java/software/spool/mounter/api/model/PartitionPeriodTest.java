package software.spool.mounter.api.model;

import org.junit.jupiter.api.Test;
import software.spool.core.model.vo.PartitionKey;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class PartitionPeriodTest {

    @Test
    void of_partitionWithAnHour_coversThatHour() {
        assertThat(PartitionPeriod.of(new PartitionKey("source=payments::year=2026::month=11::day=17::hour=10")))
                .contains(new PartitionPeriod(Instant.parse("2026-11-17T10:00:00Z"), Instant.parse("2026-11-17T11:00:00Z")));
    }

    @Test
    void of_partitionWithAMonth_coversTheWholeMonth() {
        assertThat(PartitionPeriod.of(new PartitionKey("year=2026::month=12")))
                .contains(new PartitionPeriod(Instant.parse("2026-12-01T00:00:00Z"), Instant.parse("2027-01-01T00:00:00Z")));
    }

    @Test
    void of_partitionWithAYear_coversTheWholeYear() {
        assertThat(PartitionPeriod.of(new PartitionKey("year=2026")))
                .contains(new PartitionPeriod(Instant.parse("2026-01-01T00:00:00Z"), Instant.parse("2027-01-01T00:00:00Z")));
    }

    @Test
    void of_partitionNamedOutsideTheEntries_ignoresTheEntriesWithoutValue() {
        assertThat(PartitionPeriod.of(new PartitionKey("payments::year=2026::month=11::day=17")))
                .contains(new PartitionPeriod(Instant.parse("2026-11-17T00:00:00Z"), Instant.parse("2026-11-18T00:00:00Z")));
    }

    @Test
    void of_partitionWithoutYear_hasNoPeriod() {
        assertThat(PartitionPeriod.of(new PartitionKey("country=ES::day=17"))).isEmpty();
    }

    @Test
    void of_partitionWhoseDateIsNotADate_hasNoPeriod() {
        assertThat(PartitionPeriod.of(new PartitionKey("year=2026::month=13"))).isEmpty();
        assertThat(PartitionPeriod.of(new PartitionKey("year=last"))).isEmpty();
    }
}
