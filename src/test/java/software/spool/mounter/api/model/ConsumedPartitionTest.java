package software.spool.mounter.api.model;

import org.junit.jupiter.api.Test;
import software.spool.core.model.vo.PartitionKey;

import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;

class ConsumedPartitionTest {
    private static final PartitionKey PARTITION = new PartitionKey("source=payments/year=2026/month=11/day=17/hour=10");

    @Test
    void hasChanged_moreFilesThanWhenItWasRead_isTrue() {
        assertThat(ConsumedPartition.of(PARTITION, OptionalLong.of(2)).hasChanged(OptionalLong.of(3))).isTrue();
    }

    @Test
    void hasChanged_fewerFilesThanWhenItWasRead_isTrue() {
        assertThat(ConsumedPartition.of(PARTITION, OptionalLong.of(2)).hasChanged(OptionalLong.of(1))).isTrue();
    }

    @Test
    void hasChanged_sameFilesAsWhenItWasRead_isFalse() {
        assertThat(ConsumedPartition.of(PARTITION, OptionalLong.of(2)).hasChanged(OptionalLong.of(2))).isFalse();
    }

    @Test
    void hasChanged_readWithoutKeepingItsFiles_isFalse() {
        assertThat(ConsumedPartition.withoutFiles(PARTITION).hasChanged(OptionalLong.of(5))).isFalse();
    }

    @Test
    void hasChanged_filesNowUnknown_isFalse() {
        assertThat(ConsumedPartition.of(PARTITION, OptionalLong.of(2)).hasChanged(OptionalLong.empty())).isFalse();
    }
}
