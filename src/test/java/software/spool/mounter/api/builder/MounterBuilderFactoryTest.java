package software.spool.mounter.api.builder;

import org.junit.jupiter.api.Test;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.port.bus.EventBus;
import software.spool.mounter.api.Mounter;
import software.spool.mounter.api.port.DataMartWriter;
import software.spool.mounter.api.port.MountTarget;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class MounterBuilderFactoryTest {

    @Test
    void withoutAWatchdog_mounterStillReportsTheGivenModuleId() {
        Mounter mounter = MounterBuilderFactory.watchdog(null, "payments-silver")
                .<Object, Object>reactive(mock(EventBus.class))
                .mount().onTarget(MountTarget.transformation("payments", new PartitionKey("source=payments")))
                .writingWith(mock(DataMartWriter.class)).and()
                .build();

        assertThat(mounter.checkHealth().moduleId()).isEqualTo("payments-silver");
    }

    @Test
    void watchdog_reactiveMounter_reportsTheGivenModuleId() {
        Mounter mounter = MounterBuilderFactory.watchdog("http://watchdog.invalid", "payments-silver")
                .<Object, Object>reactive(mock(EventBus.class))
                .mount().onTarget(MountTarget.transformation("payments", new PartitionKey("source=payments")))
                .writingWith(mock(DataMartWriter.class)).and()
                .build();

        assertThat(mounter.checkHealth().moduleId()).isEqualTo("payments-silver");
    }
}
