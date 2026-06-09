package software.spool.mounter.api;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.spool.core.model.spool.SpoolNode;
import software.spool.core.port.health.HealthStatus;
import software.spool.core.port.watchdog.ModuleHeartBeat;
import software.spool.core.utils.routing.ErrorRouter;
import software.spool.mounter.api.fixture.CapturingMountStrategy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class MounterTest {

    private CapturingMountStrategy strategy;
    private Mounter mounter;
    private SpoolNode.StartPermit permit;

    @BeforeEach
    void setUp() {
        strategy = new CapturingMountStrategy();
        mounter = new Mounter(strategy, new ErrorRouter(), ModuleHeartBeat.NOOP);
        permit = mock(SpoolNode.StartPermit.class);
    }

    @Test
    void start_delegatesToStrategy() {
        mounter.start(permit);
        assertThat(strategy.wasExecuted()).isTrue();
    }

    @Test
    void start_idempotent_doesNotStartTwice() {
        mounter.start(permit);
        mounter.start(permit);
        assertThat(strategy.executeCount()).isEqualTo(1);
    }

    @Test
    void stop_afterStart_cancelsPreviousToken() {
        mounter.start(permit);
        var lastToken = strategy.lastToken();

        mounter.stop(permit);

        assertThat(lastToken.isActive()).isFalse();
    }

    @Test
    void checkHealth_whenRunning_returnsHealthy() {
        mounter.start(permit);
        assertThat(mounter.checkHealth().status()).isEqualTo(HealthStatus.HEALTHY);
    }

    @Test
    void checkHealth_whenStopped_returnsDegraded() {
        assertThat(mounter.checkHealth().status()).isEqualTo(HealthStatus.DEGRADED);
    }
}
