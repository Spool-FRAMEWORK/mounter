package software.spool.mounter.internal.strategy;

import org.junit.jupiter.api.Test;
import software.spool.core.adapter.memory.InMemoryEventBus;
import software.spool.core.model.event.EnvelopePersisted;
import software.spool.core.model.event.EnvelopesMounted;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.utils.polling.CancellationToken;
import software.spool.core.utils.routing.ErrorRouter;
import software.spool.mounter.api.port.MountTarget;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ReactiveMountStrategyTest {
    private static final PartitionKey SOURCE = new PartitionKey("source=payments");

    private final InMemoryEventBus bus = new InMemoryEventBus();
    private final List<MountTarget> mounted = new ArrayList<>();

    @Test
    void execute_transformationStopped_doesNotMountWhenAnEnvelopeIsPersisted() {
        CancellationToken token = started(MountTarget.transformation("payments", SOURCE));

        token.cancel();
        bus.publish(EnvelopePersisted.builder().idempotencyKey(IdempotencyKey.of("k1")).partitionKey(SOURCE).build());

        assertThat(mounted).isEmpty();
    }

    @Test
    void execute_aggregationStopped_doesNotMountWhenSilverIsMounted() {
        CancellationToken token = started(MountTarget.aggregation("payments-total", SOURCE));

        token.cancel();
        bus.publish(EnvelopesMounted.builder().dataMart("silver/payments").partitionKey(SOURCE).build());

        assertThat(mounted).isEmpty();
    }

    private CancellationToken started(MountTarget target) {
        CancellationToken token = CancellationToken.create();
        new ReactiveMountStrategy(target, bus, mounted::add, new ErrorRouter()).execute(token);
        return token;
    }
}
