package software.spool.mounter.api.adapter;

import org.junit.jupiter.api.Test;
import software.spool.core.model.vo.PartitionKey;
import software.spool.mounter.api.port.MountTarget;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ThreadPoolPartitionDispatcherTest {

    @Test
    void dispatch_multiplePartitions_allProcessed() {
        Set<MountTarget> handled = ConcurrentHashMap.newKeySet();
        List<MountTarget> units = List.of(
            MountTarget.transformation("dm1", new PartitionKey("p1")),
            MountTarget.transformation("dm2", new PartitionKey("p2")),
            MountTarget.transformation("dm3", new PartitionKey("p3"))
        );
        ThreadPoolPartitionDispatcher dispatcher = new ThreadPoolPartitionDispatcher(Executors.newFixedThreadPool(3));

        dispatcher.dispatch(units, t -> handled.add(t));

        assertThat(handled).containsExactlyInAnyOrderElementsOf(units);
    }

    @Test
    void dispatch_workerThrowsRuntimeException_exceptionPropagated() {
        List<MountTarget> units = List.of(MountTarget.transformation("dm", new PartitionKey("p")));
        ThreadPoolPartitionDispatcher dispatcher = new ThreadPoolPartitionDispatcher(Executors.newSingleThreadExecutor());

        assertThatThrownBy(() -> dispatcher.dispatch(units, t -> { throw new RuntimeException("boom"); }))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("boom");
    }
}
