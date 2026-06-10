package software.spool.mounter.api.adapter;

import org.junit.jupiter.api.Test;
import software.spool.core.model.vo.PartitionKey;
import software.spool.mounter.api.port.MountTarget;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SequentialPartitionDispatcherTest {

    @Test
    void dispatch_multiplePartitions_executedInOrder() {
        List<MountTarget> handled = new ArrayList<>();
        List<MountTarget> units = List.of(
            MountTarget.transformation("dm1", new PartitionKey("p1")),
            MountTarget.transformation("dm2", new PartitionKey("p2"))
        );

        new SequentialPartitionDispatcher().dispatch(units, t -> handled.add(t));

        assertThat(handled).containsExactlyElementsOf(units);
    }

    @Test
    void dispatch_emptyList_handlerNeverCalled() {
        List<MountTarget> handled = new ArrayList<>();

        new SequentialPartitionDispatcher().dispatch(List.of(), t -> handled.add(t));

        assertThat(handled).isEmpty();
    }
}
