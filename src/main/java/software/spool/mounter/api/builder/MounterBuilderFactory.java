package software.spool.mounter.api.builder;

import software.spool.core.port.EventBus;
import software.spool.core.utils.ErrorRouter;
import software.spool.mounter.api.port.DataLakeReader;
import software.spool.mounter.internal.scheduler.ThreadedPollingScheduler;

public class MounterBuilderFactory {
    public static <I, O> ReactiveMounterBuilder<I, O> reactive(EventBus bus) {
        return new ReactiveMounterBuilder<>(bus, new ErrorRouter());
    }

    public static <I> PollingMounterBuilder<I, ?> polling(DataLakeReader<I> reader) {
        return new PollingMounterBuilder<>(reader, new ThreadedPollingScheduler(), new ErrorRouter());
    }
}
