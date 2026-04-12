package software.spool.mounter.api.builder;

import software.spool.core.adapter.watchdog.HttpWatchdogClient;
import software.spool.core.model.watchdog.ModuleIdentity;
import software.spool.core.port.bus.EventBus;
import software.spool.core.port.watchdog.ModuleHeartBeat;
import software.spool.core.utils.polling.PollingHeartbeat;
import software.spool.mounter.api.port.DataLakeReader;

import java.util.Objects;

public class MounterBuilderFactory {
    public static <I, O> ReactiveMounterBuilder<I, O> reactive(EventBus bus) {
        return new Configuration().reactive(bus);
    }

    public static <I> PollingMounterBuilder<I> polling(DataLakeReader<I> reader) {
        return new Configuration().polling(reader);
    }

    public static final class Configuration {
        private final String watchdogUrl;
        private final String moduleId;

        private Configuration(String watchdogUrl, String moduleId) {
            this.watchdogUrl = watchdogUrl;
            this.moduleId = moduleId;
        }

        private Configuration() {
            this(null, "mounter");
        }

        public <I> PollingMounterBuilder<I> polling(DataLakeReader<I> reader) {
            return new PollingMounterBuilder<>(reader, buildHeartbeat(watchdogUrl, moduleId));
        }

        public <I, O> ReactiveMounterBuilder<I, O> reactive(EventBus bus) {
            return new ReactiveMounterBuilder<>(bus, buildHeartbeat(watchdogUrl, moduleId));
        }
    }

    private static ModuleHeartBeat buildHeartbeat(String watchdogUrl, String moduleId) {
        return Objects.isNull(watchdogUrl) ?
                ModuleHeartBeat.NOOP : new PollingHeartbeat(
                new HttpWatchdogClient(watchdogUrl),
                ModuleIdentity.of(moduleId)
        );
    }
}
