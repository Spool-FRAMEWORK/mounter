package software.spool.mounter.api.utils;

import java.time.Duration;

public class PollingPolicy {
    private final Duration interval;

    public PollingPolicy(Duration interval) {
        this.interval = interval;
    }

    public static PollingPolicy every(Duration interval) {
        return new PollingPolicy(interval);
    }

    public boolean shouldPoll(Duration elapsed) {
        return elapsed.compareTo(interval) > 0;
    }
}
