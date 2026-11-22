package software.spool.mounter.acceptance;

/**
 * The round of the mounter a scenario has built, shared by the step classes, so "the mounter mounts" runs it whichever
 * feature built the mounter. Cucumber hands the same instance to every step class of a scenario.
 */
public class MountRound {
    private Runnable round = () -> {
        throw new IllegalStateException("The scenario has not built a mounter");
    };

    public void runWith(Runnable round) {
        this.round = round;
    }

    public void run() {
        round.run();
    }
}
