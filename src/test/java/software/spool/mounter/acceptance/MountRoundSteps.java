package software.spool.mounter.acceptance;

import io.cucumber.java.en.When;

public class MountRoundSteps {
    private final MountRound round;

    public MountRoundSteps(MountRound round) {
        this.round = round;
    }

    @When("the mounter mounts")
    public void theMounterMounts() {
        round.run();
    }
}
