package software.spool.mounter.api.exception;

import software.spool.core.exception.SpoolException;

public class MissingPartitionAttributeException extends SpoolException {
    private final String attribute;

    public MissingPartitionAttributeException(String attribute) {
        super("Partition attribute " + attribute + " is missing or null in the mount result");
        this.attribute = attribute;
    }

    public String attribute() {
        return attribute;
    }
}
