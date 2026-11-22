package software.spool.mounter.api.port;

/**
 * Removes from a data mart a result that a {@link DataMartWriter} wrote there, found from the result itself. Removing
 * a result that is not there does nothing.
 */
@FunctionalInterface
public interface DataMartRemover {
    void remove(MountTarget target, PartitionedRecord<?> result);
}
