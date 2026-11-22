package software.spool.mounter.api;

public enum MountMode {
    TRANSFORMATION {
        @Override
        public String qualify(String dataMart) {
            return "silver/" + dataMart;
        }
    },
    AGGREGATION {
        @Override
        public String qualify(String dataMart) {
            return "gold/" + dataMart;
        }
    };

    public abstract String qualify(String dataMart);

    /** Whether a data mart named with its layer, as {@code silver/payments}, is in the layer this mode writes. */
    public boolean writesTo(String qualifiedDataMart) {
        return qualifiedDataMart.startsWith(qualify(""));
    }
}