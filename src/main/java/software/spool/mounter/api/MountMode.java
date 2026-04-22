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
}