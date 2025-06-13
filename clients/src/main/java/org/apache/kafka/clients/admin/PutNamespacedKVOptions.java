package org.apache.kafka.clients.admin;

public class PutNamespacedKVOptions extends AbstractOptions<PutNamespacedKVOptions> {

    private boolean overwrite = false;
    private long ifMatchEpoch = 0L;

    public PutNamespacedKVOptions overwrite(boolean overwrite) {
        this.overwrite = overwrite;
        return this;
    }

    public PutNamespacedKVOptions ifMatchEpoch(long epoch) {
        this.ifMatchEpoch = epoch;
        return this;
    }

    public boolean overwrite() {
        return overwrite;
    }
    public long ifMatchEpoch() {
        return ifMatchEpoch;
    }
}
