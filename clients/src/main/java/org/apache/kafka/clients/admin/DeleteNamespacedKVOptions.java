package org.apache.kafka.clients.admin;

public class DeleteNamespacedKVOptions extends AbstractOptions<DeleteNamespacedKVOptions> {

    private long ifMatchEpoch = 0L;

    public DeleteNamespacedKVOptions ifMatchEpoch(long epoch) {
        this.ifMatchEpoch = epoch;
        return this;
    }

    public long ifMatchEpoch() {
        return ifMatchEpoch;
    }
}
