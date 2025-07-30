package com.automq.stream;

import com.automq.stream.api.KVClient;
import com.automq.stream.s3.ConfirmWAL;
import com.automq.stream.s3.cache.SnapshotReadCache;

public class Context {
    private SnapshotReadCache snapshotReadCache;
    private ConfirmWAL confirmWAL;
    private KVClient kvClient;

    public static final Context INSTANCE = new Context();

    public static Context instance() {
        return INSTANCE;
    }

    public KVClient kvClient() {
        return kvClient;
    }

    public void kvClient(KVClient kvClient) {
        this.kvClient = kvClient;
    }

    public void snapshotReadCache(SnapshotReadCache snapshotReadCache) {
        this.snapshotReadCache = snapshotReadCache;
    }

    public SnapshotReadCache snapshotReadCache() {
        return snapshotReadCache;
    }

    public void confirmWAL(ConfirmWAL confirmWAL) {
        this.confirmWAL = confirmWAL;
    }

    public ConfirmWAL confirmWAL() {
        return confirmWAL;
    }

}
