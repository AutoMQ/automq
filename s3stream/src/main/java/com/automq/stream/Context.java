package com.automq.stream;

import com.automq.stream.s3.ConfirmWAL;
import com.automq.stream.s3.cache.SnapshotReadCache;

public class Context {
    private SnapshotReadCache snapshotReadCache;
    private ConfirmWAL confirmWAL;

    public static final Context INSTANCE = new Context();

    public static Context instance() {
        return INSTANCE;
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
