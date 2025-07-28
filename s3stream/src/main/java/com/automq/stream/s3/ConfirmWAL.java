package com.automq.stream.s3;

import com.automq.stream.s3.wal.WriteAheadLog;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class ConfirmWAL {
    private final WriteAheadLog log;
    private final Function<Long, CompletableFuture<Void>> commitHandle;

    public ConfirmWAL(WriteAheadLog log, Function<Long, CompletableFuture<Void>> commitHandle) {
        this.log = log;
        this.commitHandle = commitHandle;
    }

    public long confirmOffset() {
        return log.confirmOffset();
    }

    /**
     * Commit with lazy timeout.
     * If in [0, lazyTimeoutMills), there is no other commit happened, then trigger a new commit.
     * @param lazyTimeoutMillis lazy timeout milliseconds.
     */
    public CompletableFuture<Void> commit(long lazyTimeoutMillis) {
        return commitHandle.apply(lazyTimeoutMillis);
    }

}
