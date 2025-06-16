package com.automq.stream.s3;

import com.automq.stream.s3.S3Storage.LazyCommit;
import com.automq.stream.s3.wal.RecordOffset;
import com.automq.stream.s3.wal.WriteAheadLog;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class ConfirmWAL {
    private final WriteAheadLog log;
    private final Function<LazyCommit, CompletableFuture<Void>> commitHandle;

    public ConfirmWAL(WriteAheadLog log, Function<LazyCommit, CompletableFuture<Void>> commitHandle) {
        this.log = log;
        this.commitHandle = commitHandle;
    }

    public RecordOffset confirmOffset() {
        return log.confirmOffset();
    }

    /**
     * Commit with lazy timeout.
     * If in [0, lazyLingerMs), there is no other commit happened, then trigger a new commit.
     * @param lazyLingerMs lazy linger milliseconds.
     */
    public CompletableFuture<Void> commit(long lazyLingerMs, boolean awaitTrim) {
        return commitHandle.apply(new LazyCommit(lazyLingerMs, awaitTrim));
    }

    public CompletableFuture<Void> commit(long lazyLingerMs) {
        return commit(lazyLingerMs, true);
    }

}
