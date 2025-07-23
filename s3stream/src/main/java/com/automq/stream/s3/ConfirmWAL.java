package com.automq.stream.s3;

import com.automq.stream.s3.wal.WriteAheadLog;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class ConfirmWAL {
    private final WriteAheadLog log;
    private final Function<Void, CompletableFuture<Void>> commitHandle;

    public ConfirmWAL(WriteAheadLog log, Function<Void, CompletableFuture<Void>> commitHandle) {
        this.log = log;
        this.commitHandle = commitHandle;
    }

    public long confirmOffset() {
        return log.confirmOffset();
    }

    public CompletableFuture<Void> commit() {
        return commitHandle.apply(null);
    }

}
