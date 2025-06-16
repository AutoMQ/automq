package com.automq.stream.s3.wal;

import com.automq.stream.s3.model.StreamRecordBatch;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public interface WriteAheadLogV2 {

    WriteAheadLogV2 start() throws IOException;

    void shutdownGracefully();

    /**
     * Append {@link StreamRecordBatch} and return the record offset.
     */
    CompletableFuture<AppendResult> append(StreamRecordBatch streamRecordBatch);

    /**
     * Get record by record offset.
     */
    CompletableFuture<StreamRecordBatch> get(long offset);

    /**
     * Get all records between [startOffset, inclusiveEndOffset].
     */
    void get(long startOffset, long inclusiveEndOffset, Consumer<StreamRecordBatch> streamRecordBatchConsumer);

    Iterator<RecoverResult> recover();

    /**
     * Clear and reset the log.
     */
    CompletableFuture<Void> reset();

    /**
     * Trim the log to the given offset.
     * @param inclusiveOffset the inclusive offset to trim to
     */
    CompletableFuture<Void> trim(long inclusiveOffset);


    class AppendResult {
        private final long offset;

        public AppendResult(long offset) {
            this.offset = offset;
        }

        public long offset() {
            return offset;
        }
    }

}
