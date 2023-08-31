/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.log.s3;

import com.automq.elasticstream.client.DefaultAppendResult;
import com.automq.elasticstream.client.api.AppendResult;
import com.automq.elasticstream.client.api.ElasticStreamClientException;
import com.automq.elasticstream.client.api.FetchResult;
import com.automq.elasticstream.client.api.RecordBatch;
import com.automq.elasticstream.client.api.RecordBatchWithContext;
import com.automq.elasticstream.client.api.Stream;
import com.automq.elasticstream.client.flatc.header.ErrorCode;
import kafka.log.es.FutureUtil;
import kafka.log.es.RecordBatchWithContextWrapper;
import kafka.log.s3.cache.S3BlockCache;
import kafka.log.s3.model.StreamRecordBatch;
import kafka.log.s3.streams.StreamManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class S3Stream implements Stream {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3Stream.class);
    private final String logIdent;
    private final long streamId;
    private final long epoch;
    private long startOffset;
    final AtomicLong confirmOffset;
    private final AtomicLong nextOffset;
    private final Wal wal;
    private final S3BlockCache blockCache;
    private final StreamManager streamManager;
    private final Status status;

    public S3Stream(long streamId, long epoch, long startOffset, long nextOffset, Wal wal, S3BlockCache blockCache, StreamManager streamManager) {
        this.streamId = streamId;
        this.epoch = epoch;
        this.startOffset = startOffset;
        this.logIdent = "[Stream id=" + streamId + " epoch" + epoch + "]";
        this.nextOffset = new AtomicLong(nextOffset);
        this.confirmOffset = new AtomicLong(nextOffset);
        this.status = new Status();
        this.wal = wal;
        this.blockCache = blockCache;
        this.streamManager = streamManager;
    }


    @Override
    public long streamId() {
        return this.streamId;
    }

    @Override
    public long startOffset() {
        return this.startOffset;
    }

    @Override
    public long nextOffset() {
        return nextOffset.get();
    }

    @Override
    public CompletableFuture<AppendResult> append(RecordBatch recordBatch) {
        if (!status.isWritable()) {
            return FutureUtil.failedFuture(new ElasticStreamClientException(ErrorCode.STREAM_ALREADY_CLOSED, logIdent + " stream is not writable"));
        }
        long offset = nextOffset.getAndAdd(recordBatch.count());
        StreamRecordBatch streamRecordBatch = new StreamRecordBatch(streamId, epoch, offset, recordBatch);
        CompletableFuture<AppendResult> cf = wal.append(streamRecordBatch).thenApply(nil -> {
            updateConfirmOffset(offset + recordBatch.count());
            return new DefaultAppendResult(offset);
        });
        return cf.whenComplete((rst, ex) -> {
            if (ex == null) {
                return;
            }
            // Wal should keep retry append until stream is fenced or wal is closed.
            status.markFenced();
            if (ex instanceof ElasticStreamClientException && ((ElasticStreamClientException) ex).getCode() == ErrorCode.EXPIRED_STREAM_EPOCH) {
                LOGGER.info("{} stream append, stream is fenced", logIdent);
            } else {
                LOGGER.warn("{} stream append fail", logIdent, ex);
            }
        });
    }

    @Override
    public CompletableFuture<FetchResult> fetch(long startOffset, long endOffset, int maxBytes) {
        if (status.isClosed()) {
            return FutureUtil.failedFuture(new ElasticStreamClientException(ErrorCode.STREAM_ALREADY_CLOSED, logIdent + " stream is already closed"));
        }
        long confirmOffset = this.confirmOffset.get();
        if (startOffset < startOffset() || endOffset > confirmOffset) {
            return FutureUtil.failedFuture(
                    new ElasticStreamClientException(
                            ErrorCode.OFFSET_OUT_OF_RANGE_BOUNDS,
                            String.format("fetch range[%s, %s) is out of stream bound [%s, %s)", startOffset, endOffset, startOffset(), confirmOffset)
                    ));
        }
        return blockCache.read(streamId, startOffset, endOffset, maxBytes).thenApply(dataBlock -> {
            List<RecordBatchWithContext> records = dataBlock.getRecords().stream().map(r -> new RecordBatchWithContextWrapper(r.getRecordBatch(), r.getBaseOffset())).collect(Collectors.toList());
            return new DefaultFetchResult(records);
        });
    }

    @Override
    public CompletableFuture<Void> trim(long newStartOffset) {
        if (newStartOffset < this.startOffset) {
            throw new IllegalArgumentException("newStartOffset[" + newStartOffset + "] cannot be less than current start offset["
                    + this.startOffset + "]");
        }
        this.startOffset = newStartOffset;
        return streamManager.trimStream(streamId, epoch, newStartOffset);
    }

    @Override
    public CompletableFuture<Void> close() {
        status.markClosed();
        return streamManager.closeStream(streamId, epoch);
    }

    @Override
    public CompletableFuture<Void> destroy() {
        status.markDestroy();
        startOffset = this.confirmOffset.get();
        return streamManager.deleteStream(streamId, epoch);
    }

    private void updateConfirmOffset(long newOffset) {
        for (; ; ) {
            long oldConfirmOffset = confirmOffset.get();
            if (oldConfirmOffset >= newOffset) {
                break;
            }
            if (confirmOffset.compareAndSet(oldConfirmOffset, newOffset)) {
                break;
            }
        }
    }

    static class DefaultFetchResult implements FetchResult {
        private final List<RecordBatchWithContext> records;

        public DefaultFetchResult(List<RecordBatchWithContext> records) {
            this.records = records;
        }

        @Override
        public List<RecordBatchWithContext> recordBatchList() {
            return records;
        }
    }

    static class Status {
        private static final int CLOSED_MARK = 1;
        private static final int FENCED_MARK = 1 << 1;
        private static final int DESTROY_MARK = 1 << 2;
        private final AtomicInteger status = new AtomicInteger();

        public void markFenced() {
            status.getAndUpdate(operand -> operand | FENCED_MARK);
        }

        public void markClosed() {
            status.getAndUpdate(operand -> operand | CLOSED_MARK);
        }

        public void markDestroy() {
            status.getAndUpdate(operand -> operand | DESTROY_MARK);
        }

        public boolean isClosed() {
            return (status.get() & CLOSED_MARK) != 0;
        }

        public boolean isWritable() {
            return status.get() == 0;
        }
    }
}
