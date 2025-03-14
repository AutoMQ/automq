/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
package kafka.log.streamaspect;

import kafka.log.streamaspect.cache.FileCache;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.utils.PrimitiveRef;
import org.apache.kafka.storage.internals.log.AbortedTxn;
import org.apache.kafka.storage.internals.log.TransactionIndex;
import org.apache.kafka.storage.internals.log.TxnIndexSearchResult;

import com.automq.stream.api.FetchResult;
import com.automq.stream.api.RecordBatchWithContext;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class ElasticTransactionIndex extends TransactionIndex {
    private final StreamSliceSupplier streamSupplier;
    ElasticStreamSlice stream;
    private final FileCache cache;
    private final String path;
    private final long cacheId;
    private volatile LastAppend lastAppend;

    private boolean closed = false;

    public ElasticTransactionIndex(long startOffset, File file, StreamSliceSupplier streamSupplier,
        FileCache cache) throws IOException {
        super(startOffset, file);
        this.streamSupplier = streamSupplier;
        this.stream = streamSupplier.get();
        this.cache = cache;
        this.cacheId = cache.newCacheId();
        this.path = file.getPath();
        lastAppend = new LastAppend(CompletableFuture.completedFuture(null));
    }

    @Override
    public void updateParentDir(File parentDir) {
        // noop implementation. AutoMQ partition doesn't have a real file.
    }

    @Override
    public void append(AbortedTxn abortedTxn) {
        if (closed)
            throw new IllegalStateException("Attempt to append to closed transaction index " + path);
        lastOffset.ifPresent(offset -> {
            if (offset >= abortedTxn.lastOffset())
                throw new IllegalArgumentException("The last offset of appended transactions must increase sequentially, but "
                    + abortedTxn.lastOffset() + " is not greater than current last offset " + offset + " of index "
                    + path);
        });
        lastOffset = OptionalLong.of(abortedTxn.lastOffset());
        long position = stream.nextOffset();
        CompletableFuture<?> cf = stream.append(RawPayloadRecordBatch.of(abortedTxn.buffer().duplicate()));
        lastAppend = new LastAppend(cf);
        cache.put(cacheId, position, Unpooled.wrappedBuffer(abortedTxn.buffer()));
    }

    @Override
    public void flush() throws IOException {
        try {
            lastAppend.cf.get();
        } catch (Throwable e) {
            throw new IOException(e);
        }
    }

    @Override
    public void reset() throws IOException {
        stream = streamSupplier.reset();
        lastOffset = OptionalLong.empty();
    }

    @Override
    public void close() throws IOException {
        closed = true;
    }

    @Override
    public boolean deleteIfExists() throws IOException {
        close();
        return true;
    }

    @Override
    public void renameTo(File f) throws IOException {
        // noop implementation. AutoMQ partition doesn't have a real file.
    }

    @Override
    public void truncateTo(long offset) throws IOException {
        throw new UnsupportedOperationException("truncateTo() is not supported");
    }

    @Override
    public List<AbortedTxn> allAbortedTxns() {
        // it's ok to call super method
        return super.allAbortedTxns();
    }

    @Override
    public TxnIndexSearchResult collectAbortedTxns(long fetchOffset, long upperBoundOffset) {
        // it's ok to call super method
        return super.collectAbortedTxns(fetchOffset, upperBoundOffset);
    }

    @Override
    public void sanityCheck() {
        // noop implementation.
    }

    /**
     * In the case of unclean shutdown, the last entry needs to be recovered from the txn index.
     */
    public OptionalLong loadLastOffset() {
        if (stream.nextOffset() == 0) {
            return OptionalLong.empty();
        } else {
            long nextOffset = stream.nextOffset();
            FetchResult rst = fetchStream(nextOffset - 1, nextOffset, Integer.MAX_VALUE);
            try {
                RecordBatchWithContext record = rst.recordBatchList().get(0);
                ByteBuffer readBuf = record.rawPayload();
                AbortedTxn abortedTxn = new AbortedTxn(readBuf);
                return OptionalLong.of(abortedTxn.lastOffset());
            } finally {
                rst.free();
            }
        }
    }

    public void seal() {
        stream.seal();
    }

    @Override
    protected Iterable<TransactionIndex.AbortedTxnWithPosition> iterable(Supplier<ByteBuffer> allocate) {
        // await last append complete, usually the abort transaction is not frequent, so it's ok to block here.
        LastAppend lastAppend = this.lastAppend;
        try {
            lastAppend.cf.get();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
        int endPosition = (int) stream.confirmOffset();
        PrimitiveRef.IntRef position = PrimitiveRef.ofInt(0);
        Queue<AbortedTxnWithPosition> queue = new ArrayDeque<>();
        return () -> new Iterator<>() {
            /**
             * Note that nextOffset in stream here actually represents the physical size (or position).
             */
            public boolean hasNext() {
                return !queue.isEmpty() || endPosition - position.value >= AbortedTxn.TOTAL_SIZE;
            }

            public AbortedTxnWithPosition next() {
                if (!queue.isEmpty()) {
                    AbortedTxnWithPosition item = queue.poll();
                    if (item.txn.version() > AbortedTxn.CURRENT_VERSION)
                        throw new KafkaException("Unexpected aborted transaction version " + item.txn.version()
                            + " in transaction index " + path + ", current version is "
                            + AbortedTxn.CURRENT_VERSION);
                    return item;
                }
                int endOffset = Math.min(position.value + AbortedTxn.TOTAL_SIZE * 128, endPosition);
                Optional<ByteBuf> cacheDataOpt = cache.get(cacheId, position.value, endOffset - position.value);
                ByteBuf buf;
                if (cacheDataOpt.isPresent()) {
                    buf = cacheDataOpt.get();
                } else {
                    FetchResult records = fetchStream(position.value, endOffset, endOffset - position.value);
                    ByteBuf txnListBuf = Unpooled.buffer(records.recordBatchList().size() * AbortedTxn.TOTAL_SIZE);
                    records.recordBatchList().forEach(r -> txnListBuf.writeBytes(r.rawPayload()));
                    cache.put(cacheId, position.value, txnListBuf);
                    records.free();
                    buf = txnListBuf;
                }
                while (buf.readableBytes() > 0) {
                    AbortedTxn abortedTxn = new AbortedTxn(buf.slice(buf.readerIndex(), AbortedTxn.TOTAL_SIZE).nioBuffer());
                    queue.add(new AbortedTxnWithPosition(abortedTxn, position.value));
                    position.value += AbortedTxn.TOTAL_SIZE;
                    buf.skipBytes(AbortedTxn.TOTAL_SIZE);
                }
                return queue.poll();
            }
        };
    }

    private FetchResult fetchStream(long startOffset, long endOffset, int maxBytes) {
        try {
            return stream.fetch(startOffset, endOffset, maxBytes).get();
        } catch (Throwable e) {
            throw new KafkaException(e);
        }
    }

    static class LastAppend {
        final CompletableFuture<?> cf;

        LastAppend(CompletableFuture<?> cf) {
            this.cf = cf;
        }

    }
}
