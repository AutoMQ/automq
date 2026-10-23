/*
 * Copyright 2026, AutoMQ HK Limited. Licensed under Apache-2.0.
 */

package com.automq.stream.s3.wal.impl.object;

import com.automq.stream.s3.DefaultByteBufSupplier;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.operator.MemoryObjectStorage;
import com.automq.stream.s3.trace.context.TraceContext;
import com.automq.stream.s3.wal.AppendResult;
import com.automq.stream.utils.MockTime;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Covers object-WAL urgent scheduling, successful durability and upload failure propagation. */
@Tag("S3Unit")
@Timeout(15)
public class UrgentAppendTest {
    /** An urgent append flushes preceding data, waits for storage, and leaves later batching unchanged. */
    @Test
    public void testUrgentAppendFlushesBatchButWaitsForDurableUpload() throws Exception {
        ControlledStorage storage = new ControlledStorage();
        ObjectWALService wal = wal(storage);
        try {
            CompletableFuture<AppendResult> ordinary = wal.append(TraceContext.DEFAULT, record(0));
            assertFalse(ordinary.isDone());
            assertEquals(0, storage.writes.get());
            CompletableFuture<AppendResult> urgent = wal.appendUrgent(TraceContext.DEFAULT, record(1));
            storage.started.get(2, TimeUnit.SECONDS);
            assertFalse(ordinary.isDone());
            assertFalse(urgent.isDone());
            storage.release.complete(null);
            AppendResult first = ordinary.get(2, TimeUnit.SECONDS);
            AppendResult second = urgent.get(2, TimeUnit.SECONDS);
            assertEquals(1, storage.writes.get());
            StreamRecordBatch recovered = wal.get(second.recordOffset()).get(2, TimeUnit.SECONDS);
            assertEquals(1, recovered.getBaseOffset());
            recovered.release();
            recovered = wal.get(first.recordOffset()).get(2, TimeUnit.SECONDS);
            assertEquals(0, recovered.getBaseOffset());
            recovered.release();

            // Urgency is per append, not a persistent change to normal batching.
            CompletableFuture<AppendResult> subsequent = wal.append(TraceContext.DEFAULT, record(2));
            assertFalse(subsequent.isDone());
            assertEquals(1, storage.writes.get());
            ((DefaultWriter) wal.writer).flush().get(2, TimeUnit.SECONDS);
            subsequent.get(2, TimeUnit.SECONDS);
        } finally {
            storage.release.complete(null);
            wal.shutdownGracefully();
            storage.close();
        }
    }

    /** A failed urgent upload must fail the append and prevent subsequent successful acknowledgement. */
    @Test
    public void testUrgentUploadFailureDoesNotAcknowledge() throws Exception {
        ControlledStorage storage = new ControlledStorage();
        ObjectWALService wal = wal(storage);
        try {
            CompletableFuture<AppendResult> urgent = wal.appendUrgent(TraceContext.DEFAULT, record(0));
            storage.started.get(2, TimeUnit.SECONDS);
            assertFalse(urgent.isDone());
            storage.release.completeExceptionally(new IOException("injected upload failure"));
            assertThrows(ExecutionException.class, () -> urgent.get(2, TimeUnit.SECONDS));
            assertThrows(ExecutionException.class, () -> wal.appendUrgent(TraceContext.DEFAULT, record(1)).get(2, TimeUnit.SECONDS));
        } finally {
            storage.release.complete(null);
            wal.shutdownGracefully();
            storage.close();
        }
    }

    /** Recovery appends share one short batch, and none acknowledge until the whole upload is durable. */
    @Test
    public void testRecoveryAppendsCoalesceAndWaitForDurability() throws Exception {
        ControlledStorage storage = new ControlledStorage();
        ObjectWALService wal = wal(storage);
        try {
            CompletableFuture<AppendResult> first = wal.appendRecovery(TraceContext.DEFAULT, record(0));
            CompletableFuture<AppendResult> second = wal.appendRecovery(TraceContext.DEFAULT, record(1));
            assertEquals(0, storage.writes.get());
            storage.started.get(2, TimeUnit.SECONDS);
            assertEquals(1, storage.writes.get());
            assertFalse(first.isDone());
            assertFalse(second.isDone());
            storage.release.complete(null);
            first.get(2, TimeUnit.SECONDS);
            second.get(2, TimeUnit.SECONDS);
            assertEquals(1, storage.writes.get());
        } finally {
            storage.release.complete(null);
            wal.shutdownGracefully();
            storage.close();
        }
    }

    /** A failed coalesced recovery upload fails every append; no scheduling event is an ACK. */
    @Test
    public void testRecoveryFailureFailsWholeBatch() throws Exception {
        ControlledStorage storage = new ControlledStorage();
        ObjectWALService wal = wal(storage);
        try {
            CompletableFuture<AppendResult> first = wal.appendRecovery(TraceContext.DEFAULT, record(0));
            CompletableFuture<AppendResult> second = wal.appendRecovery(TraceContext.DEFAULT, record(1));
            storage.started.get(2, TimeUnit.SECONDS);
            storage.release.completeExceptionally(new IOException("injected recovery upload failure"));
            assertThrows(ExecutionException.class, () -> first.get(2, TimeUnit.SECONDS));
            assertThrows(ExecutionException.class, () -> second.get(2, TimeUnit.SECONDS));
        } finally {
            storage.release.complete(null);
            wal.shutdownGracefully();
            storage.close();
        }
    }

    private static ObjectWALService wal(ControlledStorage storage) throws IOException {
        ObjectWALConfig config = ObjectWALConfig.builder().withMaxBytesInBatch(1048576).withBatchInterval(60000).build();
        ObjectWALService wal = new ObjectWALService(new MockTime(), storage, config);
        wal.start();
        return wal;
    }

    private static StreamRecordBatch record(long offset) {
        return StreamRecordBatch.of(7L, 1L, offset, 1, Unpooled.wrappedBuffer(new byte[] {1, 2, 3}), DefaultByteBufSupplier.INSTANCE);
    }

    private static class ControlledStorage extends MemoryObjectStorage {
        final CompletableFuture<Void> started = new CompletableFuture<>();
        final CompletableFuture<Void> release = new CompletableFuture<>();
        final AtomicInteger writes = new AtomicInteger();

        @Override
        public CompletableFuture<WriteResult> write(WriteOptions options, String path, ByteBuf data) {
            writes.incrementAndGet();
            started.complete(null);
            return release.handle((nil, error) -> {
                if (error != null) {
                    data.release();
                    return CompletableFuture.<WriteResult>failedFuture(error);
                }
                return super.write(options, path, data);
            }).thenCompose(future -> future);
        }
    }
}
