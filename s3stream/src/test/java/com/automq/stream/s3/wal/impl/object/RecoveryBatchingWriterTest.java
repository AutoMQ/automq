/*
 * Copyright 2026, AutoMQ HK Limited. Licensed under Apache-2.0.
 */

package com.automq.stream.s3.wal.impl.object;

import com.automq.stream.s3.DefaultByteBufSupplier;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.operator.MemoryObjectStorage;
import com.automq.stream.s3.wal.AppendResult;
import com.automq.stream.utils.MockTime;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Exercises actual scheduled uploads while keeping acknowledgement and storage failure semantics. */
@Tag("S3Unit")
@Timeout(15)
public class RecoveryBatchingWriterTest {
    /** Manual WALs leave recovery batching to the router and acknowledge only after its explicit durable flush. */
    @Test
    public void testRecoveryDoesNotUploadManualBatches() throws Exception {
        MockTime time = new MockTime();
        RecoveryBatchingWindow window = new RecoveryBatchingWindow(time);
        ControlledStorage storage = new ControlledStorage();
        ObjectWALConfig config = ObjectWALConfig.builder().withManualMode(true)
            .withBatchInterval(1).withMaxBytesInBatch(1).build();
        DefaultWriter writer = new DefaultWriter(time, storage, config, window);
        writer.start();
        try {
            CompletableFuture<AppendResult> append = writer.appendRecovery(record(0));
            assertTrue(window.startRecovery());
            assertThrows(TimeoutException.class, () -> append.get(30, TimeUnit.MILLISECONDS));
            assertEquals(0, storage.writes.get());
            CompletableFuture<Void> flush = writer.flush();
            storage.started.get(2, TimeUnit.SECONDS);
            assertFalse(append.isDone());
            assertFalse(flush.isDone());
            storage.release.complete(null);
            flush.get(2, TimeUnit.SECONDS);
            append.get(2, TimeUnit.SECONDS);
        } finally {
            storage.release.complete(null);
            writer.close();
            storage.close();
        }
    }

    /** A pending batch is shortened even with no new append, but acknowledgements still wait for storage. */
    @Test
    public void testExistingBatchIsShortenedAndRemainsDurable() throws Exception {
        MockTime time = new MockTime();
        RecoveryBatchingWindow window = new RecoveryBatchingWindow(time);
        ControlledStorage storage = new ControlledStorage();
        DefaultWriter writer = writer(time, storage, window);
        try {
            CompletableFuture<AppendResult> first = writer.append(record(0));
            CompletableFuture<AppendResult> second = writer.append(record(1));
            assertEquals(0, storage.writes.get());
            assertTrue(window.startRecovery());
            storage.started.get(2, TimeUnit.SECONDS);
            assertFalse(first.isDone());
            assertFalse(second.isDone());
            storage.release.complete(null);
            AppendResult a = first.get(2, TimeUnit.SECONDS);
            AppendResult b = second.get(2, TimeUnit.SECONDS);
            assertTrue(a.recordOffset().compareTo(b.recordOffset()) < 0);
            assertEquals(1, storage.writes.get());
            DefaultReader reader = new DefaultReader(storage, writer.config.clusterId(), writer.config.nodeId(), writer.config.type(), time);
            StreamRecordBatch recovered = reader.get(b.recordOffset()).get(2, TimeUnit.SECONDS);
            try {
                assertEquals(1, recovered.getBaseOffset());
                byte[] bytes = new byte[recovered.getPayload().readableBytes()];
                recovered.getPayload().getBytes(recovered.getPayload().readerIndex(), bytes);
                assertArrayEquals(new byte[] {1, 2, 3}, bytes);
            } finally {
                recovered.release();
            }

            // A later append also uses the short interval; the policy is not a one-record hint.
            writer.append(record(2)).get(2, TimeUnit.SECONDS);
            assertEquals(2, storage.writes.get());
            time.sleep(2000);
            CompletableFuture<AppendResult> normal = writer.append(record(3));
            assertFalse(normal.isDone());
            assertEquals(2, storage.writes.get());
            writer.flush().get(2, TimeUnit.SECONDS);
            normal.get(2, TimeUnit.SECONDS);
        } finally {
            storage.release.complete(null);
            writer.close();
            storage.close();
        }
    }

    /** Recovery upload failure must never acknowledge data; subsequent appends observe the failed writer. */
    @Test
    public void testFailedRecoveryUploadDoesNotAcknowledge() throws Exception {
        MockTime time = new MockTime();
        RecoveryBatchingWindow window = new RecoveryBatchingWindow(time);
        ControlledStorage storage = new ControlledStorage();
        DefaultWriter writer = writer(time, storage, window);
        try {
            assertTrue(window.startRecovery());
            CompletableFuture<AppendResult> append = writer.append(record(0));
            storage.started.get(2, TimeUnit.SECONDS);
            assertFalse(append.isDone());
            storage.release.completeExceptionally(new IOException("injected recovery upload failure"));
            assertThrows(ExecutionException.class, () -> append.get(2, TimeUnit.SECONDS));
            assertThrows(ExecutionException.class, () -> writer.append(record(1)).get(2, TimeUnit.SECONDS));
        } finally {
            storage.release.complete(null);
            writer.close();
            storage.close();
        }
    }

    /** Starting recovery without pending data, or after close, never creates an empty upload. */
    @Test
    public void testIdleAndClosedWriterDoNotUpload() {
        MockTime time = new MockTime();
        RecoveryBatchingWindow window = new RecoveryBatchingWindow(time);
        ControlledStorage storage = new ControlledStorage();
        DefaultWriter writer = writer(time, storage, window);
        try {
            assertTrue(window.startRecovery());
            assertEquals(0, storage.writes.get());
            writer.close();
            time.sleep(60000);
            assertTrue(window.startRecovery());
            assertEquals(0, storage.writes.get());
        } finally {
            writer.close();
            storage.close();
        }
    }

    /** One handoff shortens both persistence stages; a combined response still waits for both uploads. */
    @Test
    public void testCoupledWritersShareWindowAndBothRemainDurable() throws Exception {
        MockTime time = new MockTime();
        RecoveryBatchingWindow window = new RecoveryBatchingWindow(time);
        ControlledStorage routerStorage = new ControlledStorage();
        ControlledStorage dataStorage = new ControlledStorage();
        DefaultWriter router = writer(time, routerStorage, window);
        DefaultWriter data = writer(time, dataStorage, window);
        try {
            CompletableFuture<AppendResult> routed = router.append(record(0));
            CompletableFuture<AppendResult> persisted = data.append(record(0));
            CompletableFuture<Void> response = CompletableFuture.allOf(routed, persisted);
            assertTrue(window.startRecovery());
            routerStorage.started.get(2, TimeUnit.SECONDS);
            dataStorage.started.get(2, TimeUnit.SECONDS);
            routerStorage.release.complete(null);
            routed.get(2, TimeUnit.SECONDS);
            assertFalse(response.isDone());
            dataStorage.release.complete(null);
            response.get(2, TimeUnit.SECONDS);
            assertEquals(1, routerStorage.writes.get());
            assertEquals(1, dataStorage.writes.get());
        } finally {
            routerStorage.release.complete(null);
            dataStorage.release.complete(null);
            router.close();
            data.close();
            routerStorage.close();
            dataStorage.close();
        }
    }

    /** Accelerated batching must still obey the configured single-upload admission limit. */
    @Test
    public void testRecoveryKeepsInflightUploadLimit() throws Exception {
        MockTime time = new MockTime();
        RecoveryBatchingWindow window = new RecoveryBatchingWindow(time);
        ControlledStorage storage = new ControlledStorage();
        ObjectWALConfig config = ObjectWALConfig.builder().withMaxBytesInBatch(1048576)
            .withBatchInterval(60000).withMaxInflightUploadCount(1).build();
        DefaultWriter writer = new DefaultWriter(time, storage, config, window);
        writer.start();
        try {
            assertTrue(window.startRecovery());
            CompletableFuture<AppendResult> first = writer.append(record(0));
            storage.started.get(2, TimeUnit.SECONDS);
            CompletableFuture<AppendResult> second = writer.append(record(1));
            CompletableFuture<Void> flushed = writer.flush();
            assertEquals(1, storage.writes.get());
            assertFalse(first.isDone());
            assertFalse(second.isDone());
            assertFalse(flushed.isDone());
            storage.release.complete(null);
            first.get(2, TimeUnit.SECONDS);
            second.get(2, TimeUnit.SECONDS);
            flushed.get(2, TimeUnit.SECONDS);
            assertEquals(2, storage.writes.get());
        } finally {
            storage.release.complete(null);
            writer.close();
            storage.close();
        }
    }

    private static DefaultWriter writer(MockTime time, ControlledStorage storage, RecoveryBatchingWindow window) {
        ObjectWALConfig config = ObjectWALConfig.builder().withMaxBytesInBatch(1048576).withBatchInterval(60000).build();
        DefaultWriter writer = new DefaultWriter(time, storage, config, window);
        writer.start();
        return writer;
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
