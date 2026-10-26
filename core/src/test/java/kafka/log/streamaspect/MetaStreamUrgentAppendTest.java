/*
 * Copyright 2026, AutoMQ HK Limited. Licensed under Apache-2.0.
 */

package kafka.log.streamaspect;

import kafka.log.streamaspect.reassignment.FastPartitionReassignmentManager;

import org.apache.kafka.common.Uuid;

import com.automq.stream.api.AppendResult;
import com.automq.stream.api.RecordBatch;
import com.automq.stream.api.Stream;
import com.automq.stream.s3.context.AppendContext;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Verifies that metadata bypasses optional batching without bypassing durable completion. */
@Tag("S3Unit")
public class MetaStreamUrgentAppendTest {
    /** Given a metadata append, request urgency on a private context and wait for durable storage. */
    @Test
    public void testMetadataBypassesOptionalBatchingButNotDurability() {
        Stream inner = mock(Stream.class);
        CompletableFuture<AppendResult> durable = new CompletableFuture<>();
        when(inner.append(any(AppendContext.class), any(RecordBatch.class))).thenReturn(durable);
        MetaStream meta = new MetaStream(inner, mock(ScheduledExecutorService.class), "test", Uuid.ZERO_UUID, 0,
            FastPartitionReassignmentManager.disabled());
        CompletableFuture<AppendResult> result = meta.append(MetaKeyValue.of(MetaStream.PARTITION_META_KEY, ByteBuffer.allocate(0)));
        ArgumentCaptor<AppendContext> context = ArgumentCaptor.forClass(AppendContext.class);
        verify(inner).append(context.capture(), any(RecordBatch.class));
        assertTrue(context.getValue().urgent());
        assertFalse(AppendContext.DEFAULT.urgent());
        assertFalse(result.isDone());
        durable.complete(() -> 0L);
        assertTrue(result.isDone());
    }

    /** Given durable metadata, coalesce compaction but wait for every rewrite, trim and stream close. */
    @Test
    public void testCloseCoalescesCompactionWithoutBypassingDurability() throws Exception {
        Stream inner = mock(Stream.class);
        CompletableFuture<Void> streamClosed = new CompletableFuture<>();
        when(inner.close()).thenReturn(streamClosed);
        when(inner.trim(anyLong())).thenReturn(CompletableFuture.completedFuture(null));
        CompletableFuture<AppendResult> firstRewrite = new CompletableFuture<>();
        CompletableFuture<AppendResult> lastRewrite = new CompletableFuture<>();
        AtomicLong offset = new AtomicLong();
        when(inner.nextOffset()).thenAnswer(invocation -> offset.get());
        when(inner.append(any(AppendContext.class), any(RecordBatch.class))).thenAnswer(invocation -> {
            long index = offset.incrementAndGet();
            return index == 4 ? firstRewrite : index == 5 ? lastRewrite
                : CompletableFuture.completedFuture((AppendResult) () -> index - 1L);
        });
        MetaStream meta = new MetaStream(inner, mock(ScheduledExecutorService.class), "test", Uuid.ZERO_UUID, 0,
            FastPartitionReassignmentManager.disabled());
        meta.replay();
        meta.append(MetaKeyValue.of("first", ByteBuffer.allocate(0))).join();
        meta.append(MetaKeyValue.of("second", ByteBuffer.allocate(0))).join();
        meta.append(MetaKeyValue.of("third", ByteBuffer.allocate(0))).join();
        CompletableFuture<Void> result = meta.close();
        assertSame(result, meta.close());
        assertFalse(result.isDone());
        assertTrue(meta.isFenced());
        ArgumentCaptor<AppendContext> contexts = ArgumentCaptor.forClass(AppendContext.class);
        verify(inner, times(5)).append(contexts.capture(), any(RecordBatch.class));
        assertFalse(contexts.getAllValues().get(3).urgent());
        assertTrue(contexts.getAllValues().get(4).urgent());
        verify(inner, never()).trim(anyLong());
        verify(inner, never()).close();
        lastRewrite.complete(() -> 4L);
        verify(inner, never()).trim(anyLong());
        verify(inner, never()).close();
        firstRewrite.complete(() -> 3L);
        verify(inner).trim(2);
        verify(inner).close();
        assertEquals(5, offset.get());
        streamClosed.complete(null);
        result.join();
        assertTrue(meta.isFenced());
    }

    /** Given stream-close failure, propagate it and retain the frozen admission state. */
    @Test
    public void testClosePropagatesStreamFailure() {
        Stream inner = mock(Stream.class);
        IllegalStateException failure = new IllegalStateException("close failed");
        when(inner.close()).thenReturn(CompletableFuture.failedFuture(failure));
        MetaStream meta = new MetaStream(inner, mock(ScheduledExecutorService.class), "test", Uuid.ZERO_UUID, 0,
            FastPartitionReassignmentManager.disabled());
        assertSame(failure, assertThrows(java.util.concurrent.CompletionException.class, () -> meta.close().join()).getCause());
        assertTrue(meta.isFenced());
    }

    /** Given an admitted metadata append, close freezes admission and waits for durability before closing storage. */
    @Test
    public void testCloseWaitsForAdmittedMutation() {
        Stream inner = mock(Stream.class);
        CompletableFuture<AppendResult> appendDurable = new CompletableFuture<>();
        CompletableFuture<Void> streamClosed = new CompletableFuture<>();
        when(inner.append(any(AppendContext.class), any(RecordBatch.class))).thenReturn(appendDurable);
        when(inner.close()).thenReturn(streamClosed);
        MetaStream meta = new MetaStream(inner, mock(ScheduledExecutorService.class), "test", Uuid.ZERO_UUID, 0,
            FastPartitionReassignmentManager.disabled());
        CompletableFuture<AppendResult> append = meta.append(MetaKeyValue.of("pending", ByteBuffer.allocate(0)));
        CompletableFuture<Void> result = meta.close();
        assertSame(result, meta.close());
        assertTrue(meta.append(MetaKeyValue.of("late", ByteBuffer.allocate(0))).isCompletedExceptionally());
        verify(inner, never()).close();
        assertFalse(result.isDone());
        appendDurable.complete(() -> 0L);
        append.join();
        verify(inner).close();
        assertFalse(result.isDone());
        streamClosed.complete(null);
        result.join();
        assertTrue(meta.isFenced());
    }
}
