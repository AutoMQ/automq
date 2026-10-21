/*
 * Copyright 2026, AutoMQ HK Limited. Licensed under Apache-2.0.
 */

package kafka.log.stream.s3.wal;

import kafka.log.stream.s3.node.NodeManager;

import org.apache.kafka.controller.stream.NodeMetadata;

import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.trace.context.TraceContext;
import com.automq.stream.s3.wal.AppendResult;
import com.automq.stream.s3.wal.WalFactory;
import com.automq.stream.s3.wal.WalHandle;
import com.automq.stream.s3.wal.WriteAheadLog;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Verifies that the broker WAL wrapper preserves urgent scheduling and durable completion. */
@Tag("S3Unit")
public class ConfirmWalUrgentAppendTest {
    /** Given a recovery append, do not fall back to ordinary batching or complete before durable upload. */
    @Test
    public void testBrokerWrapperPreservesCoalescedRecoveryAndDurabilityFuture() throws Exception {
        WriteAheadLog inner = mock(WriteAheadLog.class);
        WalFactory factory = mock(WalFactory.class);
        when(factory.build(any(), any())).thenReturn(inner);
        NodeManager node = mock(NodeManager.class);
        when(node.getNodeMetadata()).thenReturn(CompletableFuture.completedFuture(new NodeMetadata(1, 2, "0@s3://bucket", Map.of())));
        WalHandle handle = mock(WalHandle.class);
        when(handle.acquirePermission(anyInt(), anyLong(), any(), any())).thenReturn(CompletableFuture.completedFuture(null));
        ConfirmWal wal = new ConfirmWal(1, 2, "0@s3://bucket", false, factory, node, handle);
        try {
            StreamRecordBatch record = mock(StreamRecordBatch.class);
            CompletableFuture<AppendResult> durable = new CompletableFuture<>();
            when(inner.appendRecovery(TraceContext.DEFAULT, record)).thenReturn(durable);
            CompletableFuture<AppendResult> result = wal.appendRecovery(TraceContext.DEFAULT, record);
            assertSame(durable, result);
            assertFalse(result.isDone());
            verify(inner).appendRecovery(TraceContext.DEFAULT, record);
            verify(inner, never()).append(any(), any());
            verify(inner, never()).appendUrgent(any(), any());
        } finally {
            wal.shutdownGracefully();
        }
    }

    /** Given an urgent append, the wrapper must return the underlying durability future unchanged. */
    @Test
    public void testBrokerWrapperPreservesUrgencyAndDurabilityFuture() throws Exception {
        WriteAheadLog inner = mock(WriteAheadLog.class);
        WalFactory factory = mock(WalFactory.class);
        when(factory.build(any(), any())).thenReturn(inner);
        NodeManager node = mock(NodeManager.class);
        when(node.getNodeMetadata()).thenReturn(CompletableFuture.completedFuture(new NodeMetadata(1, 2, "0@s3://bucket", Map.of())));
        WalHandle handle = mock(WalHandle.class);
        when(handle.acquirePermission(anyInt(), anyLong(), any(), any())).thenReturn(CompletableFuture.completedFuture(null));
        ConfirmWal wal = new ConfirmWal(1, 2, "0@s3://bucket", false, factory, node, handle);
        try {
            StreamRecordBatch record = mock(StreamRecordBatch.class);
            CompletableFuture<AppendResult> durable = new CompletableFuture<>();
            when(inner.appendUrgent(TraceContext.DEFAULT, record)).thenReturn(durable);
            assertSame(durable, wal.appendUrgent(TraceContext.DEFAULT, record));
            verify(inner).appendUrgent(TraceContext.DEFAULT, record);
            verify(inner, never()).append(any(), any());
        } finally {
            wal.shutdownGracefully();
        }
    }
}
