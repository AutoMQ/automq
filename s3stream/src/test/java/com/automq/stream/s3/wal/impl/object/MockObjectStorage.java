package com.automq.stream.s3.wal.impl.object;

import com.automq.stream.s3.operator.MemoryObjectStorage;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import io.netty.buffer.ByteBuf;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class MockObjectStorage extends MemoryObjectStorage {

    private final Map<String, PendingWrite> pendingWrites = new ConcurrentHashMap<>();
    private boolean manualWrite = false;

    @Override
    public CompletableFuture<WriteResult> write(WriteOptions options, String objectPath, ByteBuf data) {
        Supplier<CompletableFuture<WriteResult>> pendingWrite = () -> super.write(options, objectPath, data);
        if (!manualWrite) {
            return pendingWrite.get();
        }

        CompletableFuture<WriteResult> mockFuture = new CompletableFuture<>();
        String offset = objectPath.substring(objectPath.lastIndexOf('/') + 1);
        pendingWrites.put(offset, new PendingWrite(mockFuture, pendingWrite));

        return mockFuture;
    }

    public void markManualWrite() {
        manualWrite = true;
    }

    public void triggerAll() {
        pendingWrites.values().forEach(PendingWrite::trigger);
        pendingWrites.clear();
    }

    public void triggerWrite(String objectPath) {
        PendingWrite pendingWrite = pendingWrites.remove(objectPath);
        assertNotNull(pendingWrite);
        pendingWrite.trigger();
    }

    private static class PendingWrite {
        public final CompletableFuture<WriteResult> mockFuture;
        public final Supplier<CompletableFuture<WriteResult>> originalFuture;

        public PendingWrite(CompletableFuture<WriteResult> mockFuture,
            Supplier<CompletableFuture<WriteResult>> originalFuture) {
            this.mockFuture = mockFuture;
            this.originalFuture = originalFuture;
        }

        public void trigger() {
            originalFuture.get().whenComplete((result, error) -> {
                if (error != null) {
                    mockFuture.completeExceptionally(error);
                } else {
                    mockFuture.complete(result);
                }
            });
        }
    }
}
