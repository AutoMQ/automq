package com.automq.stream.s3.wal.impl.object;

import com.automq.stream.s3.operator.MemoryObjectStorage;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.trace.context.TraceContext;
import com.automq.stream.s3.wal.AppendResult;
import com.automq.stream.s3.wal.RecoverResult;
import com.automq.stream.s3.wal.exception.OverCapacityException;
import com.automq.stream.utils.Time;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ObjectWALServiceTest {
    private ObjectStorage objectStorage;
    private ObjectWALService wal;
    private Random random;

    @BeforeEach
    public void setUp() throws IOException {
        objectStorage = new MemoryObjectStorage();
        wal = new ObjectWALService(Time.SYSTEM, objectStorage, ObjectWALConfig.builder().withMaxBytesInBatch(200).build());
        wal.start();
        random = new Random();
    }

    private ByteBuf generateByteBuf(int size) {
        ByteBuf byteBuf = Unpooled.buffer(size);
        byte[] bytes = new byte[size];
        random.nextBytes(bytes);
        byteBuf.writeBytes(bytes);
        return byteBuf;
    }

    @Test
    public void test() throws OverCapacityException, IOException {
        List<ByteBuf> bufferList = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            ByteBuf byteBuf = generateByteBuf(20);
            bufferList.add(byteBuf);
        }

        List<CompletableFuture<AppendResult.CallbackResult>> futureList = new ArrayList<>();
        for (ByteBuf byteBuf : bufferList) {
            AppendResult result = wal.append(TraceContext.DEFAULT, byteBuf.retainedSlice().asReadOnly(), 0);
            futureList.add(result.future());

            if (futureList.size() % 3 == 0) {
                wal.accumulator().unsafeUpload(false);
                CompletableFuture.allOf(futureList.toArray(new CompletableFuture<?>[]{})).join();
                futureList.clear();
            }
        }

        // Close S3 WAL to flush all buffering data to object storage.
        wal.shutdownGracefully();

        // Recreate S3 WAL.
        wal = new ObjectWALService(Time.SYSTEM, objectStorage, ObjectWALConfig.builder().build());
        wal.start();

        Iterator<RecoverResult> iterator = wal.recover();
        for (ByteBuf byteBuf : bufferList) {
            assertTrue(iterator.hasNext());

            ByteBuf recoveredByteBuf = iterator.next().record();
            assertEquals(byteBuf, recoveredByteBuf);
        }
        assertFalse(iterator.hasNext());
    }
}
