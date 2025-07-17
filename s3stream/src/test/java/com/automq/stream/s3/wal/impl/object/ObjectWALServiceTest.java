package com.automq.stream.s3.wal.impl.object;

import com.automq.stream.s3.ByteBufAlloc;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.trace.context.TraceContext;
import com.automq.stream.s3.wal.AppendResult;
import com.automq.stream.s3.wal.RecoverResult;
import com.automq.stream.s3.wal.WriteAheadLog;
import com.automq.stream.s3.wal.common.Record;
import com.automq.stream.s3.wal.common.RecordHeader;
import com.automq.stream.s3.wal.common.RecoverResultImpl;
import com.automq.stream.s3.wal.exception.OverCapacityException;
import com.automq.stream.s3.wal.util.WALUtil;
import com.automq.stream.utils.Time;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;

import static com.automq.stream.s3.wal.common.RecordHeader.RECORD_HEADER_SIZE;
import static com.automq.stream.s3.wal.impl.object.ObjectWALService.RecoverIterator.getContinuousFromTrimOffset;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ObjectWALServiceTest {
    private MockObjectStorage objectStorage;
    private ObjectWALService wal;
    private Random random;

    @BeforeEach
    public void setUp() throws IOException {
        objectStorage = new MockObjectStorage();
        ObjectWALConfig config = ObjectWALConfig.builder()
            .withMaxBytesInBatch(118)
            .withBatchInterval(Long.MAX_VALUE)
            .withStrictBatchLimit(true)
            .build();
        new ObjectReservationService(config.clusterId(), objectStorage, objectStorage.bucketId())
            .acquire(config.nodeId(), config.epoch(), false)
            .join();
        wal = new ObjectWALService(Time.SYSTEM, objectStorage, config);
        wal.start();
        random = new Random();
    }

    @AfterEach
    public void tearDown() {
        objectStorage.triggerAll();
        wal.shutdownGracefully();
        objectStorage.close();
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

            if (futureList.size() == 3) {
                wal.accumulator().unsafeUpload(false);
                CompletableFuture.allOf(futureList.toArray(new CompletableFuture<?>[] {})).join();
                futureList.clear();
            }
        }

        if (!futureList.isEmpty()) {
            wal.accumulator().unsafeUpload(false);
            CompletableFuture.allOf(futureList.toArray(new CompletableFuture<?>[] {})).join();
        }

        List<RecordAccumulator.WALObject> objectList = wal.accumulator().objectList();
        assertFalse(objectList.isEmpty());
        assertTrue(objectList.size() < 100);

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
            recoveredByteBuf.release();
        }
        assertFalse(iterator.hasNext());

        // Test recover after trim.
        // Trim the first 2 records.
        wal.trim((RecordHeader.RECORD_HEADER_SIZE + 20) * 2).join();
        assertEquals(66, wal.accumulator().objectList().size());

        iterator = wal.recover();
        long count = 0;
        while (iterator.hasNext()) {
            ByteBuf record = iterator.next().record();
            record.release();
            count++;
        }
        assertEquals(98, count);

        // Trim the first 3 records.
        wal.trim((RecordHeader.RECORD_HEADER_SIZE + 20) * 3).join();
        assertEquals(65, wal.accumulator().objectList().size());

        iterator = wal.recover();
        count = 0;
        while (iterator.hasNext()) {
            ByteBuf record = iterator.next().record();
            record.release();
            count++;
        }
        assertEquals(97, count);
    }

    public static Stream<Arguments> testRecoverIteratorGetContinuousFromTrimOffsetData() {
        return Stream.of(
            Arguments.of(
                "basic",
                List.of(mockWALObject(0, 10), mockWALObject(10, 20), mockWALObject(20, 30)),
                -1L,
                List.of(mockWALObject(0, 10), mockWALObject(10, 20), mockWALObject(20, 30))
            ),
            Arguments.of(
                "empty",
                List.of(),
                -1L,
                List.of()
            ),
            Arguments.of(
                "discontinuous",
                List.of(mockWALObject(0, 10), mockWALObject(20, 30)),
                -1L,
                List.of(mockWALObject(0, 10))
            ),
            Arguments.of(
                "trimmed at boundary",
                List.of(mockWALObject(0, 10), mockWALObject(10, 20), mockWALObject(20, 30)),
                10L,
                List.of(mockWALObject(10, 20), mockWALObject(20, 30))
            ),
            Arguments.of(
                "trimmed in middle",
                List.of(mockWALObject(0, 10), mockWALObject(10, 20), mockWALObject(20, 30)),
                15L,
                List.of(mockWALObject(10, 20), mockWALObject(20, 30))
            ),
            Arguments.of(
                "trimmed nothing",
                List.of(mockWALObject(0, 10), mockWALObject(10, 20), mockWALObject(20, 30)),
                10L,
                List.of(mockWALObject(10, 20), mockWALObject(20, 30))
            ),
            Arguments.of(
                "trimmed all",
                List.of(mockWALObject(0, 10), mockWALObject(10, 20), mockWALObject(20, 30)),
                30L,
                List.of()
            ),
            Arguments.of(
                "trimmed and discontinuous",
                List.of(mockWALObject(0, 10), mockWALObject(10, 20), mockWALObject(30, 40)),
                10L,
                List.of(mockWALObject(10, 20))
            )
        );
    }

    private static RecordAccumulator.WALObject mockWALObject(long start, long end) {
        return new RecordAccumulator.WALObject((short) 0, String.format("%d-%d", start, end), start, end, end - start);
    }

    @ParameterizedTest(name = "Test {index} {0}")
    @MethodSource("testRecoverIteratorGetContinuousFromTrimOffsetData")
    public void testRecoverIteratorGetContinuousFromTrimOffset(
        String name,
        List<RecordAccumulator.WALObject> objectList,
        long trimOffset,
        List<RecordAccumulator.WALObject> expected
    ) {
        List<RecordAccumulator.WALObject> got = getContinuousFromTrimOffset(objectList, trimOffset);
        assertEquals(expected, got, name);
    }

    @Test
    public void testRecoverDiscontinuousObjects() throws IOException, OverCapacityException, InterruptedException {
        objectStorage.markManualWrite();
        ByteBuf byteBuf1 = generateByteBuf(1);
        ByteBuf byteBuf2 = generateByteBuf(1);
        ByteBuf byteBuf3 = generateByteBuf(1);

        // write 3 objects
        wal.append(TraceContext.DEFAULT, byteBuf1.retainedSlice().asReadOnly(), 0);
        wal.accumulator().unsafeUpload(true);
        wal.append(TraceContext.DEFAULT, byteBuf2.retainedSlice().asReadOnly(), 0);
        wal.accumulator().unsafeUpload(true);
        wal.append(TraceContext.DEFAULT, byteBuf3.retainedSlice().asReadOnly(), 0);
        wal.accumulator().unsafeUpload(true);

        // only finish 1st and 3rd
        objectStorage.triggerWrite("0-25");
        objectStorage.triggerWrite("50-75");

        // sleep to wait for potential async callback
        Thread.sleep(100);
        assertEquals(2, wal.accumulator().objectList().size());

        // try recover, and should only got records in the 1st object
        List<RecoverResult> recovered = recover(objectStorage);
        assertEquals(1, recovered.size());
        assertEquals(byteBuf1, recovered.get(0).record());
    }

    @Test
    public void testRecoverFromV0Objects() throws IOException {
        ByteBuf data1 = generateByteBuf(1);
        ByteBuf data2 = generateByteBuf(1);
        ByteBuf data3 = generateByteBuf(1);
        ByteBuf data4 = generateByteBuf(1);

        writeV0Object(objectStorage, data1.retain(), 0);
        writeV0Object(objectStorage, data2.retain(), 25);
        writeV0Object(objectStorage, data3.retain(), 50);
        writeV0Object(objectStorage, data4.retain(), 100);

        List<RecoverResult> recovered = recover(objectStorage);
        assertEquals(List.of(
            new RecoverResultImpl(data1, 0),
            new RecoverResultImpl(data2, 25),
            new RecoverResultImpl(data3, 50)
        ), recovered);
    }

    @Test
    public void testRecoverFromV1Objects() throws IOException {
        ByteBuf data1 = generateByteBuf(1);
        ByteBuf data2 = generateByteBuf(1);
        ByteBuf data3 = generateByteBuf(1);
        ByteBuf data4 = generateByteBuf(1);

        writeV1Object(objectStorage, data1.retain(), 0, -1);
        writeV1Object(objectStorage, data2.retain(), 25, -1);
        writeV1Object(objectStorage, data3.retain(), 50, 0);
        writeV1Object(objectStorage, data4.retain(), 100, 25);

        List<RecoverResult> recovered = recover(objectStorage);
        assertEquals(List.of(
            new RecoverResultImpl(data2, 25),
            new RecoverResultImpl(data3, 50)
        ), recovered);
    }

    @Test
    public void testRecoverFromV0AndV1Objects() throws IOException {
        ByteBuf data1 = generateByteBuf(1);
        ByteBuf data2 = generateByteBuf(1);
        ByteBuf data3 = generateByteBuf(1);
        ByteBuf data4 = generateByteBuf(1);

        writeV0Object(objectStorage, data1.retain(), 0);
        writeV0Object(objectStorage, data2.retain(), 25);
        writeV1Object(objectStorage, data3.retain(), 50, 0);
        writeV1Object(objectStorage, data4.retain(), 100, 25);

        List<RecoverResult> recovered = recover(objectStorage);
        assertEquals(List.of(
            new RecoverResultImpl(data2, 25),
            new RecoverResultImpl(data3, 50)
        ), recovered);
    }

    private void writeV0Object(ObjectStorage objectStorage, ByteBuf data, long startOffset) {
        data = addRecordHeader(data, startOffset);

        String path = wal.accumulator().objectPrefix() + startOffset;

        CompositeByteBuf buffer = ByteBufAlloc.compositeByteBuffer();
        WALObjectHeader header = new WALObjectHeader(startOffset, data.readableBytes(), 0, 0, 0);
        buffer.addComponents(true, header.marshal(), data);

        objectStorage.write(new ObjectStorage.WriteOptions(), path, buffer).join();
    }

    private void writeV1Object(ObjectStorage objectStorage, ByteBuf data, long startOffset, long trimOffset) {
        data = addRecordHeader(data, startOffset);

        long endOffset = startOffset + data.readableBytes();
        String path = wal.accumulator().objectPrefix() + startOffset + "-" + endOffset;

        CompositeByteBuf buffer = ByteBufAlloc.compositeByteBuffer();
        WALObjectHeader header = new WALObjectHeader(startOffset, data.readableBytes(), 0, 0, 0, trimOffset);
        buffer.addComponents(true, header.marshal(), data);

        objectStorage.write(new ObjectStorage.WriteOptions(), path, buffer).join();
    }

    private ByteBuf addRecordHeader(ByteBuf data, long startOffset) {
        ByteBuf header = ByteBufAlloc.byteBuffer(RECORD_HEADER_SIZE);
        Record record = WALUtil.generateRecord(data, header, 0, startOffset);

        CompositeByteBuf buffer = ByteBufAlloc.compositeByteBuffer();
        buffer.addComponents(true, record.header(), record.body());
        return buffer;
    }

    private List<RecoverResult> recover(ObjectStorage objectStorage) throws IOException {
        WriteAheadLog wal = new ObjectWALService(Time.SYSTEM, objectStorage, ObjectWALConfig.builder().build());
        wal.start();
        Iterator<RecoverResult> iterator = wal.recover();
        List<RecoverResult> results = new ArrayList<>();
        while (iterator.hasNext()) {
            results.add(iterator.next());
        }
        wal.shutdownGracefully();
        return results;
    }
}
