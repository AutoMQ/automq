package com.automq.stream.s3.wal.impl.object;

import com.automq.stream.s3.ByteBufAlloc;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.trace.context.TraceContext;
import com.automq.stream.s3.wal.AppendResult;
import com.automq.stream.s3.wal.RecoverResult;
import com.automq.stream.s3.wal.common.Record;
import com.automq.stream.s3.wal.exception.OverCapacityException;
import com.automq.stream.s3.wal.impl.DefaultRecordOffset;
import com.automq.stream.s3.wal.util.WALUtil;
import com.automq.stream.utils.MockTime;
import com.automq.stream.utils.Time;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;

import static com.automq.stream.s3.wal.common.RecordHeader.RECORD_HEADER_SIZE;
import static com.automq.stream.s3.wal.impl.object.RecoverIterator.getContinuousFromTrimOffset;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Timeout(120)
public class ObjectWALServiceTest {
    private MockObjectStorage objectStorage;
    private Random random;
    private Time time;

    @BeforeEach
    public void setUp() throws IOException {
        objectStorage = new MockObjectStorage();
        time = new MockTime();
        random = new Random();
    }

    @AfterEach
    public void tearDown() {
        objectStorage.triggerAll();
        objectStorage.close();
    }

    @Test
    public void testGet_single() throws IOException, OverCapacityException, ExecutionException, InterruptedException {
        ObjectWALConfig config;
        ObjectWALService wal;
        List<CompletableFuture<AppendResult>> appendCfList = new ArrayList<>();
        for (int r = 0; r < 3; r++) {
            config = ObjectWALConfig.builder().withEpoch(r).withMaxBytesInBatch(1024).withBatchInterval(1000).build();
            wal = new ObjectWALService(time, objectStorage, config);
            acquire(config);
            wal.start();

            // append new record and verify
            for (int i = 0; i < 10; i++) {
                appendCfList.add(wal.append(TraceContext.DEFAULT, new StreamRecordBatch(233L, 10, r * 10 + i, 1, generateByteBuf(256))));
            }
            List<CompletableFuture<StreamRecordBatch>> getCfList = new ArrayList<>();
            for (int i = 0; i < appendCfList.size(); i++) {
                AppendResult appendRst = appendCfList.get(i).get();
                getCfList.add(wal.get(appendRst.recordOffset()));
            }
            for (int i = 0; i < getCfList.size(); i++) {
                StreamRecordBatch record = getCfList.get(i).get();
                assertEquals(233L, record.getStreamId());
                assertEquals(i, record.getBaseOffset());
            }

            // restart wal and test get with multiple wal epoch
            wal.shutdownGracefully();
        }
    }

    @Test
    public void testGet_batch() throws Exception {
        ObjectWALConfig config;
        ObjectWALService wal;
        List<CompletableFuture<AppendResult>> appendCfList = new ArrayList<>();
        for (int r = 0; r < 3; r++) {
            config = ObjectWALConfig.builder().withEpoch(r).withMaxBytesInBatch(1024).withBatchInterval(1000).build();
            wal = new ObjectWALService(time, objectStorage, config);
            acquire(config);
            wal.start();
            // append new record and verify
            for (int i = 0; i < 10; i++) {
                appendCfList.add(wal.append(TraceContext.DEFAULT, new StreamRecordBatch(233L, 10, r * 10 + i, 1, generateByteBuf(256))));
            }
            ((DefaultWriter) (wal.writer)).flush().join();
            for (int i = 0; i < appendCfList.size() - 3; i++) {
                List<StreamRecordBatch> records = wal.get(
                    DefaultRecordOffset.of(appendCfList.get(i).get().recordOffset()),
                    DefaultRecordOffset.of(appendCfList.get(i + 3).get().recordOffset())
                ).get();
                assertEquals(3, records.size());
                for (int j = 0; j < 3; j++) {
                    assertEquals(i + j, records.get(j).getBaseOffset());
                }
            }
            // TODO: wal end offset

            // restart wal and test get with multiple wal epoch
            wal.shutdownGracefully();
        }
    }

    @Test
    public void testTrim() throws Exception {
        ObjectWALConfig config = ObjectWALConfig.builder().withEpoch(1L).withMaxBytesInBatch(1024).withBatchInterval(1000).build();
        ObjectWALService wal = new ObjectWALService(time, objectStorage, config);
        acquire(config);
        wal.start();

        List<CompletableFuture<AppendResult>> appendCfList = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            appendCfList.add(wal.append(TraceContext.DEFAULT, new StreamRecordBatch(233L, 0, 100L + i, 1, generateByteBuf(1))));
            if (i % 2 == 0) {
                ((DefaultWriter) (wal.writer)).flush().join();
            }
        }

        wal.trim(appendCfList.get(1).get().recordOffset()).get();

        wal.shutdownGracefully();

        wal = new ObjectWALService(time, objectStorage, config);
        wal.start();

        List<RecoverResult> records = new ArrayList<>();
        wal.recover().forEachRemaining(records::add);

        assertEquals(6, records.size());
        for (int i = 0; i < records.size(); i++) {
            assertEquals(102L + i, records.get(i).record().getBaseOffset());
        }
    }

    @Test
    public void testRecover() throws Exception {
        ObjectWALConfig config;
        ObjectWALService wal;
        int trimIndex = 0;
        List<CompletableFuture<AppendResult>> appendCfList = new ArrayList<>();
        for (int r = 0; r < 4; r++) {
            config = ObjectWALConfig.builder().withEpoch(r).withMaxBytesInBatch(1024).withBatchInterval(1000).build();
            wal = new ObjectWALService(time, objectStorage, config);
            acquire(config);
            wal.start();
            List<RecoverResult> records = new ArrayList<>();
            wal.recover().forEachRemaining(records::add);
            // expect keep all records after trim offset
            for (int i = 0; i < records.size(); i++) {
                assertEquals(trimIndex + i + 1, records.get(i).record().getBaseOffset());
            }
            if (r == 3) {
                break;
            }
            for (int i = 0; i < 10; i++) {
                appendCfList.add(wal.append(TraceContext.DEFAULT, new StreamRecordBatch(233L, 10, r * 10 + i, 1, generateByteBuf(256))));
            }
            ((DefaultWriter) (wal.writer)).flush().join();
            trimIndex = r * 9;
            wal.trim(appendCfList.get(trimIndex).get().recordOffset()).get();
            wal.shutdownGracefully();
        }
    }

    @Test
    public void testReset() throws Exception {
        ObjectWALConfig config;
        ObjectWALService wal;
        int resetIndex = 0;
        List<CompletableFuture<AppendResult>> appendCfList = new ArrayList<>();
        for (int r = 0; r < 4; r++) {
            config = ObjectWALConfig.builder().withEpoch(r).withMaxBytesInBatch(1024).withBatchInterval(1000).build();
            wal = new ObjectWALService(time, objectStorage, config);
            acquire(config);
            wal.start();
            List<RecoverResult> records = new ArrayList<>();
            wal.recover().forEachRemaining(records::add);
            if (r != 0) {
                assertEquals(10, records.size());
            }
            for (int i = 0; i < records.size(); i++) {
                assertEquals(resetIndex + i, records.get(i).record().getBaseOffset());
            }
            resetIndex = appendCfList.size();
            wal.reset().get();
            for (int i = 0; i < 10; i++) {
                appendCfList.add(wal.append(TraceContext.DEFAULT, new StreamRecordBatch(233L, 10, r * 10 + i, 1, generateByteBuf(256))));
            }
            wal.shutdownGracefully();
        }
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

    private static WALObject mockWALObject(long start, long end) {
        return new WALObject((short) 0, String.format("%d-%d", start, end), 0, start, end, end - start);
    }

    @ParameterizedTest(name = "Test {index} {0}")
    @MethodSource("testRecoverIteratorGetContinuousFromTrimOffsetData")
    public void testRecoverIteratorGetContinuousFromTrimOffset(
        String name,
        List<WALObject> objectList,
        long trimOffset,
        List<WALObject> expected
    ) {
        List<WALObject> got = getContinuousFromTrimOffset(objectList, trimOffset);
        assertEquals(expected, got, name);
    }

    @Test
    public void testRecoverDiscontinuousObjects() throws IOException, OverCapacityException, InterruptedException, ExecutionException {
        ObjectWALConfig config = ObjectWALConfig.builder().withEpoch(1L).withMaxBytesInBatch(1024).withBatchInterval(1000).build();
        ObjectWALService wal = new ObjectWALService(time, objectStorage, config);
        acquire(config);
        wal.start();

        // write 4 objects
        for (int i = 0; i < 4; i++) {
            wal.append(TraceContext.DEFAULT, new StreamRecordBatch(233L, 0, 100L + i, 1, generateByteBuf(1)));
            ((DefaultWriter) (wal.writer)).flush().join();
        }

        wal.shutdownGracefully();

        // Delete the 3nd wal object to mock it upload fail.
        String nodePrefix = ObjectUtils.nodePrefix(config.clusterId(), config.nodeId());
        WALObject walObject = ObjectUtils.parse(objectStorage.list(nodePrefix).get()).get(2);
        objectStorage.delete(List.of(new ObjectStorage.ObjectPath(objectStorage.bucketId(), walObject.path()))).get();

        wal = new ObjectWALService(time, objectStorage, config);
        wal.start();

        List<RecoverResult> records = new ArrayList<>();
        wal.recover().forEachRemaining(records::add);

        assertEquals(2, records.size());
        assertEquals(100L, records.get(0).record().getBaseOffset());
        assertEquals(101L, records.get(1).record().getBaseOffset());
    }

    @Test
    public void testRecoverFromV0Objects() throws IOException {
        ObjectWALConfig config = ObjectWALConfig.builder().withEpoch(1L).withMaxBytesInBatch(1024).withBatchInterval(1000).build();

        long startOffset = 0L;
        for (int i = 0; i < 4; i++) {
            startOffset = writeV0Object(config, new StreamRecordBatch(233L, 0, 100L + i, 1, generateByteBuf(1)).encoded(), startOffset);
        }

        ObjectWALService wal = new ObjectWALService(time, objectStorage, config);
        acquire(config);
        wal.start();

        List<RecoverResult> records = new ArrayList<>();
        wal.recover().forEachRemaining(records::add);

        assertEquals(4, records.size());
        for (int i = 0; i < 4; i++) {
            assertEquals(100L + i, records.get(i).record().getBaseOffset());
        }
    }

    @Test
    public void testRecoverFromV0AndV1Objects() throws IOException {
        ObjectWALConfig config = ObjectWALConfig.builder().withEpoch(1L).withMaxBytesInBatch(1024).withBatchInterval(1000).build();
        long nextOffset = 0L;
        nextOffset = writeV0Object(config, new StreamRecordBatch(233L, 0, 100L, 1, generateByteBuf(1)).encoded(), nextOffset);
        long record1Offset = nextOffset;
        nextOffset = writeV0Object(config, new StreamRecordBatch(233L, 0, 101L, 1, generateByteBuf(1)).encoded(), nextOffset);
        nextOffset = writeV1Object(config, new StreamRecordBatch(233L, 0, 102L, 1, generateByteBuf(1)).encoded(), nextOffset, false, 0);
        nextOffset = writeV1Object(config, new StreamRecordBatch(233L, 0, 103L, 1, generateByteBuf(1)).encoded(), nextOffset, false, record1Offset);

        ObjectWALService wal = new ObjectWALService(time, objectStorage, config);
        acquire(config);
        wal.start();
        List<RecoverResult> records = new ArrayList<>();
        wal.recover().forEachRemaining(records::add);

        assertEquals(2, records.size());
        for (int i = 2; i < 4; i++) {
            assertEquals(100L + i, records.get(i - 2).record().getBaseOffset());
        }
    }

    private long writeV0Object(ObjectWALConfig config, ByteBuf data, long startOffset) {
        data = addRecordHeader(data, startOffset);
        long endOffset = startOffset + data.readableBytes();

        String path = ObjectUtils.genObjectPathV0(ObjectUtils.nodePrefix(config.clusterId(), config.nodeId()), config.epoch(), startOffset);

        CompositeByteBuf buffer = ByteBufAlloc.compositeByteBuffer();
        WALObjectHeader header = new WALObjectHeader(startOffset, data.readableBytes(), 0, 0, 0);
        buffer.addComponents(true, header.marshal(), data);

        objectStorage.write(new ObjectStorage.WriteOptions(), path, buffer).join();
        return endOffset;
    }

    private long writeV1Object(ObjectWALConfig config, ByteBuf data, long startOffset, boolean align, long trimOffset) {
        data = addRecordHeader(data, startOffset);
        long endOffset;
        if (align) {
            endOffset = ObjectUtils.ceilAlignOffset(startOffset);
        } else {
            endOffset = startOffset + data.readableBytes();
        }
        String path = ObjectUtils.genObjectPathV1(ObjectUtils.nodePrefix(config.clusterId(), config.nodeId()), config.epoch(), startOffset, endOffset);

        CompositeByteBuf buffer = ByteBufAlloc.compositeByteBuffer();
        WALObjectHeader header = new WALObjectHeader(startOffset, data.readableBytes(), 0, 0, 0, trimOffset);
        buffer.addComponents(true, header.marshal(), data);

        objectStorage.write(new ObjectStorage.WriteOptions(), path, buffer).join();
        return endOffset;
    }

    private ByteBuf addRecordHeader(ByteBuf data, long startOffset) {
        ByteBuf header = ByteBufAlloc.byteBuffer(RECORD_HEADER_SIZE);
        Record record = WALUtil.generateRecord(data, header, 0, startOffset);

        CompositeByteBuf buffer = ByteBufAlloc.compositeByteBuffer();
        buffer.addComponents(true, record.header(), record.body());
        return buffer;
    }

    private void acquire(ObjectWALConfig config) {
        new ObjectReservationService(config.clusterId(), objectStorage, objectStorage.bucketId())
            .acquire(config.nodeId(), config.epoch(), false)
            .join();
    }

    private ByteBuf generateByteBuf(int size) {
        ByteBuf byteBuf = Unpooled.buffer(size);
        byte[] bytes = new byte[size];
        random.nextBytes(bytes);
        byteBuf.writeBytes(bytes);
        return byteBuf;
    }
}
