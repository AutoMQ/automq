/*
 * Copyright 2025, AutoMQ HK Limited.
 *
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

package com.automq.stream.s3.wal.impl.object;

import com.automq.stream.s3.DefaultByteBufSupplier;
import com.automq.stream.s3.metrics.Metrics;
import com.automq.stream.s3.metrics.MetricsConfig;
import com.automq.stream.s3.metrics.MetricsLevel;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.wal.OpenMode;
import com.automq.stream.s3.wal.exception.OverCapacityException;
import com.automq.stream.utils.Time;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;

import static com.automq.stream.s3.wal.common.RecordHeader.RECORD_HEADER_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@Tag("S3Unit")
public class DefaultWriterTest {
    private DefaultWriter writer;
    private MockObjectStorage objectStorage;
    private ConcurrentSkipListMap<Long, ByteBuf> generatedByteBufMap;
    private Random random;

    @BeforeEach
    public void setUp() {
        objectStorage = new MockObjectStorage();
        ObjectWALConfig config = ObjectWALConfig.builder()
            .withMaxBytesInBatch(123)
            .withNodeId(100)
            .withEpoch(1000)
            .withBatchInterval(Long.MAX_VALUE)
            .build();
        writer = new DefaultWriter(Time.SYSTEM, objectStorage, config);
        writer.start();
        generatedByteBufMap = new ConcurrentSkipListMap<>();
        random = new Random();
    }

    @AfterEach
    public void tearDown() {
        objectStorage.triggerAll();
        writer.close();
        objectStorage.close();
    }

    private ByteBuf generateByteBuf(int size) {
        ByteBuf byteBuf = Unpooled.buffer(size);
        byte[] bytes = new byte[size];
        random.nextBytes(bytes);
        byteBuf.writeBytes(bytes);
        return byteBuf;
    }

    /**
     * Given a started writer, when it is closed repeatedly, then it remains closed and cannot restart.
     */
    @Test
    public void testCloseIsIdempotentAndPreventsRestart() {
        writer.close();
        writer.close();
        writer.start();

        assertThrows(IllegalStateException.class, writer::objectList);
    }

    /**
     * Given a pending object WAL append, gauges report the bytes used by the capacity check and its limit;
     * after the append completes and the writer closes, the value returns to zero and the gauges are removed.
     */
    @Test
    public void testUnflushedByteMetricsMatchCapacityCheck() throws Exception {
        writer.close();
        ObjectWALConfig config = ObjectWALConfig.builder()
            .withNodeId(100)
            .withBucketId((short) 2)
            .withType("rc")
            .withEpoch(1000)
            .withManualMode(true)
            .withMaxUnflushedBytes(1)
            .build();
        writer = new DefaultWriter(Time.SYSTEM, objectStorage, config);
        writer.start();

        Metrics.LongGaugeBundle.LongGauge unflushedGauge = gauge("unflushedBytesGauge");
        Metrics.LongGaugeBundle.LongGauge limitGauge = gauge("maxUnflushedBytesGauge");
        StreamRecordBatch first = StreamRecordBatch.of(1, 1, 0, 1, generateByteBuf(16),
            DefaultByteBufSupplier.INSTANCE);
        long firstSize = first.encoded().readableBytes() + RECORD_HEADER_SIZE;
        CompletableFuture<?> append = writer.append(first);
        OverCapacityException error = assertThrows(OverCapacityException.class, () -> writer.append(
            StreamRecordBatch.of(1, 1, 1, 1, generateByteBuf(16), DefaultByteBufSupplier.INSTANCE)));
        assertTrue(error.getMessage().contains(firstSize + " > 1"));

        ObservableLongMeasurement measurement = mock(ObservableLongMeasurement.class);
        recordGauge(unflushedGauge, measurement);
        ArgumentCaptor<Attributes> attributes = ArgumentCaptor.forClass(Attributes.class);
        verify(measurement).record(eq(firstSize), attributes.capture());
        assertEquals("rc", attributes.getValue().get(AttributeKey.stringKey("wal_type")));
        assertEquals(2L, attributes.getValue().get(AttributeKey.longKey("bucket_id")));
        assertEquals(100L, attributes.getValue().get(AttributeKey.longKey("wal_node_id")));
        recordGauge(limitGauge, measurement);
        verify(measurement).record(eq(1L), any());

        writer.flush().get(5, TimeUnit.SECONDS);
        append.get(5, TimeUnit.SECONDS);
        recordGauge(unflushedGauge, measurement);
        verify(measurement).record(eq(0L), any());

        Collection<?> unflushedRegistrations = registrations("UNFLUSHED_BYTES");
        Collection<?> limitRegistrations = registrations("MAX_UNFLUSHED_BYTES");
        assertTrue(unflushedRegistrations.contains(unflushedGauge));
        assertTrue(limitRegistrations.contains(limitGauge));
        writer.close();
        assertFalse(unflushedRegistrations.contains(unflushedGauge));
        assertFalse(limitRegistrations.contains(limitGauge));
    }

    /**
     * Given local and failover writers sharing a bucket and WAL type, both retain the reporting broker's
     * node_id while wal_node_id prevents a completed failover append from masking the local backlog.
     */
    @Test
    public void testLocalAndFailoverMetricsKeepSeparateWalOwners() throws Exception {
        writer.close();
        ObjectWALConfig localConfig = ObjectWALConfig.builder()
            .withNodeId(100).withBucketId((short) 2).withType("rc").withEpoch(1000)
            .withOpenMode(OpenMode.READ_WRITE).withManualMode(true).withMaxUnflushedBytes(1)
            .build();
        ObjectWALConfig failoverConfig = ObjectWALConfig.builder()
            .withNodeId(200).withBucketId((short) 2).withType("rc").withEpoch(1000)
            .withOpenMode(OpenMode.FAILOVER).withManualMode(true).withMaxUnflushedBytes(2)
            .build();
        writer = new DefaultWriter(Time.SYSTEM, objectStorage, localConfig);
        DefaultWriter failoverWriter = new DefaultWriter(Time.SYSTEM, objectStorage, failoverConfig);
        try {
            writer.start();
            failoverWriter.start();
            StreamRecordBatch localRecord = StreamRecordBatch.of(1, 1, 0, 1, generateByteBuf(16),
                DefaultByteBufSupplier.INSTANCE);
            StreamRecordBatch failoverRecord = StreamRecordBatch.of(2, 1, 0, 1, generateByteBuf(32),
                DefaultByteBufSupplier.INSTANCE);
            long localBytes = localRecord.encoded().readableBytes() + RECORD_HEADER_SIZE;
            writer.append(localRecord);
            // FAILOVER appends upload immediately, even in manual mode.
            failoverWriter.append(failoverRecord).get(5, TimeUnit.SECONDS);

            Attributes baseAttributes = Attributes.builder().put("node_id", "100").build();
            for (String fieldName : List.of("unflushedBytesGauge", "maxUnflushedBytesGauge")) {
                Metrics.LongGaugeBundle.LongGauge localGauge = gauge(writer, fieldName);
                Metrics.LongGaugeBundle.LongGauge failoverGauge = gauge(failoverWriter, fieldName);
                configureGauges(baseAttributes, localGauge, failoverGauge);
                ObservableLongMeasurement measurement = mock(ObservableLongMeasurement.class);
                recordGauge(localGauge, measurement);
                recordGauge(failoverGauge, measurement);
                ArgumentCaptor<Attributes> attributes = ArgumentCaptor.forClass(Attributes.class);
                boolean unflushed = fieldName.equals("unflushedBytesGauge");
                verify(measurement).record(eq(unflushed ? localBytes : 1L), attributes.capture());
                Attributes localAttributes = attributes.getValue();
                verify(measurement).record(eq(unflushed ? 0L : 2L), attributes.capture());
                Attributes failoverAttributes = attributes.getValue();
                for (Attributes writerAttributes : List.of(localAttributes, failoverAttributes)) {
                    assertEquals("100", writerAttributes.get(AttributeKey.stringKey("node_id")));
                    assertEquals("rc", writerAttributes.get(AttributeKey.stringKey("wal_type")));
                    assertEquals(2L, writerAttributes.get(AttributeKey.longKey("bucket_id")));
                }
                assertEquals(100L, localAttributes.get(AttributeKey.longKey("wal_node_id")));
                assertEquals(200L, failoverAttributes.get(AttributeKey.longKey("wal_node_id")));
                assertNotEquals(localAttributes, failoverAttributes);
            }
        } finally {
            failoverWriter.close();
        }
    }

    private Metrics.LongGaugeBundle.LongGauge gauge(String fieldName) throws ReflectiveOperationException {
        return gauge(writer, fieldName);
    }

    private Metrics.LongGaugeBundle.LongGauge gauge(DefaultWriter target, String fieldName) throws ReflectiveOperationException {
        Field field = DefaultWriter.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return (Metrics.LongGaugeBundle.LongGauge) field.get(target);
    }

    private void configureGauges(Attributes baseAttributes, Metrics.LongGaugeBundle.LongGauge... gauges)
        throws ReflectiveOperationException {
        Metrics metrics = Metrics.instance();
        Field meterField = Metrics.class.getDeclaredField("meter");
        Field configField = Metrics.class.getDeclaredField("globalConfig");
        meterField.setAccessible(true);
        configField.setAccessible(true);
        Object previousMeter = meterField.get(metrics);
        Object previousConfig = configField.get(metrics);
        try {
            meterField.set(metrics, mock(Meter.class));
            configField.set(metrics, new MetricsConfig(MetricsLevel.INFO, baseAttributes));
            for (Metrics.LongGaugeBundle.LongGauge gauge : gauges) {
                Method setup = gauge.getClass().getDeclaredMethod("setup");
                setup.setAccessible(true);
                setup.invoke(gauge);
            }
        } finally {
            meterField.set(metrics, previousMeter);
            configField.set(metrics, previousConfig);
        }
    }

    private Collection<?> registrations(String fieldName) throws ReflectiveOperationException {
        Field bundleField = DefaultWriter.class.getDeclaredField(fieldName);
        bundleField.setAccessible(true);
        Field gaugesField = Metrics.LongGaugeBundle.class.getDeclaredField("gauges");
        gaugesField.setAccessible(true);
        return (Collection<?>) gaugesField.get(bundleField.get(null));
    }

    private void recordGauge(Metrics.LongGaugeBundle.LongGauge gauge, ObservableLongMeasurement measurement)
        throws ReflectiveOperationException {
        Method record = gauge.getClass().getDeclaredMethod("record", ObservableLongMeasurement.class);
        record.setAccessible(true);
        record.invoke(gauge, measurement);
    }

    // TODO: fix the test
//    @Test
//    public void testOffset() throws OverCapacityException, WALFencedException {
//        ByteBuf byteBuf1 = generateByteBuf(50);
//        CompletableFuture<AppendResult.CallbackResult> future = new CompletableFuture<>();
//        recordAccumulator.append(byteBuf1.readableBytes(), offset -> byteBuf1.retainedSlice().asReadOnly(), future);
//        assertEquals(50, recordAccumulator.nextOffset());
//
//        recordAccumulator.unsafeUpload(true);
//        long flushedOffset = future.join().flushedOffset();
//        assertEquals(50, flushedOffset);
//        assertEquals(50, recordAccumulator.flushedOffset());
//
//        List<RecordAccumulator.WALObject> objectList = recordAccumulator.objectList();
//        assertEquals(1, objectList.size());
//
//        RecordAccumulator.WALObject object = objectList.get(0);
//        assertEquals(WALObjectHeader.DEFAULT_WAL_HEADER_SIZE + 50, object.length());
//        ByteBuf result = objectStorage.rangeRead(new ReadOptions().bucket((short) 0), object.path(), 0, object.length()).join();
//        ByteBuf headerBuf = result.readBytes(WALObjectHeader.DEFAULT_WAL_HEADER_SIZE);
//        WALObjectHeader objectHeader = WALObjectHeader.unmarshal(headerBuf);
//        headerBuf.release();
//        assertEquals(WALObjectHeader.DEFAULT_WAL_MAGIC_CODE, objectHeader.magicCode());
//        assertEquals(0, objectHeader.startOffset());
//        assertEquals(50, objectHeader.length());
//        // The last write timestamp is not set currently.
//        assertEquals(0L, objectHeader.stickyRecordLength());
//        assertEquals(100, objectHeader.nodeId());
//        assertEquals(1000, objectHeader.epoch());
//        assertEquals(-1, objectHeader.trimOffset());
//
//        assertEquals(byteBuf1, result);
//        byteBuf1.release();
//
//        // Test huge record.
//        ByteBuf byteBuf2 = generateByteBuf(50);
//        future = new CompletableFuture<>();
//        recordAccumulator.append(byteBuf2.readableBytes(), offset -> byteBuf2.retainedSlice().asReadOnly(), future);
//        assertEquals(100, recordAccumulator.nextOffset());
//
//        ByteBuf byteBuf3 = generateByteBuf(75);
//        future = new CompletableFuture<>();
//        recordAccumulator.append(byteBuf3.readableBytes(), offset -> byteBuf3.retainedSlice().asReadOnly(), future);
//        assertEquals(175, recordAccumulator.nextOffset());
//
//        recordAccumulator.unsafeUpload(true);
//        flushedOffset = future.join().flushedOffset();
//        assertEquals(175, flushedOffset);
//        assertEquals(175, recordAccumulator.flushedOffset());
//
//        objectList = recordAccumulator.objectList();
//        assertEquals(2, objectList.size());
//
//        object = objectList.get(1);
//        assertEquals(WALObjectHeader.DEFAULT_WAL_HEADER_SIZE + 50 + 75, object.length());
//        result = objectStorage.rangeRead(new ReadOptions().bucket((short) 0), object.path(), 0, object.length()).join();
//        result.skipBytes(WALObjectHeader.DEFAULT_WAL_HEADER_SIZE);
//        CompositeByteBuf compositeBuffer = Unpooled.compositeBuffer();
//        compositeBuffer.addComponents(true, byteBuf2);
//        compositeBuffer.addComponents(true, byteBuf3);
//        assertEquals(compositeBuffer, result);
//        compositeBuffer.release();
//
//        // Test record part
//        ByteBuf byteBuf4 = generateByteBuf(50);
//        future = new CompletableFuture<>();
//        recordAccumulator.append(byteBuf4.readableBytes(), offset -> byteBuf4.retainedSlice().asReadOnly(), future);
//        assertEquals(225, recordAccumulator.nextOffset());
//
//        ByteBuf byteBuf5 = generateByteBuf(50);
//        future = new CompletableFuture<>();
//        recordAccumulator.append(byteBuf5.readableBytes(), offset -> byteBuf5.retainedSlice().asReadOnly(), future);
//        assertEquals(275, recordAccumulator.nextOffset());
//
//        recordAccumulator.unsafeUpload(true);
//        flushedOffset = future.join().flushedOffset();
//        assertEquals(275, flushedOffset);
//        assertEquals(275, recordAccumulator.flushedOffset());
//
//        objectList = recordAccumulator.objectList();
//        assertEquals(4, objectList.size());
//
//        object = objectList.get(2);
//        assertEquals(123, object.length());
//        result = objectStorage.rangeRead(new ReadOptions().bucket((short) 0), object.path(), 0, object.length()).join();
//        result.skipBytes(WALObjectHeader.DEFAULT_WAL_HEADER_SIZE);
//        assertEquals(byteBuf4, result.readBytes(50));
//
//        object = objectList.get(3);
//        compositeBuffer = Unpooled.compositeBuffer();
//        compositeBuffer.addComponents(true, result);
//        result = objectStorage.rangeRead(new ReadOptions().bucket((short) 0), object.path(), 0, object.length()).join();
//        result.skipBytes(WALObjectHeader.DEFAULT_WAL_HEADER_SIZE);
//        compositeBuffer.addComponents(true, result);
//        assertEquals(compositeBuffer, byteBuf5);
//        byteBuf4.release();
//        byteBuf5.release();
//    }
//
//    @Test
//    public void testStrictBatchLimit() throws OverCapacityException, WALFencedException {
//        CompletableFuture<AppendResult.CallbackResult> future = new CompletableFuture<>();
//        recordAccumulator.append(50, offset -> generateByteBuf(50), new CompletableFuture<>());
//        recordAccumulator.append(50, offset -> generateByteBuf(50), new CompletableFuture<>());
//        recordAccumulator.append(50, offset -> generateByteBuf(50), future);
//        assertEquals(150, recordAccumulator.nextOffset());
//
//        recordAccumulator.unsafeUpload(true);
//        future.join();
//
//        assertEquals(2, recordAccumulator.objectList().size());
//
//        // Reset the RecordAccumulator with strict batch limit disabled.
//        recordAccumulator.close();
//        ObjectWALConfig config = ObjectWALConfig.builder()
//            .withMaxBytesInBatch(115)
//            .withNodeId(100)
//            .withEpoch(1000)
//            .withBatchInterval(Long.MAX_VALUE)
//            .withStrictBatchLimit(false)
//            .build();
//        recordAccumulator = new RecordAccumulator(Time.SYSTEM, objectStorage, ReservationService.NOOP, config);
//        recordAccumulator.start();
//
//        assertEquals(2, recordAccumulator.objectList().size());
//
//        future = new CompletableFuture<>();
//        recordAccumulator.append(50, offset -> generateByteBuf(50), new CompletableFuture<>());
//        recordAccumulator.append(50, offset -> generateByteBuf(50), new CompletableFuture<>());
//        recordAccumulator.append(50, offset -> generateByteBuf(50), future);
//        assertEquals(300, recordAccumulator.nextOffset());
//
//
//        recordAccumulator.unsafeUpload(true);
//        future.join();
//
//        assertEquals(3, recordAccumulator.objectList().size());
//    }
//
//    @ParameterizedTest
//    @ValueSource(booleans = {true, false})
//    public void testInMultiThread(boolean strictBathLimit) throws InterruptedException, WALFencedException {
//        recordAccumulator.close();
//
//        ObjectWALConfig config = ObjectWALConfig.builder()
//            .withMaxBytesInBatch(115)
//            .withNodeId(100)
//            .withEpoch(1000)
//            .withBatchInterval(Long.MAX_VALUE)
//            .withStrictBatchLimit(strictBathLimit)
//            .build();
//        recordAccumulator = new RecordAccumulator(Time.SYSTEM, objectStorage, ReservationService.NOOP, config);
//        recordAccumulator.start();
//
//        int threadCount = 10;
//        CountDownLatch startBarrier = new CountDownLatch(threadCount);
//        CountDownLatch stopCountDownLatch = new CountDownLatch(threadCount);
//        List<CompletableFuture<AppendResult.CallbackResult>> futureList = new CopyOnWriteArrayList<>();
//        for (int i = 0; i < threadCount; i++) {
//            new Thread(() -> {
//                startBarrier.countDown();
//                try {
//                    startBarrier.await();
//                } catch (InterruptedException e) {
//                    throw new RuntimeException(e);
//                }
//
//                for (int j = 0; j < 100; j++) {
//                    ByteBuf byteBuf = generateByteBuf(40);
//                    try {
//                        CompletableFuture<AppendResult.CallbackResult> future = new CompletableFuture<>();
//                        long offset = recordAccumulator.append(byteBuf.readableBytes(), o -> byteBuf.retainedSlice().asReadOnly(), future);
//                        futureList.add(future);
//                        generatedByteBufMap.put(offset, byteBuf);
//
//                        Thread.sleep(15);
//                    } catch (Exception e) {
//                        throw new RuntimeException(e);
//                    }
//                }
//
//                stopCountDownLatch.countDown();
//            }).start();
//        }
//
//        stopCountDownLatch.await();
//
//        // Ensure all records are uploaded.
//        try {
//            recordAccumulator.unsafeUpload(true);
//        } catch (Exception e) {
//            fail(e);
//        }
//
//        for (CompletableFuture<AppendResult.CallbackResult> future : futureList) {
//            future.join();
//        }
//
//        assertEquals(100 * threadCount, generatedByteBufMap.size());
//
//        assertFalse(recordAccumulator.objectList().isEmpty());
//
//        CompositeByteBuf source = Unpooled.compositeBuffer();
//        for (ByteBuf buffer : generatedByteBufMap.values()) {
//            source.addComponent(true, buffer);
//        }
//
//        CompositeByteBuf result = Unpooled.compositeBuffer();
//        for (RecordAccumulator.WALObject object : recordAccumulator.objectList()) {
//            ByteBuf buf = objectStorage.rangeRead(new ReadOptions().bucket((short) 0), object.path(), 0, object.length()).join();
//            buf.skipBytes(WALObjectHeader.DEFAULT_WAL_HEADER_SIZE);
//            result.addComponent(true, buf);
//        }
//
//        assertEquals(source, result);
//        source.release();
//        result.release();
//    }
//
//    @Test
//    public void testUploadPeriodically() throws OverCapacityException, WALFencedException {
//        recordAccumulator = new RecordAccumulator(Time.SYSTEM, objectStorage, ReservationService.NOOP, ObjectWALConfig.builder().build());
//        recordAccumulator.start();
//
//        assertTrue(recordAccumulator.objectList().isEmpty());
//
//        ByteBuf byteBuf = generateByteBuf(25);
//        CompletableFuture<AppendResult.CallbackResult> future = new CompletableFuture<>();
//        recordAccumulator.append(byteBuf.readableBytes(), o -> byteBuf.retainedSlice().asReadOnly(), future);
//
//        await().atMost(Duration.ofSeconds(1)).until(future::isDone);
//        assertEquals(1, recordAccumulator.objectList().size());
//    }
//
//    @Test
//    public void testShutdown() throws InterruptedException, OverCapacityException, WALFencedException {
//        ScheduledExecutorService executorService = recordAccumulator.executorService();
//        executorService.shutdown();
//        executorService.awaitTermination(10, TimeUnit.SECONDS);
//
//        ByteBuf byteBuf = generateByteBuf(25);
//        CompletableFuture<AppendResult.CallbackResult> future = new CompletableFuture<>();
//        recordAccumulator.append(byteBuf.readableBytes(), o -> byteBuf.retainedSlice().asReadOnly(), future);
//
//        await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(3)).until(() -> !future.isDone());
//        assertTrue(recordAccumulator.objectList().isEmpty());
//
//        // Flush all data to S3 when close.
//        recordAccumulator.close();
//        assertTrue(future.isDone());
//
//
//        ObjectWALConfig config = ObjectWALConfig.builder()
//            .withMaxBytesInBatch(115)
//            .withNodeId(100)
//            .withEpoch(1000)
//            .withBatchInterval(Long.MAX_VALUE)
//            .withStrictBatchLimit(true)
//            .build();
//        recordAccumulator = new RecordAccumulator(Time.SYSTEM, objectStorage, ReservationService.NOOP, config);
//        recordAccumulator.start();
//        assertEquals(1, recordAccumulator.objectList().size());
//    }
//
//    @Test
//    public void testTrim() throws OverCapacityException, WALFencedException {
//        ByteBuf byteBuf1 = generateByteBuf(50);
//        CompletableFuture<AppendResult.CallbackResult> future = new CompletableFuture<>();
//        recordAccumulator.append(byteBuf1.readableBytes(), offset -> byteBuf1.retainedSlice().asReadOnly(), future);
//
//        ByteBuf byteBuf2 = generateByteBuf(50);
//        future = new CompletableFuture<>();
//        recordAccumulator.append(byteBuf2.readableBytes(), offset -> byteBuf2.retainedSlice().asReadOnly(), future);
//
//        recordAccumulator.unsafeUpload(true);
//        long flushedOffset = future.join().flushedOffset();
//        assertEquals(100, flushedOffset);
//        assertEquals(100, recordAccumulator.flushedOffset());
//        assertEquals(2, recordAccumulator.objectList().size());
//
//        recordAccumulator.trim(50).join();
//        assertEquals(2, recordAccumulator.objectList().size());
//
//        recordAccumulator.trim(100).join();
//        assertEquals(0, recordAccumulator.objectList().size());
//    }
//
//    @Test
//    public void testReset() throws OverCapacityException, WALFencedException {
//        ByteBuf byteBuf1 = generateByteBuf(50);
//        CompletableFuture<AppendResult.CallbackResult> future = new CompletableFuture<>();
//        recordAccumulator.append(byteBuf1.readableBytes(), offset -> byteBuf1.retainedSlice().asReadOnly(), future);
//        recordAccumulator.unsafeUpload(true);
//        future.join();
//
//        ByteBuf byteBuf2 = generateByteBuf(50);
//        future = new CompletableFuture<>();
//        recordAccumulator.append(byteBuf2.readableBytes(), offset -> byteBuf2.retainedSlice().asReadOnly(), future);
//        recordAccumulator.unsafeUpload(true);
//        future.join();
//
//        // Close and restart with another node id.
//        recordAccumulator.close();
//        recordAccumulator = new RecordAccumulator(Time.SYSTEM, objectStorage, ReservationService.NOOP, ObjectWALConfig.builder().withEpoch(System.currentTimeMillis()).build());
//        recordAccumulator.start();
//        assertEquals(0, recordAccumulator.objectList().size());
//
//        // Close and restart with the same node id and higher node epoch.
//        recordAccumulator.close();
//        recordAccumulator = new RecordAccumulator(Time.SYSTEM, objectStorage, ReservationService.NOOP, ObjectWALConfig.builder().withNodeId(100).withEpoch(System.currentTimeMillis()).build());
//        recordAccumulator.start();
//        assertEquals(2, recordAccumulator.objectList().size());
//
//        ByteBuf byteBuf3 = generateByteBuf(50);
//        future = new CompletableFuture<>();
//        recordAccumulator.append(byteBuf3.readableBytes(), offset -> byteBuf3.retainedSlice().asReadOnly(), future);
//        recordAccumulator.unsafeUpload(true);
//        future.join();
//
//        List<RecordAccumulator.WALObject> objectList = recordAccumulator.objectList();
//        assertEquals(3, objectList.size());
//        assertEquals(byteBuf1, objectStorage.read(new ReadOptions().bucket((short) 0), objectList.get(0).path()).join().skipBytes(WALObjectHeader.DEFAULT_WAL_HEADER_SIZE));
//        assertEquals(byteBuf2, objectStorage.read(new ReadOptions().bucket((short) 0), objectList.get(1).path()).join().skipBytes(WALObjectHeader.DEFAULT_WAL_HEADER_SIZE));
//        assertEquals(byteBuf3, objectStorage.read(new ReadOptions().bucket((short) 0), objectList.get(2).path()).join().skipBytes(WALObjectHeader.DEFAULT_WAL_HEADER_SIZE));
//
//        recordAccumulator.reset().join();
//        assertEquals(0, recordAccumulator.objectList().size());
//    }
//
//    @Test
//    public void testSequentiallyComplete() throws WALFencedException, OverCapacityException, InterruptedException {
//        objectStorage.markManualWrite();
//        ByteBuf byteBuf = generateByteBuf(1);
//
//        CompletableFuture<AppendResult.CallbackResult> future0 = new CompletableFuture<>();
//        CompletableFuture<AppendResult.CallbackResult> future1 = new CompletableFuture<>();
//        CompletableFuture<AppendResult.CallbackResult> future2 = new CompletableFuture<>();
//        CompletableFuture<AppendResult.CallbackResult> future3 = new CompletableFuture<>();
//
//        recordAccumulator.append(byteBuf.readableBytes(), offset -> byteBuf.retainedSlice().asReadOnly(), future0);
//        recordAccumulator.unsafeUpload(true);
//        recordAccumulator.append(byteBuf.readableBytes(), offset -> byteBuf.retainedSlice().asReadOnly(), future1);
//        recordAccumulator.append(byteBuf.readableBytes(), offset -> byteBuf.retainedSlice().asReadOnly(), future2);
//        recordAccumulator.unsafeUpload(true);
//        recordAccumulator.append(byteBuf.readableBytes(), offset -> byteBuf.retainedSlice().asReadOnly(), future3);
//        recordAccumulator.unsafeUpload(true);
//
//        // sleep to wait for potential async callback
//        Thread.sleep(100);
//        assertFalse(future0.isDone());
//        assertFalse(future1.isDone());
//        assertFalse(future2.isDone());
//        assertFalse(future3.isDone());
//
//        objectStorage.triggerWrite("1-3");
//        Thread.sleep(100);
//        assertFalse(future0.isDone());
//        assertFalse(future1.isDone());
//        assertFalse(future2.isDone());
//        assertFalse(future3.isDone());
//
//        objectStorage.triggerWrite("0-1");
//        Thread.sleep(100);
//        assertTrue(future0.isDone());
//        assertTrue(future1.isDone());
//        assertTrue(future2.isDone());
//        assertFalse(future3.isDone());
//
//        objectStorage.triggerWrite("3-4");
//        Thread.sleep(100);
//        assertTrue(future0.isDone());
//        assertTrue(future1.isDone());
//        assertTrue(future2.isDone());
//        assertTrue(future3.isDone());
//    }
}
