/*
 * Copyright 2026, AutoMQ HK Limited.
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

package com.automq.stream.s3.operator;

import com.automq.stream.s3.metrics.operations.S3ReadPurpose;
import com.automq.stream.s3.metrics.operations.S3RequestSource;
import com.automq.stream.s3.metrics.stats.OperationLatencyMetrics;
import com.automq.stream.s3.metrics.wrapper.DeltaHistogram;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Verifies real GET/PUT completion observations, including merging and speculative retries. */
@Tag("S3Unit")
@Timeout(10)
class ObjectStorageSourceTest {
    private AbstractObjectStorage storage;

    @BeforeEach
    void setUp() {
        storage = spy(new MemoryObjectStorage(true));
    }

    @AfterEach
    void tearDown() {
        storage.close();
    }

    /** Normal retries retain ownership and read purpose while recording the existing failure/success deltas. */
    @Test
    void normalRetriesPreserveSource() throws Exception {
        doReturn(0).when(storage).retryDelay(any(), anyInt());
        doReturn(CompletableFuture.failedFuture(new TimeoutException()), CompletableFuture.completedFuture(null))
            .when(storage).doWrite(any(), anyString(), any());
        long puts = count("put_object", S3RequestSource.DELTA_WAL, "success");
        long failedPuts = count("put_object", S3RequestSource.DELTA_WAL, "failed");
        storage.write(new ObjectStorage.WriteOptions().source(S3RequestSource.DELTA_WAL), "key", data()).get();
        assertEquals(puts + 1, count("put_object", S3RequestSource.DELTA_WAL, "success"));
        assertEquals(failedPuts + 1, count("put_object", S3RequestSource.DELTA_WAL, "failed"));
        verify(storage, times(2)).doWrite(argThat(o -> o.source() == S3RequestSource.DELTA_WAL), eq("key"), any());

        doReturn(CompletableFuture.failedFuture(new TimeoutException()), CompletableFuture.completedFuture(data()))
            .when(storage).doRangeRead(any(), anyString(), anyLong(), anyLong());
        long gets = count("get_object", S3RequestSource.ROUTER_WAL, "success", S3ReadPurpose.SNAPSHOT_REPLAY);
        long failedGets = count("get_object", S3RequestSource.ROUTER_WAL, "failed", S3ReadPurpose.SNAPSHOT_REPLAY);
        storage.mergedRangeRead(new ObjectStorage.ReadOptions().source(S3RequestSource.ROUTER_WAL)
            .readPurpose(S3ReadPurpose.SNAPSHOT_REPLAY), "key", 0, 64)
            .get().release();
        await().untilAsserted(() -> assertEquals(failedGets + 1,
            count("get_object", S3RequestSource.ROUTER_WAL, "failed", S3ReadPurpose.SNAPSHOT_REPLAY)));
        assertEquals(gets + 1, count("get_object", S3RequestSource.ROUTER_WAL, "success", S3ReadPurpose.SNAPSHOT_REPLAY));
        verify(storage, times(2)).doRangeRead(argThat(o -> o.source() == S3RequestSource.ROUTER_WAL
            && o.readPurpose() == S3ReadPurpose.SNAPSHOT_REPLAY), eq("key"), eq(0L), eq(64L));
    }

    /** Attribution differences never split or double-count a merge, and caller mutation cannot relabel queued reads. */
    @ParameterizedTest
    @CsvSource({"ROUTER_WAL,ROUTER_WAL,ROUTER_WAL,ROUTER_PRODUCE,ROUTER_PRODUCE,ROUTER_PRODUCE",
        "ROUTER_WAL,ROUTER_WAL,ROUTER_WAL,ROUTER_PRODUCE,SNAPSHOT_REPLAY,MIXED",
        "ROUTER_WAL,DELTA_WAL,MIXED,ROUTER_PRODUCE,OTHER,MIXED",
        "ROUTER_WAL,UNCLASSIFIED,MIXED,OTHER,OTHER,OTHER"})
    void mergedReadsHaveOneAttribution(S3RequestSource first, S3RequestSource second, S3RequestSource expected,
        S3ReadPurpose firstPurpose, S3ReadPurpose secondPurpose, S3ReadPurpose expectedPurpose) throws Exception {
        storage.write(ObjectStorage.WriteOptions.DEFAULT, "key", data()).get();
        long before = count("get_object", expected, "success");
        long purposeBefore = count("get_object", expected, "success", expectedPurpose);
        ObjectStorage.ReadOptions options = new ObjectStorage.ReadOptions().source(first).readPurpose(firstPurpose);
        CompletableFuture<ByteBuf> left = storage.rangeRead(options, "key", 0, 32);
        CompletableFuture<ByteBuf> right = storage.rangeRead(new ObjectStorage.ReadOptions().source(second)
            .readPurpose(secondPurpose), "key", 32, 64);
        options.source(S3RequestSource.MIXED).readPurpose(S3ReadPurpose.OTHER);
        storage.tryMergeRead();
        left.get().release();
        right.get().release();
        assertEquals(before + 1, count("get_object", expected, "success"));
        assertEquals(purposeBefore + 1, count("get_object", expected, "success", expectedPurpose));
        verify(storage).doRangeRead(argThat(o -> o.source() == expected && o.readPurpose() == expectedPurpose),
            eq("key"), eq(0L), eq(64L));
    }

    /** Original and hedge successes retain read purpose, whichever finishes first, without leaking buffers. */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void readHedgePreservesSource(boolean hedgeWins) throws Exception {
        setLatency("readLatencyCalculator");
        CompletableFuture<ByteBuf> original = new CompletableFuture<>();
        CompletableFuture<ByteBuf> hedge = new CompletableFuture<>();
        doReturn(original, hedge).when(storage).doRangeRead(any(), anyString(), anyLong(), anyLong());
        long before = count("get_object", S3RequestSource.ROUTER_WAL, "success", S3ReadPurpose.ROUTER_PRODUCE);
        CompletableFuture<ByteBuf> result = storage.mergedRangeRead(
            new ObjectStorage.ReadOptions().source(S3RequestSource.ROUTER_WAL).readPurpose(S3ReadPurpose.ROUTER_PRODUCE), "key", 0, 64);
        verify(storage, timeout(1000).times(2)).doRangeRead(argThat(o -> o.source() == S3RequestSource.ROUTER_WAL
            && o.readPurpose() == S3ReadPurpose.ROUTER_PRODUCE),
            eq("key"), eq(0L), eq(64L));
        ByteBuf winner = data();
        (hedgeWins ? hedge : original).complete(winner);
        result.get().release();
        ByteBuf loser = data();
        (hedgeWins ? original : hedge).complete(loser);
        await().untilAsserted(() -> assertEquals(before + 2,
            count("get_object", S3RequestSource.ROUTER_WAL, "success", S3ReadPurpose.ROUTER_PRODUCE)));
        assertEquals(0, winner.refCnt());
        assertEquals(0, loser.refCnt());
    }

    /** PUT hedge failures count only while the original is pending; caller mutation cannot change either source. */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void writeHedgeFailurePreservesSource(boolean failureBeforeSuccess) throws Exception {
        setLatency("s3LatencyCalculator");
        CompletableFuture<Void> original = new CompletableFuture<>();
        CompletableFuture<Void> hedge = new CompletableFuture<>();
        doReturn(original, hedge).when(storage).doWrite(any(), anyString(), any());
        long succeeded = count("put_object", S3RequestSource.DELTA_WAL, "success");
        long failed = count("put_object", S3RequestSource.DELTA_WAL, "failed");
        ObjectStorage.WriteOptions options = new ObjectStorage.WriteOptions().source(S3RequestSource.DELTA_WAL).enableFastRetry(true);
        ByteBuf buffer = data();
        CompletableFuture<?> result = storage.write(options, "key", buffer);
        options.source(S3RequestSource.ROUTER_WAL);
        verify(storage, timeout(1000).times(2)).doWrite(argThat(o -> o.source() == S3RequestSource.DELTA_WAL), eq("key"), any());
        if (failureBeforeSuccess) {
            hedge.completeExceptionally(new TimeoutException());
            assertFalse(result.isDone());
        }
        original.complete(null);
        result.get();
        if (!failureBeforeSuccess) {
            hedge.completeExceptionally(new TimeoutException());
        }
        await().untilAsserted(() -> assertEquals(0, buffer.refCnt()));
        assertEquals(succeeded + 1, count("put_object", S3RequestSource.DELTA_WAL, "success"));
        assertEquals(failed + (failureBeforeSuccess ? 1 : 0), count("put_object", S3RequestSource.DELTA_WAL, "failed"));
    }

    /** Unannotated calls are explicit and repeated observations reuse one histogram registration per tuple. */
    @Test
    void defaultSourceAndRegistrationAreBounded() throws Exception {
        storage.write(ObjectStorage.WriteOptions.DEFAULT, "key", data()).get();
        int registered = histograms().size();
        long before = count("put_object", S3RequestSource.UNCLASSIFIED, "success");
        for (int i = 0; i < 5; i++) {
            storage.write(ObjectStorage.WriteOptions.DEFAULT, "key", data()).get();
        }
        assertEquals(registered, histograms().size());
        assertEquals(before + 5, count("put_object", S3RequestSource.UNCLASSIFIED, "success"));
        assertEquals(S3RequestSource.UNCLASSIFIED, ObjectStorage.WriteOptions.DEFAULT.source());
    }

    private void setLatency(String fieldName) throws Exception {
        S3LatencyCalculator calculator = mock(S3LatencyCalculator.class);
        when(calculator.valueAtPercentile(anyLong(), anyLong())).thenReturn(10L);
        Field field = AbstractObjectStorage.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(storage, calculator);
    }

    private static ByteBuf data() {
        return Unpooled.buffer(64).writeZero(64);
    }

    // Inspect the actual registered observations consumed by all five histogram export callbacks.
    private static Collection<?> histograms() throws Exception {
        Field bundle = OperationLatencyMetrics.class.getDeclaredField("OPERATION_LATENCY");
        bundle.setAccessible(true);
        return (Collection<?>) field(bundle.get(null), "histograms");
    }

    private static long count(String operation, S3RequestSource source, String status) throws Exception {
        return count(operation, source, status, null);
    }

    private static long count(String operation, S3RequestSource source, String status, S3ReadPurpose readPurpose) throws Exception {
        boolean sourceEnabled = Boolean.getBoolean("automq.s3.metrics.source.enabled");
        boolean purposeEnabled = sourceEnabled && Boolean.getBoolean("automq.s3.metrics.read.purpose.enabled")
            && source == S3RequestSource.ROUTER_WAL && "get_object".equals(operation);
        String expectedSource = sourceEnabled ? source.name() : null;
        long count = 0;
        for (Object registration : histograms()) {
            Attributes attributes = (Attributes) field(registration, "histogramAttrs");
            boolean matchesPurpose = !purposeEnabled || readPurpose == null
                || readPurpose.name().equals(attributes.get(AttributeKey.stringKey("read_purpose")));
            if (operation.equals(attributes.get(AttributeKey.stringKey("operation_name")))
                && Objects.equals(expectedSource, attributes.get(AttributeKey.stringKey("source")))
                && status.equals(attributes.get(AttributeKey.stringKey("status")))
                && "16KB".equals(attributes.get(AttributeKey.stringKey("size")))
                && matchesPurpose) {
                if (!purposeEnabled) {
                    assertNull(attributes.get(AttributeKey.stringKey("read_purpose")));
                }
                assertEquals("S3Request", attributes.get(AttributeKey.stringKey("operation_type")));
                assertEquals("16KB", attributes.get(AttributeKey.stringKey("size")));
                count += ((DeltaHistogram) field(registration, "histogram")).cumulativeCount();
            }
        }
        return count;
    }

    private static Object field(Object target, String name) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }
}
