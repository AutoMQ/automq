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

package com.automq.stream.s3.wal.impl.object;

import com.automq.stream.s3.DefaultByteBufSupplier;
import com.automq.stream.s3.exceptions.ObjectNotExistException;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.network.ThrottleStrategy;
import com.automq.stream.s3.operator.MemoryObjectStorage;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.trace.context.TraceContext;
import com.automq.stream.s3.wal.AppendResult;
import com.automq.stream.s3.wal.OpenMode;
import com.automq.stream.s3.wal.RecordOffset;
import com.automq.stream.s3.wal.impl.DefaultRecordOffset;
import com.automq.stream.utils.Time;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import static com.automq.stream.s3.wal.impl.object.ObjectUtils.DATA_FILE_ALIGN_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Exercises migration, remote reads, restart recovery, and cleanup through the real object WAL. */
@Tag("S3Unit")
@Timeout(30)
public class ObjectWALShardingTest {
    /** Both WAL types use all 256 shards, remain readable across wraparound, and delete them after recovery. */
    @ParameterizedTest
    @ValueSource(strings = {"", "rc"})
    public void testShardWraparoundAndDeletion(String type) throws Exception {
        MemoryObjectStorage storage = new MemoryObjectStorage((short) 7);
        String prefix = ObjectUtils.nodePrefix("cluster", 1001, type);
        List<RecordOffset> offsets = new ArrayList<>();
        try {
            ObjectWALService writer = new ObjectWALService(Time.SYSTEM, storage, config(type, 42, 1, OpenMode.READ_WRITE));
            writer.start();
            try {
                for (int i = 0; i < 258; i++) {
                    offsets.add(append(writer, i));
                }
                List<String> paths = storage.list(prefix).join().stream().map(ObjectStorage.ObjectInfo::key).toList();
                assertEquals(258, paths.size());
                assertEquals(256, paths.stream().map(path -> path.substring(prefix.length()).split("/")[0])
                    .collect(Collectors.toSet()).size());
                for (int index : List.of(63, 64, 255, 256, 257)) {
                    String shard = switch (index) {
                        case 63 -> "s3f";
                        case 64 -> "s40";
                        case 255 -> "sff";
                        case 256 -> "s00";
                        default -> "s01";
                    };
                    assertTrue(paths.contains(prefix + shard + "/42/wal/" + index * DATA_FILE_ALIGN_SIZE
                        + "-" + (index + 1) * DATA_FILE_ALIGN_SIZE));
                }
                for (int version : List.of(0, 1)) {
                    DefaultReader reader = new DefaultReader(storage, config(type, 42, version, OpenMode.READ_ONLY), Time.SYSTEM);
                    for (int index : List.of(63, 64, 255, 256, 257)) {
                        StreamRecordBatch record = reader.get(offsets.get(index)).get(5, TimeUnit.SECONDS);
                        try {
                            assertEquals(index, record.getBaseOffset());
                        } finally {
                            record.release();
                        }
                    }
                    assertRecords(reader.get(offsets.get(0), writer.confirmOffset()).get(5, TimeUnit.SECONDS), 258);
                }
            } finally {
                writer.shutdownGracefully();
            }

            // A rollback to version=0 still recovers and trims objects in every version=1 shard.
            ObjectWALService recovered = new ObjectWALService(Time.SYSTEM, storage, config(type, 43, 0, OpenMode.FAILOVER));
            recovered.start();
            try {
                assertRecovered(recovered, 258);
                RecordOffset next = append(recovered, 258);
                assertEquals(258 * DATA_FILE_ALIGN_SIZE, DefaultRecordOffset.of(next).offset());
                recovered.trim(next).get(5, TimeUnit.SECONDS);
                List<String> remaining = storage.list(prefix).join().stream().map(ObjectStorage.ObjectInfo::key).toList();
                assertEquals(1, remaining.size());
                assertTrue(remaining.get(0).startsWith(prefix + "43/wal/"));
            } finally {
                recovered.shutdownGracefully();
            }
        } finally {
            storage.close();
        }
    }

    /** The version=1 layout locates records inside a nonzero object, including partial batch reads. */
    @ParameterizedTest
    @ValueSource(strings = {"", "rc"})
    public void testReadsInsideShardedObject(String type) throws Exception {
        MemoryObjectStorage storage = new MemoryObjectStorage((short) 7);
        ObjectWALConfig config = ObjectWALConfig.builder().withClusterId("cluster").withNodeId(1001)
            .withVersion(1).withBucketId((short) 7).withType(type).withEpoch(42).withManualMode(true).build();
        ObjectWALService wal = new ObjectWALService(Time.SYSTEM, storage, config);
        wal.start();
        try {
            append(wal, 0);
            wal.append(TraceContext.DEFAULT, StreamRecordBatch.of(233, 0, 1, 1,
                Unpooled.buffer(4).writeInt(1), DefaultByteBufSupplier.INSTANCE));
            CompletableFuture<AppendResult> last = wal.append(TraceContext.DEFAULT, StreamRecordBatch.of(233, 0, 2, 1,
                Unpooled.buffer(4).writeInt(2), DefaultByteBufSupplier.INSTANCE));
            wal.flush().get(5, TimeUnit.SECONDS);
            RecordOffset offset = last.get(5, TimeUnit.SECONDS).recordOffset();
            assertTrue(DefaultRecordOffset.of(offset).offset() > DATA_FILE_ALIGN_SIZE);
            StreamRecordBatch record = wal.get(offset).get(5, TimeUnit.SECONDS);
            try {
                assertEquals(2, record.getBaseOffset());
            } finally {
                record.release();
            }
            List<StreamRecordBatch> records = wal.get(offset, wal.confirmOffset()).get(5, TimeUnit.SECONDS);
            try {
                assertEquals(1, records.size());
                assertEquals(2, records.get(0).getBaseOffset());
            } finally {
                records.forEach(StreamRecordBatch::release);
            }
            String prefix = ObjectUtils.nodePrefix("cluster", 1001, type);
            assertEquals(List.of(prefix + "s01/42/wal/67108864-134217728"),
                storage.list(prefix + "s01/").join().stream().map(ObjectStorage.ObjectInfo::key).toList());
        } finally {
            wal.shutdownGracefully();
            storage.close();
        }
    }

    /** Both WAL types survive legacy-to-sharded-to-legacy transitions with mixed epochs and exact-path trimming. */
    @ParameterizedTest
    @ValueSource(strings = {"", "rc"})
    public void testMigrationRecoveryAndTrim(String type) throws Exception {
        MemoryObjectStorage storage = spy(new MemoryObjectStorage((short) 7));
        List<RecordOffset> offsets = new ArrayList<>();
        String prefix = ObjectUtils.nodePrefix("cluster", 1001, type);
        int[] versions = {0, 1, 0};
        try {
            for (int epoch = 0; epoch < versions.length; epoch++) {
                ObjectWALService wal = new ObjectWALService(Time.SYSTEM, storage, config(type, epoch, versions[epoch], OpenMode.READ_WRITE));
                wal.start();
                try {
                    assertRecovered(wal, epoch * 4);
                    for (int i = 0; i < 4; i++) {
                        offsets.add(append(wal, offsets.size()));
                    }
                    assertEquals(4 * (epoch + 1) * DATA_FILE_ALIGN_SIZE,
                        DefaultRecordOffset.of(wal.confirmOffset()).offset());
                } finally {
                    wal.shutdownGracefully();
                }
            }
            List<String> paths = storage.list(prefix).join().stream().map(ObjectStorage.ObjectInfo::key).toList();
            assertEquals(12, paths.size());
            assertTrue(paths.contains(prefix + "0/wal/0-67108864"));
            assertTrue(paths.contains(prefix + "s04/1/wal/268435456-335544320"));
            assertTrue(paths.contains(prefix + "s06/1/wal/402653184-469762048"));
            assertTrue(paths.contains(prefix + "2/wal/536870912-603979776"));

            for (int readerVersion : List.of(0, 1)) {
                clearInvocations(storage);
                // Remote readers in either version must read both layouts without starting a writer.
                ObjectWALService reader = new ObjectWALService(Time.SYSTEM, storage, config(type, 0, readerVersion, OpenMode.READ_ONLY));
                reader.start();
                try {
                    for (int i = 0; i < offsets.size(); i++) {
                        StreamRecordBatch record = reader.get(offsets.get(i)).get(5, TimeUnit.SECONDS);
                        try {
                            assertEquals(i, record.getBaseOffset());
                        } finally {
                            record.release();
                        }
                    }
                    List<StreamRecordBatch> batch = reader.get(offsets.get(0), offsets.get(11)).get(5, TimeUnit.SECONDS);
                    assertRecords(batch, 11);
                    verify(storage, times(readerVersion == 0 ? 2 : 0)).rangeRead(
                        any(), eq(prefix + "1/wal/268435456-335544320"), anyLong(), anyLong());
                } finally {
                    reader.shutdownGracefully();
                }
            }

            ObjectWALService recovered = new ObjectWALService(Time.SYSTEM, storage, config(type, 3, 1, OpenMode.FAILOVER));
            recovered.start();
            try {
                assertRecovered(recovered, 12);
                RecordOffset next = append(recovered, 12);
                assertEquals(12 * DATA_FILE_ALIGN_SIZE, DefaultRecordOffset.of(next).offset());
                recovered.trim(next).get(5, TimeUnit.SECONDS);
                List<String> remaining = storage.list(prefix).join().stream().map(ObjectStorage.ObjectInfo::key).toList();
                assertEquals(1, remaining.size());
                assertFalse(paths.contains(remaining.get(0)));
                assertTrue(remaining.get(0).startsWith(prefix + "s0d/3/wal/"));
            } finally {
                recovered.shutdownGracefully();
            }
        } finally {
            storage.close();
        }
    }

    /** Single and batch reads fall back in either direction on wrapped not-found, preserving the byte range. */
    @ParameterizedTest
    @CsvSource({"false,0", "true,0", "false,1", "true,1"})
    public void testWrappedNotFoundFallback(boolean batch, int version) throws Exception {
        MemoryObjectStorage storage = spy(new MemoryObjectStorage((short) 7));
        ObjectWALService wal = new ObjectWALService(Time.SYSTEM, storage, config("rc", 42, 1 - version, OpenMode.READ_WRITE));
        wal.start();
        try {
            RecordOffset offset = append(wal, 0);
            String prefix = ObjectUtils.nodePrefix("cluster", 1001, "rc");
            String sharded = prefix + "s00/42/wal/0-67108864";
            String preferred = version == 1 ? sharded : prefix + "42/wal/0-67108864";
            doReturn(CompletableFuture.failedFuture(new CompletionException(new ObjectNotExistException("missing"))))
                .when(storage).rangeRead(any(), eq(preferred), anyLong(), anyLong());
            DefaultReader reader = new DefaultReader(storage, config("rc", 42, version, OpenMode.READ_ONLY), Time.SYSTEM);
            if (batch) {
                assertRecords(reader.get(offset, wal.confirmOffset()).get(5, TimeUnit.SECONDS), 1);
            } else {
                StreamRecordBatch record = reader.get(offset).get(5, TimeUnit.SECONDS);
                assertEquals(0, record.getBaseOffset());
                record.release();
            }
            long start = WALObjectHeader.WAL_HEADER_SIZE_V1;
            long end = batch ? -1 : start + DefaultRecordOffset.of(offset).size();
            verify(storage).rangeRead(any(), eq(sharded), eq(start), eq(end));
            verify(storage).rangeRead(any(), eq(prefix + "42/wal/0-67108864"), eq(start), eq(end));
        } finally {
            wal.shutdownGracefully();
            storage.close();
        }
    }

    /** Both read orders propagate access/transport failures and missing fallback objects without more requests. */
    @ParameterizedTest
    @CsvSource({"false,0", "true,0", "false,1", "true,1"})
    public void testReadFailures(boolean batch, int version) throws Exception {
        for (Throwable error : List.of(new IOException("503 SlowDown"), new SecurityException("403 Forbidden"),
            new ObjectNotExistException("missing legacy object"))) {
            ObjectStorage storage = mock(ObjectStorage.class);
            String prefix = ObjectUtils.nodePrefix("cluster", 1001, "rc");
            String sharded = prefix + "s00/42/wal/0-67108864";
            String legacy = prefix + "42/wal/0-67108864";
            String preferred = version == 1 ? sharded : legacy;
            String fallback = version == 1 ? legacy : sharded;
            when(storage.bucketId()).thenReturn((short) 7);
            when(storage.list(prefix)).thenReturn(CompletableFuture.completedFuture(List.of(
                new ObjectStorage.ObjectInfo((short) 7, legacy, 0, 100))));
            boolean notFound = error instanceof ObjectNotExistException;
            when(storage.rangeRead(any(), eq(preferred), anyLong(), anyLong())).thenReturn(
                CompletableFuture.failedFuture(new CompletionException(error)));
            if (notFound) {
                when(storage.rangeRead(any(), eq(fallback), anyLong(), anyLong()))
                    .thenReturn(CompletableFuture.failedFuture(error));
            }
            DefaultReader reader = new DefaultReader(storage, config("rc", 42, version, OpenMode.READ_ONLY), Time.SYSTEM);
            DefaultRecordOffset offset = DefaultRecordOffset.of(42, 0, 10);
            CompletableFuture<?> future = batch
                ? reader.get(offset, DefaultRecordOffset.of(42, DATA_FILE_ALIGN_SIZE, 0)) : reader.get(offset);
            ExecutionException failure = assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
            assertSame(error, failure.getCause());
            if (!notFound) {
                verify(storage, never()).rangeRead(any(), eq(fallback), anyLong(), anyLong());
            }
            verify(storage).rangeRead(argThat(options ->
                options.bucket() == 7 && options.throttleStrategy() == ThrottleStrategy.BYPASS),
                eq(preferred), eq((long) WALObjectHeader.WAL_HEADER_SIZE_V1), eq(batch ? -1L : WALObjectHeader.WAL_HEADER_SIZE_V1 + 10L));
        }
    }

    private static ObjectWALConfig config(String type, long epoch, int version, OpenMode openMode) {
        return ObjectWALConfig.builder().withURI("7@s3://wal?version=" + version)
            .withClusterId("cluster").withNodeId(1001).withType(type).withEpoch(epoch)
            .withManualMode(true).withOpenMode(openMode).build();
    }

    private static RecordOffset append(ObjectWALService wal, int baseOffset) throws Exception {
        ByteBuf payload = Unpooled.buffer(4).writeInt(baseOffset);
        CompletableFuture<AppendResult> append = wal.append(TraceContext.DEFAULT,
            StreamRecordBatch.of(233, 0, baseOffset, 1, payload, DefaultByteBufSupplier.INSTANCE));
        wal.flush().get(5, TimeUnit.SECONDS);
        return append.get(5, TimeUnit.SECONDS).recordOffset();
    }

    private static void assertRecovered(ObjectWALService wal, int expectedCount) {
        List<StreamRecordBatch> records = new ArrayList<>();
        wal.recover().forEachRemaining(result -> records.add(result.record()));
        assertRecords(records, expectedCount);
    }

    private static void assertRecords(List<StreamRecordBatch> records, int expectedCount) {
        try {
            assertEquals(expectedCount, records.size());
            for (int i = 0; i < records.size(); i++) {
                assertEquals(i, records.get(i).getBaseOffset());
            }
        } finally {
            records.forEach(StreamRecordBatch::release);
        }
    }
}
