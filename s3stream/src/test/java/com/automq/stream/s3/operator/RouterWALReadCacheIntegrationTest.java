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

import com.automq.stream.s3.DefaultByteBufSupplier;
import com.automq.stream.s3.cache.RouterWALReadCache;
import com.automq.stream.s3.exceptions.ObjectNotExistException;
import com.automq.stream.s3.metrics.operations.S3RequestSource;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.wal.OpenMode;
import com.automq.stream.s3.wal.common.RecordHeader;
import com.automq.stream.s3.wal.impl.DefaultRecordOffset;
import com.automq.stream.s3.wal.impl.object.ObjectUtils;
import com.automq.stream.s3.wal.impl.object.ObjectWALConfig;
import com.automq.stream.s3.wal.impl.object.ObjectWALService;
import com.automq.stream.s3.wal.impl.object.WALObjectHeader;
import com.automq.stream.s3.wal.util.WALUtil;
import com.automq.stream.utils.Time;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/** Exercises cache fills through the real S3 read dispatcher, WAL layout fallback and record decoder. */
@Tag("S3Unit")
@Timeout(10)
class RouterWALReadCacheIntegrationTest {
    /** The dispatcher permits individual 8 MiB fills without widening its 4 MiB merge cap. */
    @Test
    void cacheFillsRemainBoundedThroughReadMerger() throws Exception {
        int block = 8 * 1024 * 1024;
        assertEquals(block, RouterWALReadCache.BLOCK_SIZE);
        RecordingObjectStorage storage = new RecordingObjectStorage();
        ObjectStorage.ReadOptions produce = new ObjectStorage.ReadOptions().bucket((short) 0).source(S3RequestSource.ROUTER_WAL);
        ObjectStorage.ReadOptions replay = new ObjectStorage.ReadOptions().bucket((short) 0).source(S3RequestSource.ROUTER_WAL);
        try (RouterWALReadCache cache = new RouterWALReadCache(2L * block)) {
            CompletableFuture<ByteBuf> first = cache.read(storage, produce, "key", 1, 2);
            CompletableFuture<ByteBuf> overlapping = storage.rangeRead(replay, "key", 0, block);
            CompletableFuture<ByteBuf> largeBypass = cache.read(storage, produce, "key", 0, block + 1L);
            CompletableFuture<ByteBuf> crossing = cache.read(storage, produce, "key", block - 1L, block + 1L);
            CompletableFuture<ByteBuf> second = cache.read(storage, replay, "key", block + 1L, block + 2L);

            storage.tryMergeRead();
            for (CompletableFuture<ByteBuf> result : List.of(first, overlapping, largeBypass, crossing, second)) {
                result.get(5, TimeUnit.SECONDS).release();
            }
            // An 8 MiB fill exceeds the shared merger's cap, including for fully overlapping reads.
            assertEquals(5, storage.reads.size());
            ReadCall firstFill = storage.reads.stream().filter(read -> read.start == 0 && read.end == block)
                .findFirst().orElseThrow();
            assertEquals(S3RequestSource.ROUTER_WAL, firstFill.options.source());
            assertEquals(2, storage.reads.stream().filter(read -> read.start == 0 && read.end == block).count());
            ReadCall secondFill = storage.reads.stream().filter(read -> read.start == block && read.end == 2L * block)
                .findFirst().orElseThrow();
            assertEquals(S3RequestSource.ROUTER_WAL, secondFill.options.source());
            assertEquals(1, storage.reads.stream().filter(read -> read.start == 0 && read.end == block + 1L).count());
            assertEquals(1, storage.reads.stream().filter(read -> read.start == block - 1L && read.end == block + 1L).count());
        } finally {
            storage.close();
        }
    }

    /** The actual WAL reader decodes a valid record while the same physical fill is still pending. */
    @Test
    void walRecordReturnsBeforeDownloadTail() throws Exception {
        ByteBuf object = object();
        int recordEnd = object.readableBytes();
        int recordSize = recordEnd - WALObjectHeader.DEFAULT_WAL_HEADER_SIZE;
        object = Unpooled.wrappedBuffer(object, Unpooled.buffer(128, 128).writeZero(128));
        ControlledObjectStorage storage = new ControlledObjectStorage();
        ObjectWALConfig config = ObjectWALConfig.builder().withClusterId("cluster").withNodeId(1)
            .withType("rc").withSource(S3RequestSource.ROUTER_WAL).withVersion(0)
            .withOpenMode(OpenMode.READ_ONLY).build();
        try (RouterWALReadCache cache = new RouterWALReadCache(RouterWALReadCache.BLOCK_SIZE, true)) {
            ObjectWALService reader = new ObjectWALService(Time.SYSTEM, storage, config, false, cache);
            try {
                reader.start();
                CompletableFuture<StreamRecordBatch> result = reader.get(DefaultRecordOffset.of(2, 0, recordSize));
                ReadProgressListener progress = storage.progress.get(5, TimeUnit.SECONDS);
                progress.onProgress(object.slice(0, recordEnd - 1).asReadOnly());
                assertFalse(result.isDone());
                progress.onProgress(object.slice(0, recordEnd).asReadOnly());
                StreamRecordBatch record = result.get(5, TimeUnit.SECONDS);
                assertEquals(42, record.getStreamId());
                assertEquals(7, record.getPayload().getInt(0));
                record.release();
                assertFalse(storage.download.isDone());
                assertEquals(RouterWALReadCache.BLOCK_SIZE, cache.usedBytes());
                storage.download.complete(object.retainedDuplicate());
                String path = ObjectUtils.genObjectPathV1(ObjectUtils.nodePrefix("cluster", 1, "rc"), 2, 0);
                await().until(() -> {
                    var cached = cache.readIfPresent(storage, new ObjectStorage.ReadOptions().source(S3RequestSource.ROUTER_WAL),
                        path, WALObjectHeader.DEFAULT_WAL_HEADER_SIZE, recordEnd);
                    cached.ifPresent(copy -> copy.join().release());
                    return cached.isPresent();
                });
            } finally {
                reader.shutdownGracefully();
            }
        } finally {
            storage.download.completeExceptionally(new IllegalArgumentException("test cleanup"));
            storage.close();
            object.release();
        }
    }

    /** Both layouts serve immutable cached bytes across WAL readers and individual WAL shutdown. */
    @ParameterizedTest
    @CsvSource({"0,0", "0,1", "1,0", "1,1"})
    void cachesShortObjectsThroughWalReaders(int readerVersion, int objectVersion) throws Exception {
        ByteBuf object = object();
        String prefix = ObjectUtils.nodePrefix("cluster", 1, "rc");
        String path = objectVersion == 0 ? ObjectUtils.genObjectPathV1(prefix, 2, 0)
            : ObjectUtils.genShardedObjectPathV1(prefix, 2, 0, ObjectUtils.DATA_FILE_ALIGN_SIZE);
        TruncatingObjectStorage storage = new TruncatingObjectStorage(path, object);
        ObjectWALConfig config = ObjectWALConfig.builder().withClusterId("cluster").withNodeId(1)
            .withType("rc").withSource(S3RequestSource.ROUTER_WAL).withVersion(readerVersion)
            .withOpenMode(OpenMode.READ_ONLY).build();
        try (RouterWALReadCache cache = new RouterWALReadCache(RouterWALReadCache.BLOCK_SIZE)) {
            ObjectWALService produceReader = new ObjectWALService(Time.SYSTEM, storage, config, false, cache);
            ObjectWALService replayReader = new ObjectWALService(Time.SYSTEM, storage, config, false, cache);
            try {
                produceReader.start();
                replayReader.start();
                DefaultRecordOffset offset = DefaultRecordOffset.of(2, 0,
                    object.readableBytes() - WALObjectHeader.DEFAULT_WAL_HEADER_SIZE);
                StreamRecordBatch produce = produceReader.get(offset).get(5, TimeUnit.SECONDS);
                try {
                    assertEquals(42, produce.getStreamId());
                    assertEquals(7, produce.getPayload().getInt(0));
                    produce.getPayload().setInt(0, 99);
                } finally {
                    produce.release();
                }
                produceReader.shutdownGracefully();
                StreamRecordBatch replay = replayReader.get(offset).get(5, TimeUnit.SECONDS);
                try {
                    assertEquals(7, replay.getPayload().getInt(0));
                } finally {
                    replay.release();
                }
                assertEquals(RouterWALReadCache.BLOCK_SIZE, cache.usedBytes());
                // The first reader may probe a missing layout; the second reuses the completed cached layout.
                assertEquals(readerVersion == objectVersion ? 1 : 2, storage.reads.size());
                List<ReadCall> successfulReads = storage.reads.stream().filter(read -> path.equals(read.path)).toList();
                assertEquals(1, successfulReads.size());
                ReadCall read = successfulReads.get(0);
                assertEquals(S3RequestSource.ROUTER_WAL, read.options.source());
                assertEquals(0L, read.start);
                assertEquals((long) RouterWALReadCache.BLOCK_SIZE, read.end);
            } finally {
                produceReader.shutdownGracefully();
                replayReader.shutdownGracefully();
            }
        } finally {
            storage.close();
            object.release();
        }
    }

    private static final class ControlledObjectStorage extends MemoryObjectStorage {
        private final CompletableFuture<ReadProgressListener> progress = new CompletableFuture<>();
        private final CompletableFuture<ByteBuf> download = new CompletableFuture<>();

        @Override
        CompletableFuture<ByteBuf> doRangeRead(ReadOptions options, String path, long start, long end) {
            progress.complete(options.progressListener().get());
            return download;
        }
    }

    /** Emulates S3 EOF truncation on the instance captured by the merge scheduler. */
    private static final class TruncatingObjectStorage extends MemoryObjectStorage {
        private final String path;
        private final ByteBuf object;
        private final List<ReadCall> reads = new CopyOnWriteArrayList<>();

        private TruncatingObjectStorage(String path, ByteBuf object) {
            this.path = path;
            this.object = object;
        }

        @Override
        CompletableFuture<ByteBuf> doRangeRead(ReadOptions options, String requestedPath, long start, long end) {
            reads.add(new ReadCall(options.copy(), requestedPath, start, end));
            if (!path.equals(requestedPath)) {
                return CompletableFuture.failedFuture(new ObjectNotExistException("missing layout"));
            }
            return CompletableFuture.completedFuture(object.copy((int) start,
                (int) (Math.min(end, object.readableBytes()) - start)));
        }
    }

    /** Captures physical reads while retaining the manually driven merge dispatcher. */
    private static final class RecordingObjectStorage extends MemoryObjectStorage {
        private final List<ReadCall> reads = new CopyOnWriteArrayList<>();

        private RecordingObjectStorage() {
            super(true);
        }

        @Override
        CompletableFuture<ByteBuf> doRangeRead(ReadOptions options, String path, long start, long end) {
            reads.add(new ReadCall(options.copy(), path, start, end));
            int size = Math.toIntExact(end - start);
            return CompletableFuture.completedFuture(Unpooled.buffer(size, size).writeZero(size));
        }
    }

    private static final class ReadCall {
        private final ObjectStorage.ReadOptions options;
        private final String path;
        private final long start;
        private final long end;

        private ReadCall(ObjectStorage.ReadOptions options, String path, long start, long end) {
            this.options = options;
            this.path = path;
            this.start = start;
            this.end = end;
        }
    }

    private static ByteBuf object() {
        StreamRecordBatch batch = StreamRecordBatch.of(42, 0, 0, 1,
            Unpooled.buffer(4, 4).writeInt(7), DefaultByteBufSupplier.INSTANCE);
        ByteBuf recordHeader = WALUtil.generateHeader(batch.encoded(),
            Unpooled.buffer(RecordHeader.RECORD_HEADER_SIZE, RecordHeader.RECORD_HEADER_SIZE), 0, 0);
        ByteBuf walHeader = new WALObjectHeader(0, recordHeader.readableBytes() + batch.encoded().readableBytes(),
            0, 1, 2, 0).marshal();
        try {
            return Unpooled.buffer(walHeader.readableBytes() + recordHeader.readableBytes() + batch.encoded().readableBytes())
                .writeBytes(walHeader).writeBytes(recordHeader).writeBytes(batch.encoded());
        } finally {
            walHeader.release();
            recordHeader.release();
            batch.release();
        }
    }
}
