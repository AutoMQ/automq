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

package com.automq.stream.s3.cache;

import com.automq.stream.s3.model.StreamRecordBatch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import io.netty.buffer.ByteBuf;

/**
 * Stores complete stream record batches on disk and returns byte ranges for reading them.
 * Operations are synchronous and thread-safe.
 */
public final class StreamRecordBatchDiskCache {
    private final FileCache fileCache;
    private final long cacheId;
    private long nextBatchPosition;

    /**
     * @param path backing file, recreated for a positive capacity
     * @param capacityBytes requested capacity in bytes; zero disables storage
     * @throws IOException if the backing file cannot be created or mapped
     */
    public StreamRecordBatchDiskCache(String path, long capacityBytes) throws IOException {
        this(new FileCache(path, capacityBytes));
    }

    StreamRecordBatchDiskCache(FileCache fileCache) {
        this.fileCache = Objects.requireNonNull(fileCache, "fileCache");
        this.cacheId = fileCache.newCacheId();
    }

    /**
     * Copies one complete encoded batch. Admission is best-effort: a range may return a miss
     * immediately if storage is unavailable, or later after eviction.
     *
     * @param batch batch to copy; the caller retains ownership, and its bytes and buffer indexes are unchanged
     * @return the stored byte range to pass to {@link #get(FileCache.ReadRange)}
     * @throws ArithmeticException if the batch position counter overflows
     */
    public synchronized FileCache.ReadRange put(StreamRecordBatch batch) {
        ByteBuf encoded = batch.encoded();
        int encodedLength = encoded.readableBytes();
        long position = nextBatchPosition;
        // Leave a logical gap so FileCache does not merge separately evictable batches.
        nextBatchPosition = Math.addExact(position, (long) encodedLength + 1);
        fileCache.put(cacheId, position, encoded);
        return new FileCache.ByteRange(cacheId, position, encodedLength);
    }

    /**
     * Reads one batch into caller-owned memory. The result remains valid after cache eviction.
     *
     * @param range byte range returned by {@link #put(StreamRecordBatch)}
     * @return a batch the caller must release, or empty on a miss
     */
    public Optional<StreamRecordBatch> get(FileCache.ReadRange range) {
        Optional<ByteBuf> cached = fileCache.get(range);
        if (cached.isEmpty()) {
            return Optional.empty();
        }
        ByteBuf encoded = cached.get();
        try {
            return Optional.of(StreamRecordBatch.parse(encoded, false, null));
        } finally {
            encoded.release();
        }
    }

    /**
     * Reads all batches in list order into caller-owned memory, or reports all missing ranges without reading payloads.
     * The results remain valid after cache eviction.
     *
     * @param ranges byte ranges returned by {@link #put(StreamRecordBatch)}
     * @return batches the caller must release on a full hit, or all missing ranges with no batches on a miss;
     *     empty input returns empty batches and missing ranges
     */
    public BulkReadResult get(List<FileCache.ReadRange> ranges) {
        List<StreamRecordBatch> batches = new ArrayList<>(ranges.size());
        FileCache.BulkReadResult cached = fileCache.get(ranges);
        if (!cached.missingRanges().isEmpty()) {
            return new BulkReadResult(List.of(), cached.missingRanges());
        }
        List<ByteBuf> buffers = cached.buffers();
        try {
            for (ByteBuf encoded : buffers) {
                batches.add(StreamRecordBatch.parse(encoded, false, null));
            }
            return new BulkReadResult(batches, List.of());
        } catch (RuntimeException | Error e) {
            batches.forEach(StreamRecordBatch::release);
            throw e;
        } finally {
            buffers.forEach(ByteBuf::release);
        }
    }

    /**
     * An all-or-nothing batch read result. Empty missing ranges indicate a full hit, including empty input.
     *
     * @param batches caller-owned batches in input order on a hit; empty on a miss
     * @param missingRanges original instances of every missing input range in input order; empty on a hit
     */
    public record BulkReadResult(List<StreamRecordBatch> batches, List<FileCache.ReadRange> missingRanges) {
    }
}
