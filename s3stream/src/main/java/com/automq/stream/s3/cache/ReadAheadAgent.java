/*
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

import com.automq.stream.s3.metrics.MetricsLevel;
import com.automq.stream.s3.metrics.TimerUtil;
import com.automq.stream.s3.metrics.stats.StorageOperationStats;
import com.automq.stream.utils.LogContext;
import com.google.common.base.Objects;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

public class ReadAheadAgent {
    private static final Integer MAX_READ_AHEAD_SIZE = 40 * 1024 * 1024; // 40MB
    private static final Integer S3_OPERATION_DELAY_MS = 400; // 400ms
    private final Logger logger;
    private final Lock lock = new ReentrantLock();
    private final TimerUtil timer;
    private final long streamId;
    private final int dataBlockSize;
    private final List<Pair<Long, Long>> evictedOffsetRanges = new ArrayList<>();
    private double bytePerSecond;
    private long readCount;
    private long lastReadOffset;
    private int lastReadSize;
    private long readAheadEndOffset;
    private int lastReadAheadSize;

    public ReadAheadAgent(int dataBlockSize, long streamId, long startOffset) {
        this.logger = new LogContext(String.format("[S3BlockCache] stream=%d ", streamId)).logger(ReadAheadAgent.class);
        this.timer = new TimerUtil();
        this.dataBlockSize = dataBlockSize;
        this.streamId = streamId;
        this.lastReadOffset = startOffset;
        this.readCount = 0;
        logger.info("create read ahead agent for stream={}, startOffset={}", streamId, startOffset);
    }

    public void updateReadProgress(long startOffset) {
        try {
            lock.lock();
            if (startOffset != lastReadOffset) {
                logger.error("update read progress for stream={} failed, offset not match: expected offset {}, but get {}", streamId, lastReadOffset, startOffset);
                return;
            }
            long timeElapsedNanos = timer.elapsedAs(TimeUnit.NANOSECONDS);
            double bytesPerSec = (double) this.lastReadSize / timeElapsedNanos * TimeUnit.SECONDS.toNanos(1);
            readCount++;
            double factor = (double) readCount / (1 + readCount);
            bytePerSecond = (1 - factor) * bytePerSecond + factor * bytesPerSec;
            if (logger.isDebugEnabled()) {
                logger.debug("update read progress offset {}, lastReadSpeed: {} bytes/s, corrected speed: {} bytes/s", startOffset, bytesPerSec, bytePerSecond);
            }
        } finally {
            lock.unlock();
        }
    }

    public void updateReadResult(long startOffset, long endOffset, int size) {
        try {
            lock.lock();
            if (startOffset != lastReadOffset) {
                logger.error("update read result for stream={} failed, offset not match: expected offset {}, but get {}", streamId, lastReadOffset, startOffset);
                return;
            }
            this.lastReadSize = size;
            this.lastReadOffset = endOffset;
            timer.reset();
            if (logger.isDebugEnabled()) {
                logger.debug("update read result offset {}-{}, size: {}, readAheadOffset: {}", startOffset, endOffset, size, readAheadEndOffset);
            }
        } finally {
            lock.unlock();
        }
    }

    public void updateReadAheadResult(long readAheadEndOffset, int readAheadSize) {
        try {
            lock.lock();
            this.readAheadEndOffset = readAheadEndOffset;
            this.lastReadAheadSize = readAheadSize;
            StorageOperationStats.getInstance().readAheadSizeStats.record(MetricsLevel.INFO, readAheadSize);
            if (logger.isDebugEnabled()) {
                logger.debug("update read ahead offset {}, size: {}, lastReadOffset: {}", readAheadEndOffset, readAheadSize, lastReadOffset);
            }
        } finally {
            lock.unlock();
        }
    }

    public int getNextReadAheadSize() {
        try {
            lock.lock();
            // remove range that is not within read ahead
            this.evictedOffsetRanges.removeIf(range -> range.getLeft() >= readAheadEndOffset || range.getRight() <= lastReadOffset);
            int nextSize = calculateNextSize();
            this.lastReadAheadSize = nextSize;
            if (logger.isDebugEnabled()) {
                logger.debug("get next read ahead size {}, {}", nextSize, this);
            }
            return nextSize;
        } finally {
            lock.unlock();
        }

    }

    private int calculateNextSize() {
        long totalEvictedSize = this.evictedOffsetRanges.stream().mapToLong(range -> {
            long left = Math.max(range.getLeft(), lastReadOffset);
            long right = Math.min(range.getRight(), readAheadEndOffset);
            return right - left;
        }).sum();
        double evictedFraction = (double) totalEvictedSize / (readAheadEndOffset - lastReadOffset);
        int nextSize = (int) (bytePerSecond * ((double) S3_OPERATION_DELAY_MS / TimeUnit.SECONDS.toMillis(1)) * (1 - evictedFraction));
        nextSize = Math.max(dataBlockSize, Math.min(nextSize, MAX_READ_AHEAD_SIZE));
        return nextSize;
    }

    public long getStreamId() {
        return streamId;
    }

    public long getReadAheadOffset() {
        try {
            lock.lock();
            return readAheadEndOffset;
        } finally {
            lock.unlock();
        }
    }

    public long getLastReadAheadSize() {
        try {
            lock.lock();
            return lastReadAheadSize;
        } finally {
            lock.unlock();
        }
    }

    public long getLastReadOffset() {
        try {
            lock.lock();
            return lastReadOffset;
        } finally {
            lock.unlock();
        }
    }

    public int getLastReadSize() {
        try {
            lock.lock();
            return lastReadSize;
        } finally {
            lock.unlock();
        }
    }

    public double getBytePerSecond() {
        try {
            lock.lock();
            return bytePerSecond;
        } finally {
            lock.unlock();
        }
    }

    public void evict(long startOffset, long endOffset) {
        try {
            lock.lock();
            if (startOffset >= endOffset
                || lastReadOffset >= readAheadEndOffset
                || endOffset <= lastReadOffset
                || startOffset >= readAheadEndOffset) {
                return;
            }

            if (logger.isDebugEnabled()) {
                logger.debug("evict range [{}, {}], lastReadOffset: {}, readAheadOffset: {}", startOffset, endOffset, lastReadOffset, readAheadEndOffset);
            }

            this.evictedOffsetRanges.add(Pair.of(startOffset, endOffset));
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ReadAheadAgent agent = (ReadAheadAgent) o;
        return streamId == agent.streamId && lastReadOffset == agent.lastReadOffset;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(streamId, lastReadOffset);
    }

    @Override
    public String toString() {
        return "ReadAheadAgent{" +
            "stream=" + streamId +
            ", bytesPerSecond=" + bytePerSecond +
            ", lastReadOffset=" + lastReadOffset +
            ", lastReadSize=" + lastReadSize +
            ", readAheadEndOffset=" + readAheadEndOffset +
            ", evictedOffsetRanges=" + evictedOffsetRanges +
            '}';
    }
}
