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

import com.automq.stream.s3.model.StreamRecordBatch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static com.automq.stream.s3.cache.LogCache.StreamRange.NOOP_OFFSET;

public class LogCache {
    public static final long MATCH_ALL_STREAMS = -1L;
    private static final Consumer<LogCacheBlock> DEFAULT_BLOCK_FREE_LISTENER = block -> block
            .records().forEach(
                    (streamId, records) -> records.forEach(StreamRecordBatch::release)
            );
    private final long cacheBlockMaxSize;
    private final List<LogCacheBlock> blocks = new ArrayList<>();
    private LogCacheBlock activeBlock;
    private long confirmOffset;
    private final AtomicLong size = new AtomicLong();
    private final Consumer<LogCacheBlock> blockFreeListener;

    public LogCache(long cacheBlockMaxSize, Consumer<LogCacheBlock> blockFreeListener) {
        this.cacheBlockMaxSize = cacheBlockMaxSize;
        this.activeBlock = new LogCacheBlock(cacheBlockMaxSize);
        this.blocks.add(activeBlock);
        this.blockFreeListener = blockFreeListener;
    }

    public LogCache(long cacheBlockMaxSize) {
        this(cacheBlockMaxSize, DEFAULT_BLOCK_FREE_LISTENER);
    }

    public boolean put(StreamRecordBatch recordBatch) {
        tryRealFree();
        size.addAndGet(recordBatch.size());
        return activeBlock.put(recordBatch);
    }

    /**
     * Get streamId [startOffset, endOffset) range records with maxBytes limit.
     * <p>
     * - If the requested range can be fully satisfied, then return the corresponding cached records.
     * - Otherwise, return the latest continuous records and leave the remaining range to block cache.
     * <p>
     * e.g. Cached block: [0, 10], [100, 200]
     * <p>
     * - query [0,10] returns [0,10] (fully satisfied)
     * </p>
     * <p>
     * - query [0, 11] returns empty list (left intersect, leave all data to block cache for simplification)
     * </p>
     * <p>
     * - query [5, 20] returns empty list (left intersect, leave all data to block cache for simplification)
     * </p>
     * <p>
     * - query [90, 110) returns [100, 110] (right intersect, leave[90, 100) to block cache)
     * </p>
     * <p>
     * - query [40, 50] returns empty list (miss match)
     * </p>
     * Note: the records is retained, the caller should release it.
     */
    public List<StreamRecordBatch> get(long streamId, long startOffset, long endOffset, int maxBytes) {
        List<StreamRecordBatch> records = get0(streamId, startOffset, endOffset, maxBytes);
        records.forEach(StreamRecordBatch::retain);
        return records;
    }

    public List<StreamRecordBatch> get0(long streamId, long startOffset, long endOffset, int maxBytes) {
        List<StreamRecordBatch> rst = new LinkedList<>();
        long nextStartOffset = startOffset;
        int nextMaxBytes = maxBytes;
        boolean fulfill = false;
        for (LogCacheBlock archiveBlock : blocks) {
            List<StreamRecordBatch> records = archiveBlock.get(streamId, nextStartOffset, endOffset, nextMaxBytes);
            if (records.isEmpty()) {
                continue;
            }
            nextStartOffset = records.get(records.size() - 1).getLastOffset();
            nextMaxBytes -= Math.min(nextMaxBytes, records.stream().mapToInt(StreamRecordBatch::size).sum());
            rst.addAll(records);
            if (nextStartOffset >= endOffset || nextMaxBytes == 0) {
                fulfill = true;
                break;
            }
        }
        if (fulfill) {
            return rst;
        } else {
            long lastBlockStreamStartOffset = NOOP_OFFSET;
            for (int i = blocks.size() - 1; i >= 0; i--) {
                LogCacheBlock block = blocks.get(i);
                StreamRange streamRange = block.getStreamRange(streamId);
                if (streamRange.endOffset == NOOP_OFFSET) {
                    continue;
                }
                if (lastBlockStreamStartOffset == NOOP_OFFSET || lastBlockStreamStartOffset == streamRange.endOffset) {
                    lastBlockStreamStartOffset = streamRange.startOffset;
                } else {
                    break;
                }
            }
            if (lastBlockStreamStartOffset == NOOP_OFFSET /* Mismatch */
                    || lastBlockStreamStartOffset >= endOffset /* non-right intersect */
                    || lastBlockStreamStartOffset <= startOffset /* left intersect */) {
                return Collections.emptyList();
            }
            return get0(streamId, lastBlockStreamStartOffset, endOffset, maxBytes);
        }
    }

    public LogCacheBlock archiveCurrentBlock() {
        LogCacheBlock block = activeBlock;
        block.confirmOffset = confirmOffset;
        activeBlock = new LogCacheBlock(cacheBlockMaxSize);
        blocks.add(activeBlock);
        return block;
    }

    public Optional<LogCacheBlock> archiveCurrentBlockIfContains(long streamId) {
        if (streamId == MATCH_ALL_STREAMS) {
            if (activeBlock.size > 0) {
                return Optional.of(archiveCurrentBlock());
            } else {
                return Optional.empty();
            }
        } else {
            if (activeBlock.map.containsKey(streamId)) {
                return Optional.of(archiveCurrentBlock());
            } else {
                return Optional.empty();
            }
        }

    }

    public void markFree(LogCacheBlock block) {
        block.free = true;
        tryRealFree();
    }

    private void tryRealFree() {
        if (size.get() <= cacheBlockMaxSize * 0.9) {
            return;
        }
        blocks.removeIf(b -> {
            if (b.free) {
                size.addAndGet(-b.size);
                blockFreeListener.accept(b);
            }
            return b.free;
        });
    }

    public void setConfirmOffset(long confirmOffset) {
        this.confirmOffset = confirmOffset;
    }

    public long size() {
        return size.get();
    }

    public static class LogCacheBlock {
        private static final AtomicLong BLOCK_ID_ALLOC = new AtomicLong();
        private final long blockId;
        private final long maxSize;
        private final Map<Long, List<StreamRecordBatch>> map = new HashMap<>();
        private long size = 0;
        private long confirmOffset;
        boolean free;

        public LogCacheBlock(long maxSize) {
            this.blockId = BLOCK_ID_ALLOC.getAndIncrement();
            this.maxSize = maxSize;
        }

        public long blockId() {
            return blockId;
        }

        public boolean put(StreamRecordBatch recordBatch) {
            List<StreamRecordBatch> streamCache = map.computeIfAbsent(recordBatch.getStreamId(), id -> new ArrayList<>());
            streamCache.add(recordBatch);
            int recordSize = recordBatch.size();
            size += recordSize;
            return size >= maxSize;
        }

        public List<StreamRecordBatch> get(long streamId, long startOffset, long endOffset, int maxBytes) {
            List<StreamRecordBatch> streamRecords = map.get(streamId);
            if (streamRecords == null) {
                return Collections.emptyList();
            }
            if (streamRecords.get(0).getBaseOffset() > startOffset || streamRecords.get(streamRecords.size() - 1).getLastOffset() <= startOffset) {
                return Collections.emptyList();
            }
            int startIndex = -1;
            int endIndex = -1;
            int remainingBytesSize = maxBytes;
            // TODO: binary search the startOffset.
            for (int i = 0; i < streamRecords.size(); i++) {
                StreamRecordBatch record = streamRecords.get(i);
                if (startIndex == -1 && record.getBaseOffset() <= startOffset && record.getLastOffset() > startOffset) {
                    startIndex = i;
                }
                if (startIndex != -1) {
                    endIndex = i + 1;
                    remainingBytesSize -= Math.min(remainingBytesSize, record.size());
                    if (record.getLastOffset() >= endOffset || remainingBytesSize == 0) {
                        break;
                    }
                }
            }
            return streamRecords.subList(startIndex, endIndex);
        }

        StreamRange getStreamRange(long streamId) {
            List<StreamRecordBatch> streamRecords = map.get(streamId);
            if (streamRecords == null || streamRecords.isEmpty()) {
                return new StreamRange(NOOP_OFFSET, NOOP_OFFSET);
            } else {
                return new StreamRange(streamRecords.get(0).getBaseOffset(), streamRecords.get(streamRecords.size() - 1).getLastOffset());
            }
        }

        public Map<Long, List<StreamRecordBatch>> records() {
            return map;
        }

        public long confirmOffset() {
            return confirmOffset;
        }

        public void confirmOffset(long confirmOffset) {
            this.confirmOffset = confirmOffset;
        }

        public long size() {
            return size;
        }
    }

    static class StreamRange {
        public static final long NOOP_OFFSET = -1L;
        long startOffset;
        long endOffset;

        public StreamRange(long startOffset, long endOffset) {
            this.startOffset = startOffset;
            this.endOffset = endOffset;
        }
    }
}
