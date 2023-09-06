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

package kafka.log.s3.cache;

import kafka.log.s3.model.StreamRecordBatch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

public class LogCache {
    private final long cacheBlockMaxSize;
    private final List<LogCacheBlock> archiveBlocks = new ArrayList<>();
    private LogCacheBlock activeBlock;
    private long confirmOffset;
    private final AtomicLong size = new AtomicLong();

    public LogCache(long cacheBlockMaxSize) {
        this.cacheBlockMaxSize = cacheBlockMaxSize;
        this.activeBlock = new LogCacheBlock(cacheBlockMaxSize);
    }

    public boolean put(StreamRecordBatch recordBatch) {
        size.addAndGet(recordBatch.size());
        return activeBlock.put(recordBatch);
    }

    /**
     * Get streamId [startOffset, endOffset) range records with maxBytes limit.
     * If the cache only contain records after startOffset, the return list is empty.
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
        for (LogCacheBlock archiveBlock : archiveBlocks) {
            // TODO: fast break when cache doesn't contains the startOffset.
            List<StreamRecordBatch> records = archiveBlock.get(streamId, nextStartOffset, endOffset, nextMaxBytes);
            if (records.isEmpty()) {
                continue;
            }
            nextStartOffset = records.get(records.size() - 1).getLastOffset();
            nextMaxBytes -= Math.min(nextMaxBytes, records.stream().mapToInt(StreamRecordBatch::size).sum());
            rst.addAll(records);
            if (nextStartOffset >= endOffset || nextMaxBytes == 0) {
                return rst;
            }
        }
        List<StreamRecordBatch> records = activeBlock.get(streamId, nextStartOffset, endOffset, nextMaxBytes);
        rst.addAll(records);
        return rst;
    }

    public LogCacheBlock archiveCurrentBlock() {
        LogCacheBlock block = activeBlock;
        block.confirmOffset = confirmOffset;
        archiveBlocks.add(block);
        activeBlock = new LogCacheBlock(cacheBlockMaxSize);
        return block;
    }

    public Optional<LogCacheBlock> archiveCurrentBlockIfContains(long streamId) {
        if (activeBlock.map.containsKey(streamId)) {
            return Optional.of(archiveCurrentBlock());
        } else {
            return Optional.empty();
        }
    }

    public void free(long blockId) {
        archiveBlocks.removeIf(b -> {
            boolean remove = b.blockId == blockId;
            if (remove) {
                size.addAndGet(-b.size);
                b.records().clear();
            }
            return remove;
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

        public Map<Long, List<StreamRecordBatch>> records() {
            return map;
        }

        public long confirmOffset() {
            return confirmOffset;
        }

        public void confirmOffset(long confirmOffset) {
            this.confirmOffset = confirmOffset;
        }

    }
}
