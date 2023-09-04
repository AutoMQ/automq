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

import kafka.log.s3.FlatStreamRecordBatch;

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

    public LogCache(long cacheBlockMaxSize) {
        this.cacheBlockMaxSize = cacheBlockMaxSize;
        this.activeBlock = new LogCacheBlock(cacheBlockMaxSize);
    }

    public boolean put(FlatStreamRecordBatch recordBatch) {
        return activeBlock.put(recordBatch);
    }

    /**
     * Get streamId [startOffset, endOffset) range records with maxBytes limit.
     * If the cache only contain records after startOffset, the return list is empty.
     */
    public List<FlatStreamRecordBatch> get(long streamId, long startOffset, long endOffset, int maxBytes) {
        List<FlatStreamRecordBatch> rst = new LinkedList<>();
        long nextStartOffset = startOffset;
        int nextMaxBytes = maxBytes;
        for (LogCacheBlock archiveBlock : archiveBlocks) {
            // TODO: fast break when cache doesn't contains the startOffset.
            List<FlatStreamRecordBatch> records = archiveBlock.get(streamId, nextStartOffset, endOffset, nextMaxBytes);
            if (records.isEmpty()) {
                continue;
            }
            nextStartOffset = records.get(records.size() - 1).lastOffset();
            nextMaxBytes -= Math.min(nextMaxBytes, records.stream().mapToInt(r -> r.encodedBuf().readableBytes()).sum());
            rst.addAll(records);
            if (nextStartOffset >= endOffset || nextMaxBytes == 0) {
                return rst;
            }
        }
        List<FlatStreamRecordBatch> records = activeBlock.get(streamId, nextStartOffset, endOffset, nextMaxBytes);
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
        archiveBlocks.removeIf(b -> b.blockId == blockId);
    }

    public void setConfirmOffset(long confirmOffset) {
        this.confirmOffset = confirmOffset;
    }

    public static class LogCacheBlock {
        private static final AtomicLong BLOCK_ID_ALLOC = new AtomicLong();
        private final long blockId;
        private final long maxSize;
        private final Map<Long, List<FlatStreamRecordBatch>> map = new HashMap<>();
        private long size = 0;
        private long confirmOffset;

        public LogCacheBlock(long maxSize) {
            this.blockId = BLOCK_ID_ALLOC.getAndIncrement();
            this.maxSize = maxSize;
        }

        public long blockId() {
            return blockId;
        }

        public boolean put(FlatStreamRecordBatch recordBatch) {
            List<FlatStreamRecordBatch> streamCache = map.computeIfAbsent(recordBatch.streamId, id -> new ArrayList<>());
            streamCache.add(recordBatch);
            int recordSize = recordBatch.encodedBuf.readableBytes();
            size += recordSize;
            return size >= maxSize;
        }

        public List<FlatStreamRecordBatch> get(long streamId, long startOffset, long endOffset, int maxBytes) {
            List<FlatStreamRecordBatch> streamRecords = map.get(streamId);
            if (streamRecords == null) {
                return Collections.emptyList();
            }
            if (streamRecords.get(0).baseOffset > startOffset || streamRecords.get(streamRecords.size() - 1).lastOffset() <= startOffset) {
                return Collections.emptyList();
            }
            int startIndex = -1;
            int endIndex = -1;
            int remainingBytesSize = maxBytes;
            // TODO: binary search the startOffset.
            for (int i = 0; i < streamRecords.size(); i++) {
                FlatStreamRecordBatch record = streamRecords.get(i);
                if (startIndex == -1 && record.baseOffset <= startOffset && record.lastOffset() > startOffset) {
                    startIndex = i;
                }
                if (startIndex != -1) {
                    endIndex = i + 1;
                    remainingBytesSize -= Math.min(remainingBytesSize, record.encodedBuf().readableBytes());
                    if (record.lastOffset() >= endOffset || remainingBytesSize == 0) {
                        break;
                    }
                }
            }
            return streamRecords.subList(startIndex, endIndex);
        }

        public Map<Long, List<FlatStreamRecordBatch>> records() {
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
