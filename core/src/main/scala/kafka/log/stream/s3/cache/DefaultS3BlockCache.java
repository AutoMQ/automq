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

package kafka.log.stream.s3.cache;

import kafka.log.stream.s3.objects.ObjectManager;
import kafka.log.stream.s3.operator.S3Operator;
import kafka.log.stream.utils.Threads;
import kafka.log.stream.s3.ObjectReader;
import kafka.log.stream.s3.model.StreamRecordBatch;
import org.apache.kafka.common.utils.CloseableIterator;
import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.metadata.stream.S3ObjectMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static kafka.log.stream.utils.FutureUtil.exec;
import static kafka.log.stream.utils.FutureUtil.failedFuture;
import static kafka.log.stream.utils.FutureUtil.propagate;
import static org.apache.kafka.metadata.stream.ObjectUtils.NOOP_OFFSET;

public class DefaultS3BlockCache implements S3BlockCache {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultS3BlockCache.class);
    private final LRUCache<Long, ObjectReader> objectReaderLRU = new LRUCache<>();
    private final Map<ReadingTaskKey, CompletableFuture<?>> readaheadTasks = new ConcurrentHashMap<>();
    private final BlockCache cache;
    private final ExecutorService mainExecutor;
    private final ObjectManager objectManager;
    private final S3Operator s3Operator;

    public DefaultS3BlockCache(long cacheBytesSize, ObjectManager objectManager, S3Operator s3Operator) {
        this.cache = new BlockCache(cacheBytesSize);
        this.mainExecutor = Threads.newFixedThreadPool(
                2,
                ThreadUtils.createThreadFactory("s3-block-cache-main-%d", false),
                LOGGER);
        this.objectManager = objectManager;
        this.s3Operator = s3Operator;
    }

    @Override
    public CompletableFuture<ReadDataBlock> read(long streamId, long startOffset, long endOffset, int maxBytes) {
        CompletableFuture<ReadDataBlock> readCf = new CompletableFuture<>();
        // submit read task to mainExecutor to avoid read slower the caller thread.
        mainExecutor.execute(() -> exec(() ->
                propagate(read0(streamId, startOffset, endOffset, maxBytes, true), readCf), readCf, LOGGER, "read")
        );
        return readCf;
    }

    public CompletableFuture<ReadDataBlock> read0(long streamId, long startOffset, long endOffset, int maxBytes, boolean awaitReadahead) {
        if (startOffset >= endOffset || maxBytes <= 0) {
            return CompletableFuture.completedFuture(new ReadDataBlock(Collections.emptyList()));
        }

        if (awaitReadahead) {
            // expect readahead will fill the cache with the data we need.
            CompletableFuture<?> readaheadCf = readaheadTasks.get(new ReadingTaskKey(streamId, startOffset));
            if (readaheadCf != null) {
                CompletableFuture<ReadDataBlock> readCf = new CompletableFuture<>();
                readaheadCf.whenComplete((nil, ex) -> propagate(read0(streamId, startOffset, endOffset, maxBytes, false), readCf));
                return readCf;
            }
        }

        long nextStartOffset = startOffset;
        int nextMaxBytes = maxBytes;
        // 1. get from cache
        BlockCache.GetCacheResult cacheRst = cache.get(streamId, nextStartOffset, endOffset, nextMaxBytes);
        List<StreamRecordBatch> cacheRecords = cacheRst.getRecords();
        if (!cacheRecords.isEmpty()) {
            nextStartOffset = cacheRecords.get(cacheRecords.size() - 1).getLastOffset();
            nextMaxBytes -= Math.min(nextMaxBytes, cacheRecords.stream().mapToInt(r -> r.getRecordBatch().rawPayload().remaining()).sum());
        }
        cacheRst.getReadahead().ifPresent(readahead -> backgroundReadahead(streamId, readahead));
        if (nextStartOffset >= endOffset || nextMaxBytes == 0) {
            return CompletableFuture.completedFuture(new ReadDataBlock(cacheRecords));
        }
        // 2. get from s3

        ReadContext context = new ReadContext(Collections.emptyList(), nextStartOffset, nextMaxBytes);
        CompletableFuture<List<S3ObjectMetadata>> getObjectsCf = objectManager.getObjects(streamId, nextStartOffset, endOffset, 2);
        return getObjectsCf.thenComposeAsync(objects -> {
            context.objects = objects;
            return readFromS3(streamId, endOffset, context)
                    .thenApply(s3Rst -> {
                        List<StreamRecordBatch> records = new ArrayList<>(cacheRst.getRecords());
                        records.addAll(s3Rst.getRecords());
                        return new ReadDataBlock(records);
                    });
        }, mainExecutor);
    }

    @Override
    public void put(Map<Long, List<StreamRecordBatch>> stream2records) {
        stream2records.forEach(cache::put);
    }

    private CompletableFuture<ReadDataBlock> readFromS3(long streamId, long endOffset, ReadContext context) {
        CompletableFuture<Void> getObjectsCf = CompletableFuture.completedFuture(null);
        if (context.objectIndex >= context.objects.size()) {
            getObjectsCf = objectManager
                    .getObjects(streamId, context.nextStartOffset, endOffset, 2)
                    .orTimeout(1, TimeUnit.MINUTES)
                    .thenAccept(objects -> {
                        context.objects = objects;
                        context.objectIndex = 0;
                        if (context.objects.isEmpty()) {
                            LOGGER.error("[BUG] fail to read, expect objects not empty, streamId={}, startOffset={}, endOffset={}",
                                    streamId, context.nextStartOffset, endOffset);
                            throw new IllegalStateException("fail to read, expect objects not empty");
                        }
                    });
        }
        CompletableFuture<List<ObjectReader.DataBlockIndex>> findIndexCf = getObjectsCf.thenCompose(nil -> {
            ObjectReader reader = getObjectReader(context.objects.get(context.objectIndex));
            context.objectIndex++;
            context.reader = reader;
            return reader.find(streamId, context.nextStartOffset, endOffset, context.nextMaxBytes);
        });
        CompletableFuture<List<CompletableFuture<ObjectReader.DataBlock>>> getDataCf = findIndexCf.thenCompose(blockIndexes -> {
            List<CompletableFuture<ObjectReader.DataBlock>> blockCfList = blockIndexes.stream().map(context.reader::read).collect(Collectors.toList());
            CompletableFuture<Void> allBlockCf = CompletableFuture.allOf(blockCfList.toArray(new CompletableFuture[0]));
            return allBlockCf.thenApply(nil -> blockCfList);
        });
        getDataCf.whenComplete((rst, ex) -> {
            context.reader.release();
            context.reader = null;
        });
        return getDataCf.thenComposeAsync(blockCfList -> {
            try {
                long nextStartOffset = context.nextStartOffset;
                int nextMaxBytes = context.nextMaxBytes;
                boolean fulfill = false;
                List<StreamRecordBatch> remaining = new ArrayList<>();
                for (CompletableFuture<ObjectReader.DataBlock> blockCf : blockCfList) {
                    ObjectReader.DataBlock dataBlock = blockCf.get();
                    try (CloseableIterator<StreamRecordBatch> it = dataBlock.iterator()) {
                        while (it.hasNext()) {
                            StreamRecordBatch recordBatch = it.next();
                            if (recordBatch.getLastOffset() <= nextStartOffset) {
                                recordBatch.release();
                                continue;
                            }
                            if (fulfill) {
                                remaining.add(recordBatch);
                            } else {
                                context.records.add(recordBatch);
                                nextStartOffset = recordBatch.getLastOffset();
                                nextMaxBytes -= Math.min(nextMaxBytes, recordBatch.getRecordBatch().rawPayload().remaining());
                                if ((endOffset != NOOP_OFFSET && nextStartOffset >= endOffset) || nextMaxBytes == 0) {
                                    fulfill = true;
                                }
                            }
                        }
                    }
                    dataBlock.close();
                }
                if (!remaining.isEmpty()) {
                    cache.put(streamId, remaining);
                }
                context.nextStartOffset = nextStartOffset;
                context.nextMaxBytes = nextMaxBytes;
                if (fulfill) {
                    return CompletableFuture.completedFuture(new ReadDataBlock(context.records));
                } else {
                    return readFromS3(streamId, endOffset, context);
                }
            } catch (Throwable e) {
                return failedFuture(e);
            }
        }, mainExecutor);
    }

    private void backgroundReadahead(long streamId, BlockCache.Readahead readahead) {
        mainExecutor.execute(() -> {
            CompletableFuture<List<S3ObjectMetadata>> getObjectsCf = objectManager.getObjects(streamId, readahead.getStartOffset(), NOOP_OFFSET, 2);
            getObjectsCf.thenAccept(objects -> {
                if (objects.isEmpty()) {
                    return;
                }
                CompletableFuture<ReadDataBlock> readaheadCf = readFromS3(streamId, NOOP_OFFSET,
                        new ReadContext(objects, readahead.getStartOffset(), readahead.getSize()));
                ReadingTaskKey readingTaskKey = new ReadingTaskKey(streamId, readahead.getStartOffset());
                readaheadTasks.put(readingTaskKey, readaheadCf);
                readaheadCf
                        .thenAccept(readDataBlock -> cache.put(streamId, readDataBlock.getRecords()))
                        .whenComplete((nil, ex) -> readaheadTasks.remove(readingTaskKey, readaheadCf));
            });
        });
    }

    private ObjectReader getObjectReader(S3ObjectMetadata metadata) {
        synchronized (objectReaderLRU) {
            // TODO: evict by object readers index cache size
            while (objectReaderLRU.size() > 128) {
                Optional.ofNullable(objectReaderLRU.pop()).ifPresent(entry -> entry.getValue().close());
            }
            ObjectReader objectReader = objectReaderLRU.get(metadata.objectId());
            if (objectReader == null) {
                objectReader = new ObjectReader(metadata, s3Operator);
                objectReaderLRU.put(metadata.objectId(), objectReader);
            }
            return objectReader.retain();
        }
    }

    static class ReadContext {
        List<S3ObjectMetadata> objects;
        int objectIndex;
        ObjectReader reader;
        List<StreamRecordBatch> records;
        long nextStartOffset;
        int nextMaxBytes;

        public ReadContext(List<S3ObjectMetadata> objects, long startOffset, int maxBytes) {
            this.objects = objects;
            this.records = new LinkedList<>();
            this.nextStartOffset = startOffset;
            this.nextMaxBytes = maxBytes;
        }

    }

    static class ReadingTaskKey {
        final long streamId;
        final long startOffset;

        public ReadingTaskKey(long streamId, long startOffset) {
            this.streamId = streamId;
            this.startOffset = startOffset;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ReadingTaskKey that = (ReadingTaskKey) o;
            return streamId == that.streamId && startOffset == that.startOffset;
        }

        @Override
        public int hashCode() {
            return Objects.hash(streamId, startOffset);
        }
    }

}
