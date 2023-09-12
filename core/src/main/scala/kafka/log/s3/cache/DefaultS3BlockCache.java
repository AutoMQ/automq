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

import kafka.log.es.FutureUtil;
import kafka.log.s3.ObjectReader;
import kafka.log.s3.model.StreamRecordBatch;
import kafka.log.s3.objects.ObjectManager;
import kafka.log.s3.operator.S3Operator;
import org.apache.kafka.common.utils.CloseableIterator;
import org.apache.kafka.metadata.stream.S3ObjectMetadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class DefaultS3BlockCache implements S3BlockCache {
    private final Map<Long, ObjectReader> objectReaders = new ConcurrentHashMap<>();
    private final BlockCache cache;
    private final ObjectManager objectManager;
    private final S3Operator s3Operator;

    public DefaultS3BlockCache(long cacheBytesSize, ObjectManager objectManager, S3Operator s3Operator) {
        this.cache = new BlockCache(cacheBytesSize);
        this.objectManager = objectManager;
        this.s3Operator = s3Operator;
    }

    @Override
    public CompletableFuture<ReadDataBlock> read(long streamId, long startOffset, long endOffset, int maxBytes) {
        if (startOffset >= endOffset || maxBytes <= 0) {
            return CompletableFuture.completedFuture(new ReadDataBlock(Collections.emptyList()));
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
        if (nextStartOffset >= endOffset || nextMaxBytes == 0) {
            return CompletableFuture.completedFuture(new ReadDataBlock(cacheRecords));
        }
        // 2. get from s3
        List<S3ObjectMetadata> objects = objectManager.getObjects(streamId, nextStartOffset, endOffset, 2);
        ReadContext context = new ReadContext(objects, nextStartOffset, nextMaxBytes);
        return read0(streamId, endOffset, context).thenApply(s3Rst -> {
            List<StreamRecordBatch> records = new ArrayList<>(cacheRst.getRecords());
            records.addAll(s3Rst.getRecords());
            return new ReadDataBlock(records);
        });
    }

    @Override
    public void put(Map<Long, List<StreamRecordBatch>> stream2records) {
        stream2records.forEach(cache::put);
    }

    private CompletableFuture<ReadDataBlock> read0(long streamId, long endOffset, ReadContext context) {
        if (context.objectIndex >= context.objects.size()) {
            context.objects = objectManager.getObjects(streamId, context.nextStartOffset, endOffset, 2);
            context.objectIndex = 0;
        }
        ObjectReader reader = getObjectReader(context.objects.get(context.objectIndex));
        context.objectIndex++;
        return reader.find(streamId, context.nextStartOffset, endOffset, context.nextMaxBytes).thenCompose(blockIndexes -> {
            List<CompletableFuture<ObjectReader.DataBlock>> blockCfList = blockIndexes.stream().map(reader::read).collect(Collectors.toList());
            CompletableFuture<Void> allBlockCf = CompletableFuture.allOf(blockCfList.toArray(new CompletableFuture[0]));
            return allBlockCf.thenCompose(nil -> {
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
                                    if (nextStartOffset >= endOffset || nextMaxBytes == 0) {
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
                        return read0(streamId, endOffset, context);
                    }
                } catch (Throwable e) {
                    return FutureUtil.failedFuture(e);
                }
            });
        });
    }

    private ObjectReader getObjectReader(S3ObjectMetadata metadata) {
        // remove expired readers and close it.
        // retain & release? or ref count to release?
        return objectReaders.computeIfAbsent(metadata.objectId(), id -> new ObjectReader(metadata, s3Operator));
    }

    static class ReadContext {
        List<S3ObjectMetadata> objects;
        int objectIndex;
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


}
