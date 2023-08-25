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

import kafka.log.s3.ObjectReader;
import kafka.log.s3.model.StreamRecordBatch;
import kafka.log.s3.objects.ObjectManager;
import kafka.log.s3.objects.S3ObjectMetadata;
import kafka.log.s3.operator.S3Operator;
import org.apache.kafka.common.utils.CloseableIterator;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class DefaultS3BlockCache implements S3BlockCache {
    private final ObjectManager objectManager;
    private final S3Operator s3Operator;

    public DefaultS3BlockCache(ObjectManager objectManager, S3Operator s3Operator) {
        this.objectManager = objectManager;
        this.s3Operator = s3Operator;
    }

    @Override
    public CompletableFuture<ReadDataBlock> read(long streamId, long startOffset, long endOffset, int maxBytes) {
        if (startOffset >= endOffset || maxBytes == 0) {
            return CompletableFuture.completedFuture(new ReadDataBlock(Collections.emptyList()));
        }
        List<S3ObjectMetadata> objects = objectManager.getObjects(streamId, startOffset, endOffset, 2);
        ReadContext context = new ReadContext(objects, startOffset, maxBytes);
        return read0(streamId, endOffset, context);
    }

    private CompletableFuture<ReadDataBlock> read0(long streamId, long endOffset, ReadContext context) {
        if (context.blocks == null || context.blocks.size() <= context.blockIndex) {
            if (context.objectIndex >= context.objects.size()) {
                context.objects = objectManager.getObjects(streamId, context.nextStartOffset, endOffset, 2);
                context.objectIndex = 0;
            }
            // previous object is completed read o or is the first object.
            context.reader = new ObjectReader(context.objects.get(context.objectIndex), s3Operator);
            context.objectIndex++;
            return context.reader.find(streamId, context.nextStartOffset, endOffset).thenCompose(blocks -> {
                context.blocks = blocks;
                context.blockIndex = 0;
                return read0(streamId, endOffset, context);
            });
        } else {
            return context.reader.read(context.blocks.get(context.blockIndex)).thenCompose(dataBlock -> {
                context.blockIndex++;
                long nextStartOffset = context.nextStartOffset;
                int nextMaxBytes = context.nextMaxBytes;
                boolean matched = false;
                try (CloseableIterator<StreamRecordBatch> it = dataBlock.iterator()) {
                    while (it.hasNext()) {
                        StreamRecordBatch recordBatch = it.next();
                        if (recordBatch.getStreamId() != streamId) {
                            if (matched) {
                                break;
                            }
                            continue;
                        }
                        matched = true;
                        if (recordBatch.getLastOffset() <= nextStartOffset) {
                            continue;
                        }
                        context.records.add(recordBatch);
                        nextStartOffset = recordBatch.getLastOffset();
                        nextMaxBytes -= Math.min(nextMaxBytes, recordBatch.getRecordBatch().rawPayload().remaining());
                        if (nextStartOffset >= endOffset || nextMaxBytes == 0) {
                            break;
                        }
                        // TODO: cache the remaining records
                    }
                }
                context.nextStartOffset = nextStartOffset;
                context.nextMaxBytes = nextMaxBytes;
                if (nextStartOffset >= endOffset || nextMaxBytes == 0) {
                    return CompletableFuture.completedFuture(new ReadDataBlock(context.records));
                } else {
                    return read0(streamId, endOffset, context);
                }
            });
        }
    }

    static class ReadContext {
        List<S3ObjectMetadata> objects;
        int objectIndex;
        ObjectReader reader;
        List<ObjectReader.DataBlockIndex> blocks;
        int blockIndex;
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
