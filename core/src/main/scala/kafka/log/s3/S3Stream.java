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

package kafka.log.s3;

import com.automq.elasticstream.client.DefaultAppendResult;
import com.automq.elasticstream.client.api.AppendResult;
import com.automq.elasticstream.client.api.FetchResult;
import com.automq.elasticstream.client.api.RecordBatch;
import com.automq.elasticstream.client.api.RecordBatchWithContext;
import com.automq.elasticstream.client.api.Stream;
import kafka.log.s3.cache.ReadDataBlock;
import kafka.log.s3.cache.S3BlockCache;
import kafka.log.s3.model.StreamMetadata;
import kafka.log.s3.model.StreamRecordBatch;
import kafka.log.s3.objects.ObjectManager;
import kafka.log.s3.streams.StreamManager;

import java.util.LinkedList;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

public class S3Stream implements Stream {
    private final StreamMetadata metadata;
    private final long streamId;
    private final long epoch;
    private final AtomicLong nextOffset;
    private final Wal wal;
    private final S3BlockCache blockCache;
    private final StreamManager streamManager;
    private final ObjectManager objectManager;

    public S3Stream(StreamMetadata metadata, Wal wal, S3BlockCache blockCache, StreamManager streamManager, ObjectManager objectManager) {
        this.metadata = metadata;
        this.streamId = metadata.getStreamId();
        this.epoch = metadata.getEpoch();
        this.nextOffset = new AtomicLong(metadata.getRanges().get(metadata.getRanges().size() - 1).getStartOffset());
        this.wal = wal;
        this.blockCache = blockCache;
        this.streamManager = streamManager;
        this.objectManager = objectManager;
    }

    @Override
    public long streamId() {
        return metadata.getStreamId();
    }

    @Override
    public long startOffset() {
        return metadata.getStartOffset();
    }

    @Override
    public long nextOffset() {
        return nextOffset.get();
    }

    @Override
    public CompletableFuture<AppendResult> append(RecordBatch recordBatch) {
        long offset = nextOffset.getAndIncrement();
        StreamRecordBatch streamRecordBatch = new StreamRecordBatch(streamId, epoch, offset, recordBatch);
        return wal.append(streamRecordBatch).thenApply(nil -> new DefaultAppendResult(offset));
    }

    @Override
    public CompletableFuture<FetchResult> fetch(long startOffset, long endOffset, int maxBytes) {
        //TODO: bound check
        //TODO: concurrent read.
        //TODO: async read.
        List<Long> objects = objectManager.getObjects(streamId, startOffset, endOffset, maxBytes);
        long nextStartOffset = startOffset;
        int nextMaxBytes = maxBytes;
        List<RecordBatchWithContext> records = new LinkedList<>();
        for (long objectId : objects) {
            try {
                ReadDataBlock dataBlock = blockCache.read(objectId, streamId, nextStartOffset, endOffset, nextMaxBytes).get();
                OptionalLong blockStartOffset = dataBlock.startOffset();
                if (blockStartOffset.isEmpty()) {
                    throw new IllegalStateException("expect not empty block from object[" + objectId + "]");
                }
                if (blockStartOffset.getAsLong() != nextStartOffset) {
                    throw new IllegalStateException("records not continuous, expect start offset[" + nextStartOffset + "], actual["
                            + blockStartOffset.getAsLong() + "]");
                }
                records.addAll(dataBlock.getRecords());
                // Already check start offset, so it's safe to get end offset.
                //noinspection OptionalGetWithoutIsPresent
                nextStartOffset = dataBlock.endOffset().getAsLong();
                nextMaxBytes -= Math.min(nextMaxBytes, dataBlock.sizeInBytes());
                if (nextStartOffset >= endOffset || nextMaxBytes <= 0) {
                    break;
                }
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }
        //TODO: records integrity check.
        return CompletableFuture.completedFuture(new DefaultFetchResult(records));
    }

    @Override
    public CompletableFuture<Void> trim(long newStartOffset) {
        if (newStartOffset < metadata.getStartOffset()) {
            throw new IllegalArgumentException("newStartOffset[" + newStartOffset + "] cannot be less than current start offset["
                    + metadata.getStartOffset() + "]");
        }
        metadata.setStartOffset(newStartOffset);
        return streamManager.trimStream(metadata.getStreamId(), metadata.getEpoch(), newStartOffset);
    }

    @Override
    public CompletableFuture<Void> close() {
        // TODO: add stream status to fence future access.
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> destroy() {
        // TODO: add stream status to fence future access.
        return streamManager.deleteStream(streamId, epoch);
    }

    static class DefaultFetchResult implements FetchResult {
        private final List<RecordBatchWithContext> records;

        public DefaultFetchResult(List<RecordBatchWithContext> records) {
            this.records = records;
        }

        @Override
        public List<RecordBatchWithContext> recordBatchList() {
            return records;
        }
    }
}
