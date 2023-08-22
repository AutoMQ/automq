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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import kafka.log.s3.exception.StreamFencedException;
import kafka.log.s3.model.StreamRecordBatch;
import kafka.log.s3.objects.CommitWalObjectRequest;
import kafka.log.s3.objects.CommitWalObjectResponse;
import kafka.log.s3.objects.ObjectManager;
import kafka.log.s3.objects.ObjectStreamRange;
import kafka.log.s3.operator.S3Operator;
import kafka.log.s3.utils.ObjectUtils;
import org.apache.kafka.common.compress.ZstdFactory;
import org.apache.kafka.common.utils.ByteBufferOutputStream;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class SingleWalObjectWriteTask {
    private final List<WalWriteRequest> requests;
    private final ObjectManager objectManager;
    private final S3Operator s3Operator;
    private final List<ObjectStreamRange> streamRanges;
    private ByteBuf objectBuf;
    private CommitWalObjectResponse response;
    private volatile boolean isDone = false;

    public SingleWalObjectWriteTask(List<WalWriteRequest> records, ObjectManager objectManager, S3Operator s3Operator) {
        Collections.sort(records);
        this.requests = records;
        this.streamRanges = new LinkedList<>();
        this.objectManager = objectManager;
        this.s3Operator = s3Operator;
        parse();
    }

    public CompletableFuture<Void> upload() {
        if (isDone) {
            return CompletableFuture.completedFuture(null);
        }
        UploadContext context = new UploadContext();
        CompletableFuture<Long> objectIdCf = objectManager.prepareObject(1, TimeUnit.SECONDS.toMillis(30));
        CompletableFuture<Void> writeCf = objectIdCf.thenCompose(objectId -> {
            context.objectId = objectId;
            // TODO: fill cluster name
            return s3Operator.write(ObjectUtils.genKey(0, "todocluster", objectId), objectBuf.duplicate());
        });
        return writeCf
                .thenCompose(nil -> {
                    CommitWalObjectRequest request = new CommitWalObjectRequest();
                    request.setObjectId(context.objectId);
                    request.setObjectSize(objectBuf.readableBytes());
                    request.setStreamRanges(streamRanges);
                    return objectManager.commitWalObject(request);
                })
                .thenApply(resp -> {
                    isDone = true;
                    response = resp;
                    objectBuf.release();
                    return null;
                });
    }

    public void parse() {
        int totalSize = requests.stream().mapToInt(r -> r.record.getRecordBatch().rawPayload().remaining()).sum();
        ByteBufferOutputStream compressed = new ByteBufferOutputStream(totalSize);
        OutputStream out = ZstdFactory.wrapForOutput(compressed);
        long streamId = -1;
        long streamStartOffset = -1;
        long streamEpoch = -1;
        long streamEndOffset = -1;
        for (WalWriteRequest request : requests) {
            StreamRecordBatch record = request.record;
            long currentStreamId = record.getStreamId();
            if (streamId != currentStreamId) {
                if (streamId != -1) {
                    streamRanges.add(new ObjectStreamRange(streamId, streamEpoch, streamStartOffset, streamEndOffset));
                }
                streamId = currentStreamId;
                streamEpoch = record.getEpoch();
                streamStartOffset = record.getBaseOffset();
            }
            streamEndOffset = record.getBaseOffset() + record.getRecordBatch().count();
            ByteBuf recordBuf = RecordBatchCodec.encode(record.getStreamId(), record.getBaseOffset(), record.getRecordBatch());
            try {
                out.write(recordBuf.array(), recordBuf.readerIndex(), recordBuf.readableBytes());
            } catch (IOException e) {
                // won't happen
                throw new RuntimeException(e);
            }
            recordBuf.release();
        }
        // add last stream range
        if (streamId != -1) {
            streamRanges.add(new ObjectStreamRange(streamId, streamEpoch, streamStartOffset, streamEndOffset));
        }
        objectBuf = Unpooled.wrappedBuffer(compressed.buffer().flip());
    }

    public boolean isDone() {
        return isDone;
    }

    public void ack() {
        Set<Long> failedStreamId = new HashSet<>(response.getFailedStreamIds());
        for (WalWriteRequest request : requests) {
            long streamId = request.record.getStreamId();
            if (failedStreamId.contains(streamId)) {
                request.cf.completeExceptionally(new StreamFencedException());
            } else {
                request.cf.complete(null);
            }
        }
    }

    static class UploadContext {
        long objectId;
    }

}
