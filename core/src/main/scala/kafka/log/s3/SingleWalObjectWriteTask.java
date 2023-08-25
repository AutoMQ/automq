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

import com.automq.elasticstream.client.api.ElasticStreamClientException;
import com.automq.elasticstream.client.flatc.header.ErrorCode;
import kafka.log.s3.objects.CommitWalObjectRequest;
import kafka.log.s3.objects.CommitWalObjectResponse;
import kafka.log.s3.objects.ObjectManager;
import kafka.log.s3.operator.S3Operator;
import kafka.log.s3.utils.ObjectUtils;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class SingleWalObjectWriteTask {
    private final List<WalWriteRequest> requests;
    private final ObjectManager objectManager;
    private final S3Operator s3Operator;
    private ObjectWriter objectWriter;
    private CommitWalObjectResponse response;
    private volatile boolean isDone = false;

    public SingleWalObjectWriteTask(List<WalWriteRequest> records, ObjectManager objectManager, S3Operator s3Operator) {
        Collections.sort(records);
        this.requests = records;
        this.objectManager = objectManager;
        this.s3Operator = s3Operator;
    }

    public CompletableFuture<Void> upload() {
        if (isDone) {
            return CompletableFuture.completedFuture(null);
        }
        // TODO: partial retry
        UploadContext context = new UploadContext();
        CompletableFuture<Long> objectIdCf = objectManager.prepareObject(1, TimeUnit.SECONDS.toMillis(30));
        CompletableFuture<Void> writeCf = objectIdCf.thenCompose(objectId -> {
            context.objectId = objectId;
            // TODO: fill cluster name
            String objectKey = ObjectUtils.genKey(0, "todocluster", objectId);
            objectWriter = new ObjectWriter(objectKey, s3Operator);
            for (WalWriteRequest request : requests) {
                objectWriter.write(request.record);
            }
            return objectWriter.close();
        });
        return writeCf
                .thenCompose(nil -> {
                    CommitWalObjectRequest request = new CommitWalObjectRequest();
                    request.setObjectId(context.objectId);
                    request.setObjectSize(objectWriter.size());
                    request.setStreamRanges(objectWriter.getStreamRanges());
                    return objectManager.commitWalObject(request);
                })
                .thenApply(resp -> {
                    isDone = true;
                    response = resp;
                    return null;
                });
    }

    public boolean isDone() {
        return isDone;
    }

    public void ack() {
        Set<Long> failedStreamId = new HashSet<>(response.getFailedStreamIds());
        for (WalWriteRequest request : requests) {
            long streamId = request.record.getStreamId();
            if (failedStreamId.contains(streamId)) {
                request.cf.completeExceptionally(new ElasticStreamClientException(ErrorCode.EXPIRED_STREAM_EPOCH, "Stream " + streamId + " epoch expired"));
            } else {
                request.cf.complete(null);
            }
        }
    }

    static class UploadContext {
        long objectId;
    }

}
