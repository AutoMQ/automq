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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Stack;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import kafka.log.s3.objects.CommitStreamObjectRequest;
import kafka.log.s3.objects.ObjectManager;
import kafka.log.s3.operator.S3Operator;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.metadata.stream.S3ObjectMetadata;
import org.apache.kafka.metadata.stream.S3ObjectType;
import org.apache.kafka.metadata.stream.S3StreamObjectMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stream objects compaction task.
 * It intends to compact some stream objects with the same stream ID into one new stream object.
 */
public class StreamObjectsCompactionTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamObjectsCompactionTask.class);
    private Queue<List<S3StreamObjectMetadata>> compactGroups;
    private final long compactedStreamObjectMaxSize;
    private final long compactableStreamObjectLivingTimeInMs;
    private long nextStartSearchingOffset;
    private final S3Stream stream;
    private final ObjectManager objectManager;
    private final S3Operator s3Operator;
    public StreamObjectsCompactionTask(ObjectManager objectManager, S3Operator s3Operator, S3Stream stream,
        long compactedStreamObjectMaxSize, long compactableStreamObjectLivingTimeInMs) {
        this.objectManager = objectManager;
        this.s3Operator = s3Operator;
        this.stream = stream;
        this.compactedStreamObjectMaxSize = compactedStreamObjectMaxSize;
        this.compactableStreamObjectLivingTimeInMs = compactableStreamObjectLivingTimeInMs;
        this.nextStartSearchingOffset = stream.startOffset();
    }

    private CompletableFuture<Void> doCompaction(List<S3StreamObjectMetadata> streamObjectMetadataList) {
        List<S3ObjectMetadata> objectMetadatas = streamObjectMetadataList.stream().map(metadata ->
            new S3ObjectMetadata(metadata.objectId(), metadata.objectSize(), S3ObjectType.STREAM)
        ).collect(Collectors.toList());

        long startOffset = streamObjectMetadataList.get(0).startOffset();
        long endOffset = streamObjectMetadataList.get(streamObjectMetadataList.size() - 1).endOffset();
        List<Long> sourceObjectIds = streamObjectMetadataList
            .stream()
            .map(S3StreamObjectMetadata::objectId)
            .collect(Collectors.toList());

        if (stream.isClosed()) {
            return CompletableFuture.failedFuture(new HaltException("halt compaction task with stream "
                + stream.streamId()
                + " with offset range from "
                + startOffset
                + " to "
                + endOffset));
        }

        return objectManager.prepareObject(1, TimeUnit.MINUTES.toMillis(30))
            .thenCompose(objId -> {
                StreamObjectCopyer objectCopyer = new StreamObjectCopyer(objId, s3Operator);
                objectMetadatas.forEach(objectCopyer::write);
                return objectCopyer
                    .close()
                    .thenApply(v -> new CommitStreamObjectRequest(objId, objectCopyer.size(), stream.streamId(), startOffset, endOffset, sourceObjectIds));
            })
            .thenCompose(request -> objectManager
                .commitStreamObject(request)
                .thenApply(resp -> {
                    LOGGER.info("stream objects compaction task for stream {} from {} to {} is done", stream.streamId(), startOffset, endOffset);
                    return null;
                })
            );
    }

    public CompletableFuture<Void> doCompactions() {
        CompletableFuture<Void> lastCompactionFuture = CompletableFuture.completedFuture(null);
        while (!compactGroups.isEmpty()) {
            List<S3StreamObjectMetadata> streamObjectMetadataList = compactGroups.poll();
            CompletableFuture<Void> future = new CompletableFuture<>();
            lastCompactionFuture.whenComplete((v, ex) -> {
                if (ex != null) {
                    future.completeExceptionally(ex);
                } else {
                    // only start when last compaction is done.
                    doCompaction(streamObjectMetadataList).whenComplete((v1, ex1) -> {
                        if (ex1 != null) {
                            LOGGER.error("get exception when do compactions: {}", ex1.getMessage());
                            future.completeExceptionally(ex1);
                        } else {
                            future.complete(v1);
                        }
                    });
                }
            });
            lastCompactionFuture = future;
        }
        return lastCompactionFuture;
    }

    public void prepare() {
        this.compactGroups = prepareCompactGroups(this.nextStartSearchingOffset);
    }

    /**
     * Prepare compact groups. Each group will be compacted into one new stream object.
     * nextStartSearchingOffset will also be calculated for next compaction task.
     *
     * @param startSearchingOffset start searching offset.
     * @return compact groups.
     */
    public Queue<List<S3StreamObjectMetadata>> prepareCompactGroups(long startSearchingOffset) {
        long startOffset = Utils.max(startSearchingOffset, stream.startOffset());
        List<S3StreamObjectMetadata> rawFetchedStreamObjects = objectManager
            .getStreamObjects(stream.streamId(), startOffset, stream.nextOffset(), Integer.MAX_VALUE);

        this.nextStartSearchingOffset = calculateNextStartSearchingOffset(rawFetchedStreamObjects, startOffset);

        List<S3StreamObjectMetadata> streamObjects = rawFetchedStreamObjects
            .stream()
            .filter(streamObject -> streamObject.objectSize() < compactedStreamObjectMaxSize)
            .collect(Collectors.toList());
        Collections.sort(streamObjects);

        return groupContinuousObjects(streamObjects)
            .stream()
            .map(this::groupEligibleObjects)
            .reduce(new LinkedList<>(), (acc, item) -> {
                acc.addAll(item);
                return acc;
            });
    }

    public long getNextStartSearchingOffset() {
        return nextStartSearchingOffset;
    }

    // no operation for now.
    public void close() {}

    /**
     * Calculate next start searching offset. It will be used for next compaction task.
     *
     * @param streamObjects           stream objects.
     * @param rawStartSearchingOffset raw start searching offset.
     * @return next start searching offset.
     */
    private long calculateNextStartSearchingOffset(List<S3StreamObjectMetadata> streamObjects,
        long rawStartSearchingOffset) {
        long lastEndOffset = rawStartSearchingOffset;
        if (streamObjects == null || streamObjects.isEmpty()) {
            return lastEndOffset;
        }

        int index = 0;
        while (index < streamObjects.size()
            && streamObjects.get(index).startOffset() == lastEndOffset
            && streamObjects.get(index).objectSize() >= compactedStreamObjectMaxSize) {
            lastEndOffset = streamObjects.get(index).endOffset();
            index += 1;
        }
        return lastEndOffset;
    }

    /**
     * Group stream objects with continuous offsets. Each group should have more than one stream object.
     *
     * @param streamObjects stream objects.
     * @return object groups.
     */
    private List<List<S3StreamObjectMetadata>> groupContinuousObjects(List<S3StreamObjectMetadata> streamObjects) {
        if (streamObjects == null || streamObjects.size() <= 1) {
            return new LinkedList<>();
        }

        List<Stack<S3StreamObjectMetadata>> stackList = new LinkedList<>();
        Stack<S3StreamObjectMetadata> stack = new Stack<>();
        stackList.add(stack);

        for (S3StreamObjectMetadata object : streamObjects) {
            if (stack.isEmpty()) {
                stack.push(object);
            } else {
                if (object.startOffset() < stack.peek().endOffset()) {
                    throw new RuntimeException("get overlapped stream objects");
                }
                if (object.startOffset() == stack.peek().endOffset()) {
                    stack.push(object);
                } else {
                    stack = new Stack<>();
                    stack.push(object);
                    stackList.add(stack);
                }
            }
        }

        return stackList
            .stream()
            .filter(s -> s.size() > 1)
            .map(ArrayList::new)
            .collect(Collectors.toList());
    }

    /**
     * Further split the stream object group.
     * It tries to filter some subgroups which just have a size less than {@link #compactedStreamObjectMaxSize} and have
     * a living time more than {@link #compactableStreamObjectLivingTimeInMs}.
     *
     * @param streamObjects stream objects.
     * @return stream object subgroups.
     */
    private Queue<List<S3StreamObjectMetadata>> groupEligibleObjects(List<S3StreamObjectMetadata> streamObjects) {
        if (streamObjects == null || streamObjects.size() <= 1) {
            return new LinkedList<>();
        }

        Queue<List<S3StreamObjectMetadata>> groups = new LinkedList<>();

        int startIndex = 0;
        int endIndex = 0;
        while (startIndex < streamObjects.size() - 1) {
            endIndex = startIndex + 1;
            while (endIndex <= streamObjects.size()) {
                List<S3StreamObjectMetadata> subGroup = streamObjects.subList(startIndex, endIndex);
                // The subgroup is too new or too big, then break;
                if (calculateTimePassedInMs(subGroup) < compactableStreamObjectLivingTimeInMs ||
                    calculateTotalSize(subGroup) > compactedStreamObjectMaxSize) {
                    break;
                }
                endIndex += 1;
            }
            if (endIndex - 2 > startIndex) {
                groups.add(streamObjects.subList(startIndex, endIndex - 1));
                startIndex = endIndex - 1;
            } else {
                startIndex += 1;
            }
        }
        return groups;
    }

    private long calculateTimePassedInMs(List<S3StreamObjectMetadata> streamObjects) {
        return System.currentTimeMillis() - streamObjects.stream().mapToLong(S3StreamObjectMetadata::timestamp).max().orElse(0L);
    }

    private long calculateTotalSize(List<S3StreamObjectMetadata> streamObjects) {
        return streamObjects.stream().mapToLong(S3StreamObjectMetadata::objectSize).sum();
    }

    public static class HaltException extends RuntimeException {
        public HaltException(String message) {
            super(message);
        }
    }
}
