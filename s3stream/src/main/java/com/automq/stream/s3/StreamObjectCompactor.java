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

package com.automq.stream.s3;

import com.automq.stream.s3.metadata.ObjectUtils;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.network.ThrottleStrategy;
import com.automq.stream.s3.objects.CompactStreamObjectRequest;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.operator.S3Operator;
import com.automq.stream.s3.operator.Writer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stream objects compaction task.
 * It intends to compact some stream objects with the same stream ID into one new stream object.
 */
public class StreamObjectCompactor {
    /**
     * max object count in one group, the group count will limit the compact request size to kraft and multi-part object
     * part count (less than {@code Writer.MAX_PART_COUNT}).
     */
    private static final int MAX_OBJECT_GROUP_COUNT = Math.min(5000, Writer.MAX_PART_COUNT / 2);
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamObjectCompactor.class);
    public static final int DEFAULT_DATA_BLOCK_GROUP_SIZE_THRESHOLD = 1024 * 1024; // 1MiB
    private final Logger s3ObjectLogger;
    private final long maxStreamObjectSize;
    private final S3Stream stream;
    private final ObjectManager objectManager;
    private final S3Operator s3Operator;
    private final int dataBlockGroupSizeThreshold;

    private StreamObjectCompactor(ObjectManager objectManager, S3Operator s3Operator, S3Stream stream,
        long maxStreamObjectSize, int dataBlockGroupSizeThreshold) {
        this.objectManager = objectManager;
        this.s3Operator = s3Operator;
        this.stream = stream;
        this.maxStreamObjectSize = Math.min(maxStreamObjectSize, Writer.MAX_OBJECT_SIZE);
        String logIdent = "[StreamObjectsCompactionTask streamId=" + stream.streamId() + "] ";
        this.s3ObjectLogger = S3ObjectLogger.logger(logIdent);
        this.dataBlockGroupSizeThreshold = dataBlockGroupSizeThreshold;
    }

    public void compact() {
        try {
            compact0();
        } catch (Throwable e) {
            LOGGER.error("Failed to compact {} stream objects", stream.streamId(), e);
        }
    }

    void compact0() throws ExecutionException, InterruptedException {
        List<List<S3ObjectMetadata>> objectGroups = group();
        long streamId = stream.streamId();
        long startOffset = stream.startOffset();
        for (List<S3ObjectMetadata> objectGroup : objectGroups) {
            // the object group is single object and there is no data block need to be removed.
            if (objectGroup.size() == 1 && objectGroup.get(0).startOffset() >= startOffset) {
                continue;
            }
            long objectId = objectManager.prepareObject(1, TimeUnit.MINUTES.toMillis(60)).get();
            Optional<CompactStreamObjectRequest> requestOpt = new StreamObjectGroupCompactor(streamId, startOffset,
                objectGroup, objectId, dataBlockGroupSizeThreshold, s3Operator).compact();
            if (requestOpt.isPresent()) {
                CompactStreamObjectRequest request = requestOpt.get();
                objectManager.compactStreamObject(request).get();
                if (s3ObjectLogger.isTraceEnabled()) {
                    s3ObjectLogger.trace("{}", request);
                }
            }
        }
    }

    List<List<S3ObjectMetadata>> group() throws ExecutionException, InterruptedException {
        List<S3ObjectMetadata> objects = objectManager.getStreamObjects(stream.streamId(), stream.startOffset(), stream.confirmOffset(), Integer.MAX_VALUE).get();
        return group0(objects, maxStreamObjectSize);
    }

    static class StreamObjectGroupCompactor {
        private final List<S3ObjectMetadata> objectGroup;
        private final long streamId;
        private final long startOffset;
        // compact object group to the new object
        private final long objectId;
        private final S3Operator s3Operator;
        private final int dataBlockGroupSizeThreshold;

        public StreamObjectGroupCompactor(long streamId, long startOffset, List<S3ObjectMetadata> objectGroup,
            long objectId, int dataBlockGroupSizeThreshold, S3Operator s3Operator) {
            this.streamId = streamId;
            this.startOffset = startOffset;
            this.objectGroup = objectGroup;
            this.objectId = objectId;
            this.dataBlockGroupSizeThreshold = dataBlockGroupSizeThreshold;
            this.s3Operator = s3Operator;
        }

        public Optional<CompactStreamObjectRequest> compact() throws ExecutionException, InterruptedException {
            long nextBlockPosition = 0;
            long objectSize = 0;
            long compactedStartOffset = objectGroup.get(0).startOffset();
            long compactedEndOffset = objectGroup.get(objectGroup.size() - 1).endOffset();
            List<Long> compactedObjectIds = new LinkedList<>();
            CompositeByteBuf indexes = DirectByteBufAlloc.compositeByteBuffer();
            Writer writer = s3Operator.writer(ObjectUtils.genKey(0, objectId), ThrottleStrategy.THROTTLE_2);
            long groupStartOffset = -1L;
            long groupStartPosition = -1L;
            int groupSize = 0;
            int groupRecordCount = 0;
            DataBlockIndex lastIndex = null;
            for (S3ObjectMetadata object : objectGroup) {
                try (ObjectReader reader = new ObjectReader(object, s3Operator)) {
                    ObjectReader.BasicObjectInfo basicObjectInfo = reader.basicObjectInfo().get();
                    ByteBuf subIndexes = DirectByteBufAlloc.byteBuffer(basicObjectInfo.indexBlock().count() * DataBlockIndex.BLOCK_INDEX_SIZE);
                    Iterator<DataBlockIndex> it = basicObjectInfo.indexBlock().iterator();
                    long validDataBlockStartPosition = 0;
                    while (it.hasNext()) {
                        DataBlockIndex dataBlock = it.next();
                        if (dataBlock.endOffset() <= startOffset) {
                            validDataBlockStartPosition = dataBlock.endPosition();
                            compactedStartOffset = dataBlock.endOffset();
                            continue;
                        }
                        if (groupSize == 0 // the first data block
                            || (long) groupSize + dataBlock.size() > dataBlockGroupSizeThreshold
                            || (long) groupRecordCount + dataBlock.recordCount() > Integer.MAX_VALUE
                            || dataBlock.endOffset() - groupStartOffset > Integer.MAX_VALUE) {
                            if (groupSize != 0) {
                                new DataBlockIndex(streamId, groupStartOffset, (int) (lastIndex.endOffset() - groupStartOffset),
                                    groupRecordCount, groupStartPosition, groupSize).encode(subIndexes);
                            }
                            groupStartOffset = dataBlock.startOffset();
                            groupStartPosition = nextBlockPosition;
                            groupSize = 0;
                            groupRecordCount = 0;
                        }
                        groupSize += dataBlock.size();
                        groupRecordCount += dataBlock.recordCount();
                        nextBlockPosition += dataBlock.size();
                        lastIndex = dataBlock;
                    }
                    writer.copyWrite(ObjectUtils.genKey(0, object.objectId()), validDataBlockStartPosition, basicObjectInfo.dataBlockSize());
                    objectSize += basicObjectInfo.dataBlockSize() - validDataBlockStartPosition;
                    indexes.addComponent(true, subIndexes);
                    compactedObjectIds.add(object.objectId());
                }
            }
            if (lastIndex != null) {
                ByteBuf subIndexes = DirectByteBufAlloc.byteBuffer(DataBlockIndex.BLOCK_INDEX_SIZE);
                new DataBlockIndex(streamId, groupStartOffset, (int) (lastIndex.endOffset() - groupStartOffset),
                    groupRecordCount, groupStartPosition, groupSize).encode(subIndexes);
                indexes.addComponent(true, subIndexes);
            }

            CompositeByteBuf indexBlockAndFooter = DirectByteBufAlloc.compositeByteBuffer();
            indexBlockAndFooter.addComponent(true, indexes);
            indexBlockAndFooter.addComponent(true, new ObjectWriter.Footer(nextBlockPosition, indexBlockAndFooter.readableBytes()).buffer());

            objectSize += indexBlockAndFooter.readableBytes();
            writer.write(indexBlockAndFooter.duplicate());
            writer.close().get();
            return Optional.of(new CompactStreamObjectRequest(objectId, objectSize, streamId, compactedStartOffset, compactedEndOffset, compactedObjectIds));
        }

    }

    static List<List<S3ObjectMetadata>> group0(List<S3ObjectMetadata> objects, long maxStreamObjectSize) {
        List<List<S3ObjectMetadata>> objectGroups = new LinkedList<>();
        long groupSize = 0;
        long groupNextOffset = -1L;
        List<S3ObjectMetadata> group = new LinkedList<>();
        int partCount = 0;
        for (S3ObjectMetadata object : objects) {
            int objectPartCount = (int) ((object.objectSize() + Writer.MAX_PART_SIZE - 1) / Writer.MAX_PART_SIZE);
            if (objectPartCount >= Writer.MAX_PART_COUNT) {
                continue;
            }
            if (groupNextOffset == -1L) {
                groupNextOffset = object.startOffset();
            }
            // group the objects when the object's range is continuous
            if (groupNextOffset != object.startOffset()
                // the group object size is less than maxStreamObjectSize
                || (groupSize + object.objectSize() > maxStreamObjectSize && !group.isEmpty())
                // object count in group is larger than MAX_OBJECT_GROUP_COUNT
                || group.size() >= MAX_OBJECT_GROUP_COUNT
                || partCount + objectPartCount > Writer.MAX_PART_COUNT
            ) {
                objectGroups.add(group);
                group = new LinkedList<>();
                groupSize = 0;
            }
            group.add(object);
            groupSize += object.objectSize();
            groupNextOffset = object.endOffset();
            partCount += objectPartCount;
        }
        if (!group.isEmpty()) {
            objectGroups.add(group);
        }
        return objectGroups;
    }

    // no operation for now.
    public void close() {
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private ObjectManager objectManager;
        private S3Operator s3Operator;
        private S3Stream stream;
        private long maxStreamObjectSize;
        private int dataBlockGroupSizeThreshold = DEFAULT_DATA_BLOCK_GROUP_SIZE_THRESHOLD;

        public Builder objectManager(ObjectManager objectManager) {
            this.objectManager = objectManager;
            return this;
        }

        public Builder s3Operator(S3Operator s3Operator) {
            this.s3Operator = s3Operator;
            return this;
        }

        public Builder stream(S3Stream stream) {
            this.stream = stream;
            return this;
        }

        /**
         * Set compacted stream object max size.
         *
         * @param maxStreamObjectSize compacted stream object max size in bytes.
         *                            If it is bigger than {@link Writer#MAX_OBJECT_SIZE},
         *                            it will be set to {@link Writer#MAX_OBJECT_SIZE}.
         * @return builder.
         */
        public Builder maxStreamObjectSize(long maxStreamObjectSize) {
            this.maxStreamObjectSize = maxStreamObjectSize;
            return this;
        }

        public Builder dataBlockGroupSizeThreshold(int dataBlockGroupSizeThreshold) {
            this.dataBlockGroupSizeThreshold = dataBlockGroupSizeThreshold;
            return this;
        }

        public StreamObjectCompactor build() {
            return new StreamObjectCompactor(objectManager, s3Operator, stream, maxStreamObjectSize, dataBlockGroupSizeThreshold);
        }
    }
}
