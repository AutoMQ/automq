/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3;

import com.automq.stream.api.Stream;
import com.automq.stream.s3.metadata.ObjectUtils;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metrics.TimerUtil;
import com.automq.stream.s3.network.ThrottleStrategy;
import com.automq.stream.s3.objects.CompactStreamObjectRequest;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.operator.S3Operator;
import com.automq.stream.s3.operator.Writer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.automq.stream.s3.ByteBufAlloc.STREAM_OBJECT_COMPACTION_READ;
import static com.automq.stream.s3.ByteBufAlloc.STREAM_OBJECT_COMPACTION_WRITE;
import static com.automq.stream.s3.metadata.ObjectUtils.NOOP_OBJECT_ID;
import static com.automq.stream.s3.metadata.ObjectUtils.NOOP_OFFSET;

/**
 * Stream objects compaction task.
 * It intends to:
 * 1. Clean up expired stream objects.
 * 2. Compact some stream objects with the same stream ID into bigger stream objects.
 */
public class StreamObjectCompactor {
    public static final int EXPIRED_OBJECTS_CLEAN_UP_STEP = 1000;
    public static final long MINOR_COMPACTION_SIZE_THRESHOLD = 128 * 1024 * 1024; // 128MiB

    /**
     * max object count in one group, the group count will limit the compact request size to kraft and multipart object
     * part count (less than {@code Writer.MAX_PART_COUNT}).
     */
    private static final int MAX_OBJECT_GROUP_COUNT = Math.min(5000, Writer.MAX_PART_COUNT / 2);
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamObjectCompactor.class);
    public static final int DEFAULT_DATA_BLOCK_GROUP_SIZE_THRESHOLD = 1024 * 1024; // 1MiB
    private final Logger s3ObjectLogger;
    private final long maxStreamObjectSize;
    private final Stream stream;
    private final ObjectManager objectManager;
    private final S3Operator s3Operator;
    private final int dataBlockGroupSizeThreshold;
    private CompactStreamObjectRequest request;

    private StreamObjectCompactor(ObjectManager objectManager, S3Operator s3Operator, Stream stream,
        long maxStreamObjectSize, int dataBlockGroupSizeThreshold) {
        this.objectManager = objectManager;
        this.s3Operator = s3Operator;
        this.stream = stream;
        this.maxStreamObjectSize = Math.min(maxStreamObjectSize, Writer.MAX_OBJECT_SIZE);
        String logIdent = "[StreamObjectsCompactionTask streamId=" + stream.streamId() + "] ";
        this.s3ObjectLogger = S3ObjectLogger.logger(logIdent);
        this.dataBlockGroupSizeThreshold = dataBlockGroupSizeThreshold;
    }

    public void compact(CompactionType compactionType) {
        try {
            compact0(compactionType);
        } catch (Throwable e) {
            handleCompactException(compactionType, e);
        }
    }

    private void handleCompactException(CompactionType compactionType, Throwable e) {
        if (stream instanceof S3StreamClient.StreamWrapper && ((S3StreamClient.StreamWrapper) stream).isClosed()) {
            LOGGER.warn("[STREAM_OBJECT_COMPACT_FAIL],[STREAM_CLOSED],{},type={},req={}", stream.streamId(), compactionType, request, e);
        } else {
            LOGGER.error("[STREAM_OBJECT_COMPACT_FAIL],[UNEXPECTED],{},type={},req={}", stream.streamId(), compactionType, request, e);
        }
    }

    void compact0(CompactionType compactionType) throws ExecutionException, InterruptedException {
        long streamId = stream.streamId();
        long startOffset = stream.startOffset();

        List<S3ObjectMetadata> objects = objectManager.getStreamObjects(stream.streamId(), 0L, stream.confirmOffset(), Integer.MAX_VALUE).get();
        List<S3ObjectMetadata> expiredObjects = new ArrayList<>(objects.size());
        List<S3ObjectMetadata> livingObjects = new ArrayList<>(objects.size());
        for (S3ObjectMetadata object : objects) {
            if (object.endOffset() <= startOffset) {
                expiredObjects.add(object);
            } else {
                livingObjects.add(object);
            }
        }

        // clean up the expired objects
        if (!expiredObjects.isEmpty()) {
            List<Long> compactedObjectIds = expiredObjects.stream().map(S3ObjectMetadata::objectId).collect(Collectors.toList());
            int expiredObjectCount = compactedObjectIds.size();
            // limit the expired objects compaction step to EXPIRED_OBJECTS_CLEAN_UP_STEP
            for (int i = 0; i < expiredObjectCount; ) {
                int start = i;
                int end = Math.min(i + EXPIRED_OBJECTS_CLEAN_UP_STEP, expiredObjectCount);
                request = new CompactStreamObjectRequest(NOOP_OBJECT_ID, 0,
                    streamId, stream.streamEpoch(), NOOP_OFFSET, NOOP_OFFSET, new ArrayList<>(compactedObjectIds.subList(start, end)));
                objectManager.compactStreamObject(request).get();
                if (s3ObjectLogger.isTraceEnabled()) {
                    s3ObjectLogger.trace("{}", request);
                }
                i = end;
            }

        }

        if (CompactionType.CLEANUP.equals(compactionType)) {
            return;
        }

        // compact the living objects
        long maxStreamObjectSize = CompactionType.MINOR.equals(compactionType) ? MINOR_COMPACTION_SIZE_THRESHOLD : this.maxStreamObjectSize;
        maxStreamObjectSize = Math.min(maxStreamObjectSize, this.maxStreamObjectSize);
        List<List<S3ObjectMetadata>> objectGroups = group0(livingObjects, maxStreamObjectSize);
        for (List<S3ObjectMetadata> objectGroup : objectGroups) {
            if (objectGroup.size() == 1) {
                // TODO: find a better way to cleanup the single head object
                continue;
            }
            TimerUtil start = new TimerUtil();
            long objectId = objectManager.prepareObject(1, TimeUnit.MINUTES.toMillis(60)).get();
            Optional<CompactStreamObjectRequest> requestOpt = new StreamObjectGroupCompactor(streamId, stream.streamEpoch(),
                startOffset, objectGroup, objectId, dataBlockGroupSizeThreshold, s3Operator).compact();
            if (requestOpt.isPresent()) {
                request = requestOpt.get();
                objectManager.compactStreamObject(request).get();
                if (s3ObjectLogger.isTraceEnabled()) {
                    s3ObjectLogger.trace("{} cost {}ms", request, start.elapsedAs(TimeUnit.MILLISECONDS));
                }
            }
        }
    }

    static class StreamObjectGroupCompactor {
        private final List<S3ObjectMetadata> objectGroup;
        private final long streamId;
        private final long streamEpoch;
        private final long startOffset;
        // compact object group to the new object
        private final long objectId;
        private final S3Operator s3Operator;
        private final int dataBlockGroupSizeThreshold;

        public StreamObjectGroupCompactor(long streamId, long streamEpoch, long startOffset,
            List<S3ObjectMetadata> objectGroup,
            long objectId, int dataBlockGroupSizeThreshold, S3Operator s3Operator) {
            this.streamId = streamId;
            this.streamEpoch = streamEpoch;
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
            CompositeByteBuf indexes = ByteBufAlloc.compositeByteBuffer();
            Writer writer = s3Operator.writer(new Writer.Context(STREAM_OBJECT_COMPACTION_READ), ObjectUtils.genKey(0, objectId), ThrottleStrategy.COMPACTION);
            long groupStartOffset = -1L;
            long groupStartPosition = -1L;
            int groupSize = 0;
            int groupRecordCount = 0;
            DataBlockIndex lastIndex = null;
            for (S3ObjectMetadata object : objectGroup) {
                try (ObjectReader reader = new ObjectReader(object, s3Operator)) {
                    ObjectReader.BasicObjectInfo basicObjectInfo = reader.basicObjectInfo().get();
                    ByteBuf subIndexes = ByteBufAlloc.byteBuffer(basicObjectInfo.indexBlock().count() * DataBlockIndex.BLOCK_INDEX_SIZE, STREAM_OBJECT_COMPACTION_WRITE);
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
                    writer.copyWrite(object, validDataBlockStartPosition, basicObjectInfo.dataBlockSize());
                    objectSize += basicObjectInfo.dataBlockSize() - validDataBlockStartPosition;
                    indexes.addComponent(true, subIndexes);
                    compactedObjectIds.add(object.objectId());
                } catch (Throwable t) {
                    LOGGER.error("[COPY_WRITE_FAILED] streamId={}, objectId={}, {}", streamId, object.objectId(), t.getMessage());
                    indexes.release();
                    writer.release();
                    throw t;
                }
            }
            if (lastIndex != null) {
                ByteBuf subIndexes = ByteBufAlloc.byteBuffer(DataBlockIndex.BLOCK_INDEX_SIZE, STREAM_OBJECT_COMPACTION_WRITE);
                new DataBlockIndex(streamId, groupStartOffset, (int) (lastIndex.endOffset() - groupStartOffset),
                    groupRecordCount, groupStartPosition, groupSize).encode(subIndexes);
                indexes.addComponent(true, subIndexes);
            }

            CompositeByteBuf indexBlockAndFooter = ByteBufAlloc.compositeByteBuffer();
            indexBlockAndFooter.addComponent(true, indexes);
            indexBlockAndFooter.addComponent(true, new ObjectWriter.Footer(nextBlockPosition, indexBlockAndFooter.readableBytes()).buffer());

            objectSize += indexBlockAndFooter.readableBytes();
            writer.write(indexBlockAndFooter.duplicate());
            writer.close().get();
            return Optional.of(new CompactStreamObjectRequest(objectId, objectSize, streamId, streamEpoch,
                compactedStartOffset, compactedEndOffset, compactedObjectIds));
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
                // object count in a group is larger than MAX_OBJECT_GROUP_COUNT
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
        private Stream stream;
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

        public Builder stream(Stream stream) {
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

    public enum CompactionType {
        // cleanup: only remove the expired objects.
        CLEANUP,
        // minor: limit the max compaction size to MINOR_COMPACTION_SIZE_THRESHOLD to quick compact the small objects.
        MINOR,
        // major: full compact the objects.
        MAJOR
    }
}
