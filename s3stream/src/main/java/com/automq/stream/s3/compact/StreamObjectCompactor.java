/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3.compact;

import com.automq.stream.api.Stream;
import com.automq.stream.s3.ByteBufAlloc;
import com.automq.stream.s3.CompositeObject;
import com.automq.stream.s3.CompositeObjectReader.BasicObjectInfoExt;
import com.automq.stream.s3.CompositeObjectReader.ObjectIndex;
import com.automq.stream.s3.CompositeObjectWriter;
import com.automq.stream.s3.DataBlockIndex;
import com.automq.stream.s3.ObjectReader;
import com.automq.stream.s3.ObjectWriter;
import com.automq.stream.s3.S3ObjectLogger;
import com.automq.stream.s3.S3StreamClient;
import com.automq.stream.s3.metadata.ObjectUtils;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metrics.TimerUtil;
import com.automq.stream.s3.network.ThrottleStrategy;
import com.automq.stream.s3.objects.CompactStreamObjectRequest;
import com.automq.stream.s3.objects.ObjectAttributes;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.objects.ObjectStreamRange;
import com.automq.stream.s3.operator.LocalFileObjectStorage;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.ObjectStorage.ObjectPath;
import com.automq.stream.s3.operator.ObjectStorage.WriteOptions;
import com.automq.stream.s3.operator.Writer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;

import static com.automq.stream.s3.ByteBufAlloc.STREAM_OBJECT_COMPACTION_READ;
import static com.automq.stream.s3.ByteBufAlloc.STREAM_OBJECT_COMPACTION_WRITE;
import static com.automq.stream.s3.Constants.NOOP_OBJECT_ID;
import static com.automq.stream.s3.compact.StreamObjectCompactor.CompactionType.CLEANUP;
import static com.automq.stream.s3.compact.StreamObjectCompactor.CompactionType.CLEANUP_V1;
import static com.automq.stream.s3.compact.StreamObjectCompactor.CompactionType.MAJOR;
import static com.automq.stream.s3.compact.StreamObjectCompactor.CompactionType.MAJOR_V1;
import static com.automq.stream.s3.compact.StreamObjectCompactor.CompactionType.MINOR;
import static com.automq.stream.s3.compact.StreamObjectCompactor.CompactionType.MINOR_V1;
import static com.automq.stream.s3.metadata.ObjectUtils.NOOP_OFFSET;
import static com.automq.stream.s3.objects.ObjectAttributes.Type.Composite;
import static com.automq.stream.s3.objects.ObjectAttributes.Type.Normal;

/**
 * Stream objects compaction task.
 * It intends to:
 * 1. Clean up expired stream objects.
 * 2. Compact some stream objects with the same stream ID into bigger stream objects.
 */
public class StreamObjectCompactor {
    public static final int EXPIRED_OBJECTS_CLEAN_UP_STEP = 1000;
    public static final long MINOR_COMPACTION_SIZE_THRESHOLD = 128 * 1024 * 1024; // 128MiB
    public static final long MINOR_V1_COMPACTION_SIZE_THRESHOLD = 4 * 1024 * 1024; // 4MiB
    /**
     * max object count in one group, the group count will limit the compact request size to kraft and multipart object
     * part count (less than {@code Writer.MAX_PART_COUNT}).
     */
    private static final int MAX_OBJECT_GROUP_COUNT = Math.min(5000, Writer.MAX_PART_COUNT / 2);
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamObjectCompactor.class);
    public static final int DEFAULT_DATA_BLOCK_GROUP_SIZE_THRESHOLD = 1024 * 1024; // 1MiB
    private static final long MAX_DIRTY_BYTES = 512 * 1024 * 1024;
    protected static final EnumSet<CompactionType> SKIP_COMPACTION_TYPE_WHEN_ONE_OBJECT_IN_GROUP =
        EnumSet.of(MINOR, MAJOR, MINOR_V1, MAJOR_V1);

    private final Logger s3ObjectLogger;
    private final long maxStreamObjectSize;
    private final Stream stream;
    private final ObjectManager objectManager;
    private final ObjectStorage objectStorage;
    private final int dataBlockGroupSizeThreshold;
    private final long minorV1CompactionThreshold;
    private final boolean majorV1CompactionSkipSmallObject;
    private CompactStreamObjectRequest request;

    private StreamObjectCompactor(ObjectManager objectManager,
                                  ObjectStorage objectStorage,
                                  Stream stream,
                                  long maxStreamObjectSize,
                                  int dataBlockGroupSizeThreshold,
                                  long minorV1CompactionThreshold,
                                  boolean majorV1CompactionSkipSmallObject) {
        this.objectManager = objectManager;
        this.objectStorage = objectStorage;
        this.stream = stream;
        this.maxStreamObjectSize = Math.min(maxStreamObjectSize, Writer.MAX_OBJECT_SIZE);
        String logIdent = "[StreamObjectsCompactionTask streamId=" + stream.streamId() + "] ";
        this.s3ObjectLogger = S3ObjectLogger.logger(logIdent);
        this.dataBlockGroupSizeThreshold = dataBlockGroupSizeThreshold;
        this.minorV1CompactionThreshold = minorV1CompactionThreshold;
        this.majorV1CompactionSkipSmallObject = majorV1CompactionSkipSmallObject;
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

    protected static Predicate<S3ObjectMetadata> getObjectFilter(CompactionType compactionType, long minMajorV1CompactionSize) {
        boolean includeCompositeObject = CLEANUP_V1.equals(compactionType) || MAJOR_V1.equals(compactionType);

        return object -> {
            ObjectAttributes.Type objectType = ObjectAttributes.from(object.attributes()).type();
            if (!includeCompositeObject && objectType == Composite) {
                return false;
            }

            // MAJOR_V1 compaction won't compact small object into composite object.
            // let MINOR_V1 handle small object merge.
            // set minMajorV1CompactionSize = 0 to disable this.
            if (MAJOR_V1.equals(compactionType) &&
                objectType == Normal &&
                object.objectSize() < minMajorV1CompactionSize) {
                return false;
            }

            return true;
        };
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
        cleanupExpiredObject(expiredObjects);

        if (CLEANUP.equals(compactionType)) {
            return;
        }

        // compact the living objects
        List<List<S3ObjectMetadata>> objectGroups = group0(livingObjects,
            getMaxGroupSize(compactionType),
            getObjectFilter(compactionType, majorV1CompactionSkipSmallObject ? minorV1CompactionThreshold : 0));

        for (List<S3ObjectMetadata> objectGroup : objectGroups) {
            if (!checkObjectGroupCouldBeCompact(objectGroup, startOffset, compactionType)) {
                continue;
            }
            TimerUtil start = new TimerUtil();
            long objectId = objectManager.prepareObject(1, TimeUnit.MINUTES.toMillis(60)).get();
            Optional<CompactStreamObjectRequest> requestOpt;
            if (MINOR.equals(compactionType) || MAJOR.equals(compactionType) || MINOR_V1.equals(compactionType)) {
                requestOpt = new CompactByPhysicalMerge(streamId, stream.streamEpoch(),
                    startOffset, objectGroup, objectId, dataBlockGroupSizeThreshold, objectStorage).compact();
            } else {
                requestOpt = new CompactByCompositeObject(streamId, stream.streamEpoch(), startOffset, objectGroup,
                    objectId, objectStorage).compact();
            }
            if (requestOpt.isEmpty()) {
                continue;
            }
            request = requestOpt.get();
            objectManager.compactStreamObject(request).get();
            s3ObjectLogger.info("Compact stream finished, {} {} cost {}ms", compactionType, request, start.elapsedAs(TimeUnit.MILLISECONDS));
        }
    }

    long getMaxGroupSize(CompactionType compactionType) {
        switch (compactionType) {
            case MINOR:
                return MINOR_COMPACTION_SIZE_THRESHOLD;
            case MAJOR:
                return this.maxStreamObjectSize;
            case CLEANUP_V1:
                return this.maxStreamObjectSize;
            case MINOR_V1:
                return this.minorV1CompactionThreshold;
            case MAJOR_V1:
                return this.maxStreamObjectSize;
            default:
                throw new IllegalArgumentException("Unsupported compaction type: " + compactionType);
        }
    }

    static boolean checkObjectGroupCouldBeCompact(List<S3ObjectMetadata> objectGroup, long startOffset,
        CompactionType compactionType) {
        if (objectGroup.size() == 1 && SKIP_COMPACTION_TYPE_WHEN_ONE_OBJECT_IN_GROUP.contains(compactionType)) {
            return false;
        }
        if (objectGroup.stream().anyMatch(o -> o.bucket() == LocalFileObjectStorage.BUCKET_ID)) {
            return false;
        }
        if (CLEANUP_V1.equals(compactionType)) {
            S3ObjectMetadata metadata = objectGroup.get(0);
            if (ObjectAttributes.from(metadata.attributes()).type() != Composite) {
                return false;
            }
            double dirtySize = ((double) startOffset - metadata.startOffset()) / (metadata.endOffset() - metadata.startOffset()) * metadata.objectSize();
            return dirtySize > MAX_DIRTY_BYTES;
        }
        return true;
    }

    private void cleanupExpiredObject(
        List<S3ObjectMetadata> expiredObjects) throws ExecutionException, InterruptedException {
        if (expiredObjects.isEmpty()) {
            return;
        }
        List<Long> compactedObjectIds = expiredObjects.stream().map(S3ObjectMetadata::objectId).collect(Collectors.toList());
        int expiredObjectCount = compactedObjectIds.size();
        // limit the expired objects compaction step to EXPIRED_OBJECTS_CLEAN_UP_STEP
        for (int i = 0; i < expiredObjectCount; ) {
            int start = i;
            int end = Math.min(i + EXPIRED_OBJECTS_CLEAN_UP_STEP, expiredObjectCount);
            List<Long> subCompactedObjectIds = new ArrayList<>(compactedObjectIds.subList(start, end));
            List<CompactOperations> operations = subCompactedObjectIds.stream().map(id -> CompactOperations.DEEP_DELETE).collect(Collectors.toList());
            request = new CompactStreamObjectRequest(ObjectUtils.NOOP_OBJECT_ID, 0,
                stream.streamId(), stream.streamEpoch(), NOOP_OFFSET, NOOP_OFFSET, subCompactedObjectIds, operations, ObjectAttributes.DEFAULT.attributes());
            objectManager.compactStreamObject(request).get();
            if (s3ObjectLogger.isTraceEnabled()) {
                s3ObjectLogger.trace("{}", request);
            }
            i = end;
        }
    }

    static class CompactByPhysicalMerge {
        private final List<S3ObjectMetadata> objectGroup;
        private final long streamId;
        private final long streamEpoch;
        private final long startOffset;
        // compact object group to the new object
        private final long objectId;
        private final ObjectStorage objectStorage;
        private final int dataBlockGroupSizeThreshold;

        public CompactByPhysicalMerge(long streamId, long streamEpoch, long startOffset,
            List<S3ObjectMetadata> objectGroup,
            long objectId, int dataBlockGroupSizeThreshold, ObjectStorage objectStorage) {
            this.streamId = streamId;
            this.streamEpoch = streamEpoch;
            this.startOffset = startOffset;
            this.objectGroup = objectGroup;
            this.objectId = objectId;
            this.dataBlockGroupSizeThreshold = dataBlockGroupSizeThreshold;
            this.objectStorage = objectStorage;
        }

        public Optional<CompactStreamObjectRequest> compact() throws ExecutionException, InterruptedException {
            long nextBlockPosition = 0;
            long objectSize = 0;
            long compactedStartOffset = objectGroup.get(0).startOffset();
            long compactedEndOffset = objectGroup.get(objectGroup.size() - 1).endOffset();
            List<Long> compactedObjectIds = new LinkedList<>();
            CompositeByteBuf indexes = ByteBufAlloc.compositeByteBuffer();
            Writer writer = objectStorage.writer(
                new WriteOptions().allocType(STREAM_OBJECT_COMPACTION_READ).throttleStrategy(ThrottleStrategy.COMPACTION),
                ObjectUtils.genKey(0, objectId));
            long groupStartOffset = -1L;
            long groupStartPosition = -1L;
            int groupSize = 0;
            int groupRecordCount = 0;
            DataBlockIndex lastIndex = null;
            for (S3ObjectMetadata object : objectGroup) {
                try (ObjectReader reader = ObjectReader.reader(object, objectStorage)) {
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
            List<CompactOperations> operations = compactedObjectIds.stream().map(id -> CompactOperations.DELETE).collect(Collectors.toList());
            return Optional.of(new CompactStreamObjectRequest(objectId, objectSize, streamId, streamEpoch,
                compactedStartOffset, compactedEndOffset, compactedObjectIds, operations, ObjectAttributes.builder().bucket(writer.bucketId()).build().attributes()));
        }
    }

    static class CompactByCompositeObject {
        private final List<S3ObjectMetadata> objectGroup;
        private final long streamId;
        private final long streamEpoch;
        private final long startOffset;
        // compact object group to the new object
        private final long objectId;
        private final ObjectStorage objectStorage;
        private final List<Long> compactedObjectIds;
        private final List<CompactOperations> operations;

        public CompactByCompositeObject(long streamId, long streamEpoch, long startOffset,
            List<S3ObjectMetadata> objectGroup, long objectId, ObjectStorage objectStorage) {
            this.streamId = streamId;
            this.streamEpoch = streamEpoch;
            this.startOffset = startOffset;
            this.objectGroup = objectGroup;
            this.objectId = objectId;
            this.objectStorage = objectStorage;
            this.compactedObjectIds = new LinkedList<>();
            this.operations = new ArrayList<>(objectGroup.size());
        }

        public Optional<CompactStreamObjectRequest> compact() throws ExecutionException, InterruptedException {
            CompositeObjectWriter objectWriter = CompositeObject.writer(objectStorage.writer(new WriteOptions(), ObjectUtils.genKey(0, objectId)));
            List<ObjectReader> readers = new ArrayList<>();
            objectGroup.stream().map(object -> ObjectReader.reader(object, objectStorage)).forEach(reader -> {
                // warm up
                reader.basicObjectInfo();
                readers.add(reader);
            });
            try {
                for (int i = 0; i < objectGroup.size(); i++) {
                    S3ObjectMetadata objectMetadata = objectGroup.get(i);
                    ObjectAttributes attributes = ObjectAttributes.from(objectMetadata.attributes());
                    ObjectReader objectReader = readers.get(i);
                    if (attributes.type() == Composite) {
                        compactCompositeObject(objectMetadata, objectReader, objectWriter);
                    } else {
                        compactNormalObject(objectMetadata, objectReader, objectWriter);
                    }
                }
                List<ObjectStreamRange> ranges = objectWriter.getStreamRanges();
                if (ranges.isEmpty()) {
                    // All data blocks are expired
                    compactedObjectIds.add(objectId);
                    operations.add(CompactOperations.DELETE);
                    return Optional.of(new CompactStreamObjectRequest(NOOP_OBJECT_ID, 0, streamId, streamEpoch,
                        NOOP_OFFSET, NOOP_OFFSET, compactedObjectIds, operations, ObjectAttributes.DEFAULT.attributes()));
                } else {
                    objectWriter.close().get();
                    int attributes = ObjectAttributes.builder().bucket(objectWriter.bucketId()).type(Composite).build().attributes();
                    ObjectStreamRange range = ranges.get(0);
                    return Optional.of(new CompactStreamObjectRequest(objectId, objectWriter.size(), streamId, streamEpoch,
                        range.getStartOffset(), range.getEndOffset(), compactedObjectIds, operations, attributes));
                }
            } finally {
                readers.forEach(ObjectReader::close);
            }

        }

        private void compactNormalObject(S3ObjectMetadata objectMetadata, ObjectReader objectReader,
            ObjectWriter objectWriter) throws ExecutionException, InterruptedException {
            objectWriter.addComponent(objectMetadata, objectReader.basicObjectInfo().get().indexBlock().indexes());
            // keep the data for the linked object
            compactedObjectIds.add(objectMetadata.objectId());
            operations.add(CompactOperations.KEEP_DATA);
        }

        private void compactCompositeObject(S3ObjectMetadata objectMetadata, ObjectReader objectReader,
            ObjectWriter objectWriter) throws ExecutionException, InterruptedException {
            BasicObjectInfoExt info = (BasicObjectInfoExt) objectReader.basicObjectInfo().get();
            List<ObjectIndex> linkedObjectIndexes = info.objectsBlock().indexes();
            List<DataBlockIndex> dataBlockIndexes = info.indexBlock().indexes();

            List<ObjectPath> needDeleteObject = new ArrayList<>();

            for (ObjectIndex linkedObjectIndex : linkedObjectIndexes) {
                boolean hasLiveBlocks = false;
                S3ObjectMetadata linkedObjectMetadata = new S3ObjectMetadata(linkedObjectIndex.objectId(), ObjectAttributes.builder().bucket(linkedObjectIndex.bucketId()).build().attributes());
                for (int j = linkedObjectIndex.blockStartIndex(); j < linkedObjectIndex.blockEndIndex(); j++) {
                    DataBlockIndex dataBlockIndex = dataBlockIndexes.get(j);
                    if (dataBlockIndex.endOffset() <= startOffset) {
                        continue;
                    }
                    hasLiveBlocks = true;
                    break;
                }
                if (!hasLiveBlocks) {
                    // The linked object is fully expired, and there won't be any access to it.
                    // So we could directly delete the object from object storage.
                    needDeleteObject.add(new ObjectPath(linkedObjectMetadata.bucket(), linkedObjectMetadata.key()));
                } else {
                    // Keep all blocks in the linked object even part of them are expired.
                    // So we could get more precise composite object retained size.
                    objectWriter.addComponent(
                        linkedObjectMetadata,
                        dataBlockIndexes.subList(linkedObjectIndex.blockStartIndex(), linkedObjectIndex.blockEndIndex())
                    );
                    // The linked object's metadata is already deleted from KRaft after the first time become a part of composite object.
                }
            }

            if (!needDeleteObject.isEmpty()) {
                objectStorage.delete(needDeleteObject).get();
                needDeleteObject.clear();
            }

            // delete the old composite object
            compactedObjectIds.add(objectMetadata.objectId());
            operations.add(CompactOperations.DELETE);
        }
    }

    static List<List<S3ObjectMetadata>> group0(List<S3ObjectMetadata> objects,
                                               long maxStreamObjectSize,
                                               Predicate<S3ObjectMetadata> objectFilter) {
        // TODO: switch to include/exclude composite object
        List<List<S3ObjectMetadata>> objectGroups = new LinkedList<>();
        long groupSize = 0;
        long groupNextOffset = -1L;
        List<S3ObjectMetadata> group = new LinkedList<>();
        int partCount = 0;
        for (S3ObjectMetadata object : objects) {
            if (!objectFilter.test(object)) {
                continue;
            }

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
        private ObjectStorage objectStorage;
        private Stream stream;
        private long maxStreamObjectSize;
        private int dataBlockGroupSizeThreshold = DEFAULT_DATA_BLOCK_GROUP_SIZE_THRESHOLD;
        private long minorV1CompactionThreshold = MINOR_V1_COMPACTION_SIZE_THRESHOLD;
        private boolean majorV1CompactionSkipSmallObject = false;

        public Builder objectManager(ObjectManager objectManager) {
            this.objectManager = objectManager;
            return this;
        }

        public Builder objectStorage(ObjectStorage objectStorage) {
            this.objectStorage = objectStorage;
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

        public Builder minorV1CompactionThreshold(long minorV1CompactionThreshold) {
            this.minorV1CompactionThreshold = minorV1CompactionThreshold;
            return this;
        }

        public Builder majorV1CompactionSkipSmallObject(boolean majorV1CompactionSkipSmallObject) {
            this.majorV1CompactionSkipSmallObject = majorV1CompactionSkipSmallObject;
            return this;
        }

        public StreamObjectCompactor build() {
            return new StreamObjectCompactor(objectManager, objectStorage, stream, maxStreamObjectSize, dataBlockGroupSizeThreshold, minorV1CompactionThreshold, majorV1CompactionSkipSmallObject);
        }
    }

    public enum CompactionType {
        // cleanup: remove the expired objects.
        CLEANUP,
        // minor: 1. CLEANUP; 2. physical merge the object to a large object with max group size MINOR_COMPACTION_SIZE_THRESHOLD
        MINOR,
        // major: 1. CLEANUP; 2. physical merge the object to a large object with max group size maxStreamObjectSize
        MAJOR,
        // cleanup v1: 1. CLEANUP; 2. cleanup the composite object which dirty data exceed MAX_DIRTY_BYTES
        CLEANUP_V1,
        // minor v1: 1. CLEANUP; 2. physical merge the small object to a large object with max group size MINOR_V1_COMPACTION_SIZE_THRESHOLD
        MINOR_V1,
        // major v1: 1. CLEANUP; 2. use composite object logic merge the small object to a large object with max group size maxStreamObjectSize
        MAJOR_V1
    }
}
