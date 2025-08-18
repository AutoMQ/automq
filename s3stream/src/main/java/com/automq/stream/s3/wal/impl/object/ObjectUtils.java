/*
 * Copyright 2025, AutoMQ HK Limited.
 *
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

package com.automq.stream.s3.wal.impl.object;

import com.automq.stream.s3.Constants;
import com.automq.stream.s3.StreamRecordBatchCodec;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.wal.common.RecordHeader;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;

import io.netty.buffer.ByteBuf;

import static com.automq.stream.s3.wal.common.RecordHeader.RECORD_HEADER_SIZE;

public class ObjectUtils {
    public static final long DATA_FILE_ALIGN_SIZE = 64L * 1024 * 1024; // 64MiB
    private static final Logger LOGGER = LoggerFactory.getLogger(ObjectUtils.class);
    public static final String OBJECT_PATH_OFFSET_DELIMITER = "-";

    public static List<WALObject> parse(List<ObjectStorage.ObjectInfo> objects) {
        List<WALObject> walObjects = new ArrayList<>(objects.size());
        for (ObjectStorage.ObjectInfo object : objects) {
            // v0: md5hex(nodeId)/clusterId/nodeId/epoch/wal/startOffset
            // v1: md5hex(nodeId)/clusterId/nodeId/epoch/wal/startOffset_endOffset
            String path = object.key();
            String[] parts = path.split("/");
            try {
                long epoch = Long.parseLong(parts[parts.length - 3]);
                long length = object.size();
                String rawOffset = parts[parts.length - 1];

                WALObject walObject;
                if (rawOffset.contains(OBJECT_PATH_OFFSET_DELIMITER)) {
                    // v1 format: {startOffset}-{endOffset}
                    long startOffset = Long.parseLong(rawOffset.substring(0, rawOffset.indexOf(OBJECT_PATH_OFFSET_DELIMITER)));
                    long endOffset = Long.parseLong(rawOffset.substring(rawOffset.indexOf(OBJECT_PATH_OFFSET_DELIMITER) + 1));
                    walObject = new WALObject(object.bucketId(), path, epoch, startOffset, endOffset, length);
                } else {
                    // v0 format: {startOffset}
                    long startOffset = Long.parseLong(rawOffset);
                    walObject = new WALObject(object.bucketId(), path, epoch, startOffset, length);
                }
                walObjects.add(walObject);
            } catch (NumberFormatException e) {
                // Ignore invalid path
                LOGGER.warn("Invalid WAL object: {}", path);
            }
        }
        walObjects.sort(Comparator.comparingLong(WALObject::epoch).thenComparingLong(WALObject::startOffset));
        return walObjects;
    }

    /**
     * Remove overlap objects.
     *
     * @return overlap objects.
     */
    public static List<WALObject> skipOverlapObjects(List<WALObject> objects) {
        List<WALObject> overlapObjects = new ArrayList<>();
        WALObject lastObject = null;
        for (WALObject object : objects) {
            if (lastObject == null) {
                lastObject = object;
                continue;
            }
            if (lastObject.epoch() != object.epoch()) {
                if (lastObject.endOffset() > object.startOffset()) {
                    // maybe the old epoch node write dirty object after it was fenced.
                    overlapObjects.add(lastObject);
                }
            }
        }
        overlapObjects.forEach(objects::remove);
        return overlapObjects;
    }

    public static String nodePrefix(String clusterId, int nodeId, String type) {
        return DigestUtils.md5Hex(String.valueOf(nodeId)).toUpperCase(Locale.ROOT) + "/" + Constants.DEFAULT_NAMESPACE + clusterId + "/" + nodeId + (StringUtils.isBlank(type) ? "" : ("_" + type)) + "/";
    }

    public static String nodePrefix(String clusterId, int nodeId) {
        return nodePrefix(clusterId, nodeId, null);
    }

    public static String genObjectPathV0(String nodePrefix, long epoch, long objectStartOffset) {
        return nodePrefix + epoch + "/wal/" + objectStartOffset;
    }

    public static String genObjectPathV1(String nodePrefix, long epoch, long objectStartOffset, long objectEndOffset) {
        return nodePrefix + epoch + "/wal/" + objectStartOffset + OBJECT_PATH_OFFSET_DELIMITER + objectEndOffset;
    }

    public static String genObjectPathV1(String nodePrefix, long epoch, long objectStartOffset) {
        long endOffset = objectStartOffset + DATA_FILE_ALIGN_SIZE;
        return nodePrefix + epoch + "/wal/" + objectStartOffset + OBJECT_PATH_OFFSET_DELIMITER + endOffset;
    }

    public static long floorAlignOffset(long offset) {
        return offset / DATA_FILE_ALIGN_SIZE * DATA_FILE_ALIGN_SIZE;
    }

    public static long ceilAlignOffset(long offset) {
        return (offset + DATA_FILE_ALIGN_SIZE - 1) / DATA_FILE_ALIGN_SIZE * DATA_FILE_ALIGN_SIZE;
    }

    public static StreamRecordBatch decodeRecordBuf(ByteBuf dataBuffer) {
        // TODO: thread local to avoid alloc
        ByteBuf recordHeaderBuf = dataBuffer.readBytes(RECORD_HEADER_SIZE);
        RecordHeader header = new RecordHeader(recordHeaderBuf);
        recordHeaderBuf.release();
        if (header.getMagicCode() != RecordHeader.RECORD_HEADER_DATA_MAGIC_CODE) {
            throw new IllegalStateException("Invalid magic code in record header.");
        }
        int length = header.getRecordBodyLength();
        StreamRecordBatch streamRecordBatch = StreamRecordBatchCodec.sliceRetainDecode(dataBuffer.slice(dataBuffer.readerIndex(), length));
        dataBuffer.skipBytes(length);
        return streamRecordBatch;
    }
}
