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

import com.automq.stream.s3.compact.objects.CompactedObjectBuilder;
import com.automq.stream.s3.compact.objects.CompactionType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class CompactionStats {
    private final CompactionStreamRecord streamRecord;
    private final Map<Long, Integer> s3ObjectToCompactedObjectNumMap;

    private CompactionStats(CompactionStreamRecord streamRecord, Map<Long, Integer> s3ObjectToCompactedObjectNumMap) {
        this.streamRecord = streamRecord;
        this.s3ObjectToCompactedObjectNumMap = s3ObjectToCompactedObjectNumMap;
    }

    public static CompactionStats of(List<CompactedObjectBuilder> compactedObjectBuilders) {
        int streamNumInStreamSet = 0;
        int streamObjectNum = 0;
        Map<Long, Integer> tmpObjectRecordMap = new HashMap<>();
        for (CompactedObjectBuilder compactedObjectBuilder : compactedObjectBuilders) {
            Set<Long> objectIdSet = compactedObjectBuilder.uniqueObjectIds();
            for (Long objectId : objectIdSet) {
                tmpObjectRecordMap.putIfAbsent(objectId, 0);
                tmpObjectRecordMap.put(objectId, tmpObjectRecordMap.get(objectId) + 1);
            }
            if (compactedObjectBuilder.type() == CompactionType.SPLIT && !compactedObjectBuilder.streamDataBlocks().isEmpty()) {
                streamObjectNum++;
            } else if (compactedObjectBuilder.type() == CompactionType.COMPACT) {
                streamNumInStreamSet += compactedObjectBuilder.totalStreamNum();
            }
        }
        return new CompactionStats(new CompactionStreamRecord(streamNumInStreamSet, streamObjectNum), tmpObjectRecordMap);
    }

    public CompactionStreamRecord getStreamRecord() {
        return streamRecord;
    }

    public Map<Long, Integer> getS3ObjectToCompactedObjectNumMap() {
        return s3ObjectToCompactedObjectNumMap;
    }

    public static final class CompactionStreamRecord {
        private final int streamNumInStreamSet;
        private final int streamObjectNum;

        public CompactionStreamRecord(int streamNumInStreamSet, int streamObjectNum) {
            this.streamNumInStreamSet = streamNumInStreamSet;
            this.streamObjectNum = streamObjectNum;
        }

        public int streamNumInStreamSet() {
            return streamNumInStreamSet;
        }

        public int streamObjectNum() {
            return streamObjectNum;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this)
                return true;
            if (obj == null || obj.getClass() != this.getClass())
                return false;
            var that = (CompactionStreamRecord) obj;
            return this.streamNumInStreamSet == that.streamNumInStreamSet &&
                this.streamObjectNum == that.streamObjectNum;
        }

        @Override
        public int hashCode() {
            return Objects.hash(streamNumInStreamSet, streamObjectNum);
        }

        @Override
        public String toString() {
            return "CompactionStreamRecord[" +
                "streamNumInStreamSet=" + streamNumInStreamSet + ", " +
                "streamObjectNum=" + streamObjectNum + ']';
        }

    }
}
