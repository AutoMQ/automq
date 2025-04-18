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
