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

package org.apache.kafka.image;

import com.automq.stream.s3.metadata.StreamState;
import org.apache.kafka.common.metadata.RangeRecord;
import org.apache.kafka.common.metadata.RemoveRangeRecord;
import org.apache.kafka.common.metadata.RemoveS3StreamObjectRecord;
import org.apache.kafka.common.metadata.S3StreamObjectRecord;
import org.apache.kafka.common.metadata.S3StreamRecord;
import org.apache.kafka.common.metadata.S3StreamRecord.TagCollection;
import org.apache.kafka.metadata.stream.RangeMetadata;
import org.apache.kafka.metadata.stream.S3StreamObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

public class S3StreamMetadataDelta {

    private final S3StreamMetadataImage image;

    private long streamId;
    private long newStartOffset;
    private long newEpoch;
    private StreamState currentState;
    private TagCollection tags;

    private final Map<Integer/*rangeIndex*/, RangeMetadata> changedRanges = new HashMap<>();
    private final Set<Integer/*rangeIndex*/> removedRanges = new HashSet<>();
    private final Map<Long/*objectId*/, S3StreamObject> changedS3StreamObjects = new HashMap<>();
    private final Set<Long/*objectId*/> removedS3StreamObjectIds = new HashSet<>();

    public S3StreamMetadataDelta(S3StreamMetadataImage image) {
        this.image = image;
        this.newEpoch = image.getEpoch();
        this.streamId = image.getStreamId();
        this.newStartOffset = image.getStartOffset();
        this.tags = image.tags();
        this.currentState = image.state();
    }

    public void replay(S3StreamRecord record) {
        this.streamId = record.streamId();
        this.newEpoch = record.epoch();
        this.newStartOffset = record.startOffset();
        this.currentState = StreamState.fromByte(record.streamState());
        if (!record.tags().isEmpty()) {
            this.tags = record.tags();
        }
    }

    public void replay(RangeRecord record) {
        changedRanges.put(record.rangeIndex(), RangeMetadata.of(record));
        // new add or update, so remove from removedRanges
        removedRanges.remove(record.rangeIndex());
    }

    public void replay(RemoveRangeRecord record) {
        removedRanges.add(record.rangeIndex());
        // new remove, so remove from changedRanges
        changedRanges.remove(record.rangeIndex());
    }

    public void replay(S3StreamObjectRecord record) {
        changedS3StreamObjects.put(record.objectId(), S3StreamObject.of(record));
        // new add or update, so remove from removedObjects
        removedS3StreamObjectIds.remove(record.objectId());
    }

    public void replay(RemoveS3StreamObjectRecord record) {
        removedS3StreamObjectIds.add(record.objectId());
        // new remove, so remove from addedObjects
        changedS3StreamObjects.remove(record.objectId());
    }

    public S3StreamMetadataImage apply() {
        List<RangeMetadata> newRanges;
        if (changedRanges.isEmpty() && removedRanges.isEmpty()) {
            newRanges = image.getRanges();
        } else {
            NavigableMap<Integer, RangeMetadata> ranges = new TreeMap<>();
            image.getRanges().forEach(range -> ranges.put(range.rangeIndex(), range));
            // add all new changed ranges
            ranges.putAll(changedRanges);
            // remove all removed ranges
            removedRanges.forEach(ranges::remove);
            newRanges = new ArrayList<>(ranges.values());
        }

        DeltaMap<Long, S3StreamObject> newS3StreamObjects;
        if (changedS3StreamObjects.isEmpty() && removedS3StreamObjectIds.isEmpty()) {
            newS3StreamObjects = image.streamObjectsMap;
        } else {
            newS3StreamObjects = image.streamObjectsMap.copy();
            newS3StreamObjects.putAll(changedS3StreamObjects);
            newS3StreamObjects.removeAll(removedS3StreamObjectIds);
        }
        return new S3StreamMetadataImage(streamId, newEpoch, currentState, tags, newStartOffset, newRanges, newS3StreamObjects);
    }

}
