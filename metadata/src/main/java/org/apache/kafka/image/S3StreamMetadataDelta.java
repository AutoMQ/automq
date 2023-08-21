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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.metadata.RangeRecord;
import org.apache.kafka.common.metadata.RemoveRangeRecord;
import org.apache.kafka.common.metadata.RemoveS3StreamObjectRecord;
import org.apache.kafka.common.metadata.S3StreamObjectRecord;
import org.apache.kafka.metadata.stream.RangeMetadata;
import org.apache.kafka.metadata.stream.S3StreamObject;

public class S3StreamMetadataDelta {
    private final S3StreamMetadataImage image;

    private Long newEpoch;

    private final Map<Integer/*rangeIndex*/, RangeMetadata> changedRanges = new HashMap<>();
    private final Set<Integer/*rangeIndex*/> removedRanges = new HashSet<>();
    private final Set<S3StreamObject> changedS3StreamObjects = new HashSet<>();
    private final Set<S3StreamObject> removedS3StreamObjects = new HashSet<>();

    public S3StreamMetadataDelta(S3StreamMetadataImage image) {
        this.image = image;
        this.newEpoch = image.getEpoch();
    }

    public void replay(RangeRecord record) {
        changedRanges.put(record.rangeIndex(), RangeMetadata.of(record));
    }

    public void replay(RemoveRangeRecord record) {
        removedRanges.add(record.rangeIndex());
    }

    public void replay(S3StreamObjectRecord record) {
        changedS3StreamObjects.add(S3StreamObject.of(record));
    }

    public void replay(RemoveS3StreamObjectRecord record) {
        removedS3StreamObjects.add(new S3StreamObject(record.objectId()));
    }

    public S3StreamMetadataImage apply() {
        Map<Integer, RangeMetadata> newRanges = new HashMap<>(image.getRanges().size());
        // apply the delta changes of old ranges since the last image
        image.getRanges().forEach((rangeIndex, range) -> {
            RangeMetadata changedRange = changedRanges.get(rangeIndex);
            if (changedRange == null) {
                // no change, check if deleted
                if (!removedRanges.contains(rangeIndex)) {
                    newRanges.put(rangeIndex, range);
                }
            } else {
                // changed, apply the delta
                newRanges.put(rangeIndex, changedRange);
            }
        });
        // apply the new created ranges
        changedRanges.entrySet().stream().filter(entry -> !newRanges.containsKey(entry.getKey()))
            .forEach(entry -> newRanges.put(entry.getKey(), entry.getValue()));

        List<S3StreamObject> newS3StreamObjects = new ArrayList<>(image.getStreamObjects());
        // remove all removed stream-objects
        newS3StreamObjects.removeAll(removedS3StreamObjects);
        // add all changed stream-objects
        newS3StreamObjects.addAll(changedS3StreamObjects);
        return new S3StreamMetadataImage(image.getStreamId(), newEpoch, image.getStartOffset(), newRanges, newS3StreamObjects);
    }

}
