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
    private final Set<Long/*objectId*/> removedS3StreamObjectIds = new HashSet<>();

    public S3StreamMetadataDelta(S3StreamMetadataImage image) {
        this.image = image;
        this.newEpoch = image.getEpoch();
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
        changedS3StreamObjects.add(S3StreamObject.of(record));
        // new add or update, so remove from removedObjects
        removedS3StreamObjectIds.remove(record.objectId());
    }

    public void replay(RemoveS3StreamObjectRecord record) {
        removedS3StreamObjectIds.add(record.objectId());
        // new remove, so remove from addedObjects
        changedS3StreamObjects.remove(record.objectId());
    }

    public S3StreamMetadataImage apply() {
        Map<Integer, RangeMetadata> newRanges = new HashMap<>(image.getRanges());
        // add all new changed ranges
        newRanges.putAll(image.getRanges());
        // remove all removed ranges
        removedRanges.forEach(newRanges::remove);
        List<S3StreamObject> newS3StreamObjects = new ArrayList<>(image.getStreamObjects());
        // add all changed stream-objects
        newS3StreamObjects.addAll(changedS3StreamObjects);
        // remove all removed stream-objects
        newS3StreamObjects.removeIf(removedS3StreamObjectIds::contains);
        return new S3StreamMetadataImage(image.getStreamId(), newEpoch, image.getStartOffset(), newRanges, newS3StreamObjects);
    }

}
