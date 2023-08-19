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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.controller.stream.RangeMetadata;
import org.apache.kafka.controller.stream.s3.StreamObject;

public class StreamMetadataDelta {
    private final StreamMetadataImage image;

    private Integer newEpoch;

    private final Map<Integer/*rangeIndex*/, RangeMetadata> changedRanges = new HashMap<>();
    private final Set<Integer/*rangeIndex*/> removedRanges = new HashSet<>();
    private final Set<StreamObject> changedStreamObjects = new HashSet<>();
    private final Set<StreamObject> removedStreamObjects = new HashSet<>();

    public StreamMetadataDelta(StreamMetadataImage image) {
        this.image = image;
        this.newEpoch = image.getEpoch();
    }

    public StreamMetadataImage apply() {
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

        Set<StreamObject> newStreamObjects = new HashSet<>(image.getStreams());
        // remove all removed stream-objects
        newStreamObjects.removeAll(removedStreamObjects);
        // add all changed stream-objects
        newStreamObjects.addAll(changedStreamObjects);
        return new StreamMetadataImage(image.getStreamId(), newEpoch, image.getStartOffset(), newRanges, newStreamObjects);
    }

}
