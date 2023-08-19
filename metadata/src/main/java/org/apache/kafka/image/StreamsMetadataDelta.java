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

public final class StreamsMetadataDelta {

    private final StreamsMetadataImage image;

    private final Map<Long, StreamMetadataDelta> changedStreams = new HashMap<>();

    private final Map<Integer, BrokerStreamMetadataDelta> changedBrokers = new HashMap<>();

    private final Set<Long> deletedStreams = new HashSet<>();
    private final Set<Long> deletedBrokers = new HashSet<>();

    public StreamsMetadataDelta(StreamsMetadataImage image) {
        this.image = image;
    }

    StreamsMetadataImage apply() {
        Map<Long, StreamMetadataImage> newStreams = new HashMap<>(image.getStreamsMetadata().size());
        Map<Integer, BrokerStreamMetadataImage> newBrokerStreams = new HashMap<>(image.getBrokerStreamsMetadata().size());
        // apply the delta changes of old streams since the last image
        image.getStreamsMetadata().forEach((streamId, streamMetadataImage) -> {
            StreamMetadataDelta delta = changedStreams.get(streamId);
            if (delta == null) {
                // no change, check if deleted
                if (!deletedStreams.contains(streamId)) {
                    newStreams.put(streamId, streamMetadataImage);
                }
            } else {
                // changed, apply the delta
                StreamMetadataImage newStreamMetadataImage = delta.apply();
                newStreams.put(streamId, newStreamMetadataImage);
            }
        });
        // apply the new created streams
        changedStreams.entrySet().stream().filter(entry -> !newStreams.containsKey(entry.getKey()))
            .forEach(entry -> {
                StreamMetadataImage newStreamMetadataImage = entry.getValue().apply();
                newStreams.put(entry.getKey(), newStreamMetadataImage);
            });

        // apply the delta changes of old brokers since the last image
        image.getBrokerStreamsMetadata().forEach((brokerId, brokerStreamMetadataImage) -> {
            BrokerStreamMetadataDelta delta = changedBrokers.get(brokerId);
            if (delta == null) {
                // no change, check if deleted
                if (!deletedBrokers.contains(brokerId)) {
                    newBrokerStreams.put(brokerId, brokerStreamMetadataImage);
                }
            } else {
                // changed, apply the delta
                BrokerStreamMetadataImage newBrokerStreamMetadataImage = delta.apply();
                newBrokerStreams.put(brokerId, newBrokerStreamMetadataImage);
            }
        });
        // apply the new created streams
        changedBrokers.entrySet().stream().filter(entry -> !newBrokerStreams.containsKey(entry.getKey()))
            .forEach(entry -> {
                BrokerStreamMetadataImage newBrokerStreamMetadataImage = entry.getValue().apply();
                newBrokerStreams.put(entry.getKey(), newBrokerStreamMetadataImage);
            });

        return new StreamsMetadataImage(newStreams, newBrokerStreams);
    }

}
