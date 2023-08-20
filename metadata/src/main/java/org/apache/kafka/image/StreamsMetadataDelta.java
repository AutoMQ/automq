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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import javax.swing.Spring;
import org.apache.kafka.common.metadata.RangeRecord;
import org.apache.kafka.common.metadata.RemoveRangeRecord;
import org.apache.kafka.common.metadata.RemoveStreamObjectRecord;
import org.apache.kafka.common.metadata.RemoveStreamRecord;
import org.apache.kafka.common.metadata.RemoveWALObjectRecord;
import org.apache.kafka.common.metadata.StreamObjectRecord;
import org.apache.kafka.common.metadata.StreamRecord;
import org.apache.kafka.common.metadata.WALObjectRecord;

public final class StreamsMetadataDelta {

    private final StreamsMetadataImage image;

    private final Map<Long, StreamMetadataDelta> changedStreams = new HashMap<>();

    private final Map<Integer, BrokerStreamMetadataDelta> changedBrokers = new HashMap<>();

    private final Set<Long> deletedStreams = new HashSet<>();
    // TODO: when we recycle the broker's memory data structure
    // We don't use pair of specify BrokerCreateRecord and BrokerRemoveRecord to create or remove brokers, and
    // we create BrokerStreamMetadataImage when we create the first WALObjectRecord for a broker,
    // so we should decide when to recycle the broker's memory data structure
    private final Set<Long> deletedBrokers = new HashSet<>();

    public StreamsMetadataDelta(StreamsMetadataImage image) {
        this.image = image;
    }

    public void replay(StreamRecord record) {
        StreamMetadataDelta delta;
        if (!image.getStreamsMetadata().containsKey(record.streamId())) {
            // create a new StreamMetadata with empty ranges and streams if not exist
            delta = new StreamMetadataDelta(
                new StreamMetadataImage(record.streamId(), record.epoch(), record.startOffset(), Collections.emptyMap(), Collections.emptySet()));
        } else {
            // update the epoch if exist
            StreamMetadataImage streamMetadataImage = image.getStreamsMetadata().get(record.streamId());
            delta = new StreamMetadataDelta(
                new StreamMetadataImage(record.streamId(), record.epoch(), record.startOffset(), streamMetadataImage.getRanges(),
                    streamMetadataImage.getStreams()));
        }
        // add the delta to the changedStreams
        changedStreams.put(record.streamId(), delta);
    }

    public void replay(RemoveStreamRecord record) {
        // add the streamId to the deletedStreams
        deletedStreams.add(record.streamId());
    }

    public void replay(RangeRecord record) {
        getOrCreateStreamMetadataDelta(record.streamId()).replay(record);
    }

    public void replay(RemoveRangeRecord record) {
        getOrCreateStreamMetadataDelta(record.streamId()).replay(record);
    }

    public void replay(StreamObjectRecord record) {
        getOrCreateStreamMetadataDelta(record.streamId()).replay(record);
    }

    public void replay(RemoveStreamObjectRecord record) {
        getOrCreateStreamMetadataDelta(record.streamId()).replay(record);
    }

    public void replay(WALObjectRecord record) {
        getOrCreateBrokerStreamMetadataDelta(record.brokerId()).replay(record);
    }

    public void replay(RemoveWALObjectRecord record) {
        getOrCreateBrokerStreamMetadataDelta(record.brokerId()).replay(record);
    }

    private StreamMetadataDelta getOrCreateStreamMetadataDelta(Long streamId) {
        StreamMetadataDelta delta = changedStreams.get(streamId);
        if (delta == null) {
            delta = new StreamMetadataDelta(image.getStreamsMetadata().get(streamId));
            changedStreams.put(streamId, delta);
        }
        return delta;
    }

    private BrokerStreamMetadataDelta getOrCreateBrokerStreamMetadataDelta(Integer brokerId) {
        BrokerStreamMetadataDelta delta = changedBrokers.get(brokerId);
        if (delta == null) {
            delta = new BrokerStreamMetadataDelta(
                image.getBrokerStreamsMetadata().
                    getOrDefault(brokerId, new BrokerStreamMetadataImage(brokerId, Collections.emptySet())));
            changedBrokers.put(brokerId, delta);
        }
        return delta;
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
