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
import org.apache.kafka.common.metadata.RangeRecord;
import org.apache.kafka.common.metadata.RemoveRangeRecord;
import org.apache.kafka.common.metadata.RemoveS3StreamObjectRecord;
import org.apache.kafka.common.metadata.RemoveS3StreamRecord;
import org.apache.kafka.common.metadata.RemoveWALObjectRecord;
import org.apache.kafka.common.metadata.S3StreamRecord;
import org.apache.kafka.common.metadata.S3StreamObjectRecord;
import org.apache.kafka.common.metadata.WALObjectRecord;

public final class S3StreamsMetadataDelta {

    private final S3StreamsMetadataImage image;

    private final Map<Long, S3StreamMetadataDelta> changedStreams = new HashMap<>();

    private final Map<Integer, BrokerS3WALMetadataDelta> changedBrokers = new HashMap<>();

    private final Set<Long> deletedStreams = new HashSet<>();
    // TODO: when we recycle the broker's memory data structure
    // We don't use pair of specify BrokerCreateRecord and BrokerRemoveRecord to create or remove brokers, and
    // we create BrokerStreamMetadataImage when we create the first WALObjectRecord for a broker,
    // so we should decide when to recycle the broker's memory data structure
    private final Set<Long> deletedBrokers = new HashSet<>();

    public S3StreamsMetadataDelta(S3StreamsMetadataImage image) {
        this.image = image;
    }

    public void replay(S3StreamRecord record) {
        S3StreamMetadataDelta delta;
        if (!image.getStreamsMetadata().containsKey(record.streamId())) {
            // create a new StreamMetadata with empty ranges and streams if not exist
            delta = new S3StreamMetadataDelta(
                new S3StreamMetadataImage(record.streamId(), record.epoch(), record.startOffset(), Collections.emptyMap(), Collections.emptyList()));
        } else {
            // update the epoch if exist
            S3StreamMetadataImage s3StreamMetadataImage = image.getStreamsMetadata().get(record.streamId());
            delta = new S3StreamMetadataDelta(
                new S3StreamMetadataImage(record.streamId(), record.epoch(), record.startOffset(), s3StreamMetadataImage.getRanges(),
                    s3StreamMetadataImage.getStreamObjects()));
        }
        // add the delta to the changedStreams
        changedStreams.put(record.streamId(), delta);
    }

    public void replay(RemoveS3StreamRecord record) {
        // add the streamId to the deletedStreams
        deletedStreams.add(record.streamId());
    }

    public void replay(RangeRecord record) {
        getOrCreateStreamMetadataDelta(record.streamId()).replay(record);
    }

    public void replay(RemoveRangeRecord record) {
        getOrCreateStreamMetadataDelta(record.streamId()).replay(record);
    }

    public void replay(S3StreamObjectRecord record) {
        getOrCreateStreamMetadataDelta(record.streamId()).replay(record);
    }

    public void replay(RemoveS3StreamObjectRecord record) {
        getOrCreateStreamMetadataDelta(record.streamId()).replay(record);
    }

    public void replay(WALObjectRecord record) {
        getOrCreateBrokerStreamMetadataDelta(record.brokerId()).replay(record);
    }

    public void replay(RemoveWALObjectRecord record) {
        getOrCreateBrokerStreamMetadataDelta(record.brokerId()).replay(record);
    }

    private S3StreamMetadataDelta getOrCreateStreamMetadataDelta(Long streamId) {
        S3StreamMetadataDelta delta = changedStreams.get(streamId);
        if (delta == null) {
            delta = new S3StreamMetadataDelta(image.getStreamsMetadata().get(streamId));
            changedStreams.put(streamId, delta);
        }
        return delta;
    }

    private BrokerS3WALMetadataDelta getOrCreateBrokerStreamMetadataDelta(Integer brokerId) {
        BrokerS3WALMetadataDelta delta = changedBrokers.get(brokerId);
        if (delta == null) {
            delta = new BrokerS3WALMetadataDelta(
                image.getBrokerWALMetadata().
                    getOrDefault(brokerId, new BrokerS3WALMetadataImage(brokerId, Collections.emptyList())));
            changedBrokers.put(brokerId, delta);
        }
        return delta;
    }

    S3StreamsMetadataImage apply() {
        Map<Long, S3StreamMetadataImage> newStreams = new HashMap<>(image.getStreamsMetadata().size());
        Map<Integer, BrokerS3WALMetadataImage> newBrokerStreams = new HashMap<>(image.getBrokerWALMetadata().size());
        // apply the delta changes of old streams since the last image
        image.getStreamsMetadata().forEach((streamId, streamMetadataImage) -> {
            S3StreamMetadataDelta delta = changedStreams.get(streamId);
            if (delta == null) {
                // no change, check if deleted
                if (!deletedStreams.contains(streamId)) {
                    newStreams.put(streamId, streamMetadataImage);
                }
            } else {
                // changed, apply the delta
                S3StreamMetadataImage newS3StreamMetadataImage = delta.apply();
                newStreams.put(streamId, newS3StreamMetadataImage);
            }
        });
        // apply the new created streams
        changedStreams.entrySet().stream().filter(entry -> !newStreams.containsKey(entry.getKey()))
            .forEach(entry -> {
                S3StreamMetadataImage newS3StreamMetadataImage = entry.getValue().apply();
                newStreams.put(entry.getKey(), newS3StreamMetadataImage);
            });

        // apply the delta changes of old brokers since the last image
        image.getBrokerWALMetadata().forEach((brokerId, brokerStreamMetadataImage) -> {
            BrokerS3WALMetadataDelta delta = changedBrokers.get(brokerId);
            if (delta == null) {
                // no change, check if deleted
                if (!deletedBrokers.contains(brokerId)) {
                    newBrokerStreams.put(brokerId, brokerStreamMetadataImage);
                }
            } else {
                // changed, apply the delta
                BrokerS3WALMetadataImage newBrokerS3WALMetadataImage = delta.apply();
                newBrokerStreams.put(brokerId, newBrokerS3WALMetadataImage);
            }
        });
        // apply the new created streams
        changedBrokers.entrySet().stream().filter(entry -> !newBrokerStreams.containsKey(entry.getKey()))
            .forEach(entry -> {
                BrokerS3WALMetadataImage newBrokerS3WALMetadataImage = entry.getValue().apply();
                newBrokerStreams.put(entry.getKey(), newBrokerS3WALMetadataImage);
            });

        return new S3StreamsMetadataImage(newStreams, newBrokerStreams);
    }

}
