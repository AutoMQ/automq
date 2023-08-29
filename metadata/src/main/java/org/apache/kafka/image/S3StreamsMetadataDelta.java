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
import org.apache.kafka.common.metadata.AdvanceRangeRecord;
import org.apache.kafka.common.metadata.AssignedStreamIdRecord;
import org.apache.kafka.common.metadata.BrokerWALMetadataRecord;
import org.apache.kafka.common.metadata.RangeRecord;
import org.apache.kafka.common.metadata.RemoveBrokerWALMetadataRecord;
import org.apache.kafka.common.metadata.RemoveRangeRecord;
import org.apache.kafka.common.metadata.RemoveS3StreamObjectRecord;
import org.apache.kafka.common.metadata.RemoveS3StreamRecord;
import org.apache.kafka.common.metadata.RemoveWALObjectRecord;
import org.apache.kafka.common.metadata.S3StreamRecord;
import org.apache.kafka.common.metadata.S3StreamObjectRecord;
import org.apache.kafka.common.metadata.WALObjectRecord;

public final class S3StreamsMetadataDelta {

    private final S3StreamsMetadataImage image;

    private long currentAssignedStreamId;

    private final Map<Long, S3StreamMetadataDelta> changedStreams = new HashMap<>();

    private final Map<Integer, BrokerS3WALMetadataDelta> changedBrokers = new HashMap<>();

    private final Set<Long> deletedStreams = new HashSet<>();
    // TODO: when we recycle the broker's memory data structure
    // We don't use pair of specify BrokerCreateRecord and BrokerRemoveRecord to create or remove brokers, and
    // we create BrokerStreamMetadataImage when we create the first WALObjectRecord for a broker,
    // so we should decide when to recycle the broker's memory data structure
    private final Set<Integer> deletedBrokers = new HashSet<>();

    public S3StreamsMetadataDelta(S3StreamsMetadataImage image) {
        this.image = image;
        this.currentAssignedStreamId = image.nextAssignedStreamId() - 1;
    }

    public void replay(AssignedStreamIdRecord record) {
        this.currentAssignedStreamId = record.assignedStreamId();
    }

    public void replay(S3StreamRecord record) {
        getOrCreateStreamMetadataDelta(record.streamId()).replay(record);
        if (deletedStreams.contains(record.streamId())) {
            deletedStreams.remove(record.streamId());
        }
    }

    public void replay(RemoveS3StreamRecord record) {
        // add the streamId to the deletedStreams
        deletedStreams.add(record.streamId());
        changedStreams.remove(record.streamId());
    }

    public void replay(BrokerWALMetadataRecord record) {
        getOrCreateBrokerStreamMetadataDelta(record.brokerId()).replay(record);
        if (deletedBrokers.contains(record.brokerId())) {
            deletedBrokers.remove(record.brokerId());
        }
    }

    public void replay(RemoveBrokerWALMetadataRecord record) {
        // add the brokerId to the deletedBrokers
        deletedBrokers.add(record.brokerId());
        changedBrokers.remove(record.brokerId());
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
        record.streamsIndex().forEach(index -> {
            getOrCreateStreamMetadataDelta(index.streamId()).replay(new AdvanceRangeRecord()
                .setStartOffset(index.startOffset())
                .setEndOffset(index.endOffset()));
        });
    }

    public void replay(RemoveWALObjectRecord record) {
        getOrCreateBrokerStreamMetadataDelta(record.brokerId()).replay(record);
    }

    private S3StreamMetadataDelta getOrCreateStreamMetadataDelta(Long streamId) {
        S3StreamMetadataDelta delta = changedStreams.get(streamId);
        if (delta == null) {
            delta = new S3StreamMetadataDelta(image.streamsMetadata().getOrDefault(streamId, S3StreamMetadataImage.EMPTY));
            changedStreams.put(streamId, delta);
        }
        return delta;
    }

    private BrokerS3WALMetadataDelta getOrCreateBrokerStreamMetadataDelta(Integer brokerId) {
        BrokerS3WALMetadataDelta delta = changedBrokers.get(brokerId);
        if (delta == null) {
            delta = new BrokerS3WALMetadataDelta(
                image.brokerWALMetadata().
                    getOrDefault(brokerId, BrokerS3WALMetadataImage.EMPTY));
            changedBrokers.put(brokerId, delta);
        }
        return delta;
    }

    S3StreamsMetadataImage apply() {
        Map<Long, S3StreamMetadataImage> newStreams = new HashMap<>(image.streamsMetadata());
        Map<Integer, BrokerS3WALMetadataImage> newBrokerStreams = new HashMap<>(image.brokerWALMetadata());

        // apply the delta changes of old streams since the last image
        this.changedStreams.forEach((streamId, delta) -> {
            S3StreamMetadataImage newS3StreamMetadataImage = delta.apply();
            newStreams.put(streamId, newS3StreamMetadataImage);
        });
        // remove the deleted streams
        deletedStreams.forEach(newStreams::remove);

        // apply the delta changes of old brokers since the last image
        this.changedBrokers.forEach((brokerId, delta) -> {
            BrokerS3WALMetadataImage newBrokerS3WALMetadataImage = delta.apply();
            newBrokerStreams.put(brokerId, newBrokerS3WALMetadataImage);
        });
        // remove the deleted brokers
        deletedBrokers.forEach(newBrokerStreams::remove);

        return new S3StreamsMetadataImage(currentAssignedStreamId, newStreams, newBrokerStreams);
    }

}
