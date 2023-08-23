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

package org.apache.kafka.controller.stream;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.message.CloseStreamRequestData;
import org.apache.kafka.common.message.CloseStreamResponseData;
import org.apache.kafka.common.message.CommitCompactObjectRequestData;
import org.apache.kafka.common.message.CommitCompactObjectResponseData;
import org.apache.kafka.common.message.CommitStreamObjectRequestData;
import org.apache.kafka.common.message.CommitStreamObjectResponseData;
import org.apache.kafka.common.message.CommitWALObjectRequestData;
import org.apache.kafka.common.message.CommitWALObjectResponseData;
import org.apache.kafka.common.message.CreateStreamRequestData;
import org.apache.kafka.common.message.CreateStreamResponseData;
import org.apache.kafka.common.message.DeleteStreamRequestData;
import org.apache.kafka.common.message.DeleteStreamResponseData;
import org.apache.kafka.common.message.OpenStreamRequestData;
import org.apache.kafka.common.message.OpenStreamResponseData;
import org.apache.kafka.common.metadata.RangeRecord;
import org.apache.kafka.common.metadata.RemoveRangeRecord;
import org.apache.kafka.common.metadata.RemoveS3StreamRecord;
import org.apache.kafka.common.metadata.S3StreamRecord;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.controller.ControllerResult;
import org.apache.kafka.metadata.stream.RangeMetadata;
import org.apache.kafka.metadata.stream.S3StreamObject;
import org.apache.kafka.metadata.stream.S3WALObject;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.apache.kafka.timeline.TimelineHashSet;
import org.slf4j.Logger;

/**
 * The StreamControlManager manages all Stream's lifecycle, such as create, open, delete, etc.
 */
public class StreamControlManager {

    public static class S3StreamMetadata {
        // current epoch, when created but not open, use 0 represent
        private long currentEpoch;
        // rangeIndex, when created but not open, there is no range, use -1 represent
        private int currentRangeIndex = -1;
        private long startOffset;
        private TimelineHashMap<Integer/*rangeIndex*/, RangeMetadata> ranges;
        private TimelineHashSet<S3StreamObject> streamObjects;

        public S3StreamMetadata(long currentEpoch, int currentRangeIndex, long startOffset,
            SnapshotRegistry registry) {
            this.currentEpoch = currentEpoch;
            this.currentRangeIndex = currentRangeIndex;
            this.startOffset = startOffset;
            this.ranges = new TimelineHashMap<>(registry, 0);
            this.streamObjects = new TimelineHashSet<>(registry, 0);
        }

        public long currentEpoch() {
            return currentEpoch;
        }

        public int currentRangeIndex() {
            return currentRangeIndex;
        }

        public long startOffset() {
            return startOffset;
        }

        public Map<Integer, RangeMetadata> ranges() {
            return ranges;
        }

        public Set<S3StreamObject> streamObjects() {
            return streamObjects;
        }

        @Override
        public String toString() {
            return "S3StreamMetadata{" +
                "currentEpoch=" + currentEpoch +
                ", currentRangeIndex=" + currentRangeIndex +
                ", startOffset=" + startOffset +
                ", ranges=" + ranges +
                ", streamObjects=" + streamObjects +
                '}';
        }
    }

    public static class BrokerS3WALMetadata {
        private int brokerId;
        private TimelineHashSet<S3WALObject> walObjects;

        public BrokerS3WALMetadata(int brokerId, SnapshotRegistry registry) {
            this.brokerId = brokerId;
            this.walObjects = new TimelineHashSet<>(registry, 0);
        }

        public int getBrokerId() {
            return brokerId;
        }

        public TimelineHashSet<S3WALObject> getWalObjects() {
            return walObjects;
        }

        @Override
        public String toString() {
            return "BrokerS3WALMetadata{" +
                "brokerId=" + brokerId +
                ", walObjects=" + walObjects +
                '}';
        }
    }

    private final SnapshotRegistry snapshotRegistry;

    private final Logger log;

    private final S3ObjectControlManager s3ObjectControlManager;

    private final TimelineHashMap<Long/*streamId*/, S3StreamMetadata> streamsMetadata;

    private final TimelineHashMap<Integer/*brokerId*/, BrokerS3WALMetadata> brokersMetadata;

    public StreamControlManager(
        SnapshotRegistry snapshotRegistry,
        LogContext logContext,
        S3ObjectControlManager s3ObjectControlManager) {
        this.snapshotRegistry = snapshotRegistry;
        this.log = logContext.logger(StreamControlManager.class);
        this.s3ObjectControlManager = s3ObjectControlManager;
        this.streamsMetadata = new TimelineHashMap<>(snapshotRegistry, 0);
        this.brokersMetadata = new TimelineHashMap<>(snapshotRegistry, 0);
    }

    public ControllerResult<CreateStreamResponseData> createStream(CreateStreamRequestData data) {
        long streamId = data.streamId();
        CreateStreamResponseData resp = new CreateStreamResponseData();
        if (this.streamsMetadata.containsKey(streamId)) {
            // already exist
            resp.setErrorCode(Errors.STREAM_EXIST.code());
            return ControllerResult.of(Collections.emptyList(), resp);
        }
        // create stream
        ApiMessageAndVersion record = new ApiMessageAndVersion(new S3StreamRecord()
            .setStreamId(streamId)
            .setEpoch(0)
            .setStartOffset(0L)
            .setRangeIndex(-1), (short) 0);
        return ControllerResult.of(Arrays.asList(record), resp);
    }

    public ControllerResult<OpenStreamResponseData> openStream(OpenStreamRequestData data) {
        OpenStreamResponseData resp = new OpenStreamResponseData();
        long streamId = data.streamId();
        int brokerId = data.brokerId();
        long epoch = data.streamEpoch();
        // verify stream exist
        if (!this.streamsMetadata.containsKey(streamId)) {
            resp.setErrorCode(Errors.STREAM_NOT_EXIST.code());
            return ControllerResult.of(Collections.emptyList(), resp);
        }
        // verify epoch match
        S3StreamMetadata streamMetadata = this.streamsMetadata.get(streamId);
        if (streamMetadata.currentEpoch > epoch) {
            resp.setErrorCode(Errors.STREAM_FENCED.code());
            return ControllerResult.of(Collections.emptyList(), resp);
        }
        if (streamMetadata.currentEpoch == epoch) {
            // epoch equals, verify broker
            RangeMetadata rangeMetadata = streamMetadata.ranges.get(streamMetadata.currentRangeIndex);
            if (rangeMetadata == null || rangeMetadata.brokerId() != brokerId) {
                resp.setErrorCode(Errors.STREAM_FENCED.code());
                return ControllerResult.of(Collections.emptyList(), resp);
            }
            // epoch equals, broker equals, regard it as redundant open operation, just return success
            resp.setStartOffset(streamMetadata.startOffset);
            return ControllerResult.of(Collections.emptyList(), resp);
        }
        // now the request in valid, update the stream's epoch and create a new range for this broker
        List<ApiMessageAndVersion> records = new ArrayList<>();
        long newEpoch = streamMetadata.currentEpoch + 1;
        int newRangeIndex = streamMetadata.currentRangeIndex + 1;
        // stream update record
        records.add(new ApiMessageAndVersion(new S3StreamRecord()
            .setStreamId(streamId)
            .setEpoch(newEpoch)
            .setRangeIndex(newRangeIndex)
            .setStartOffset(streamMetadata.startOffset), (short) 0));
        // get new range's start offset
        // default regard this range is the first range in stream, use 0 as start offset
        long startOffset = 0;
        if (newRangeIndex > 0) {
            // means that the new range is not the first range in stream, get the last range's end offset
            RangeMetadata lastRangeMetadata = streamMetadata.ranges.get(streamMetadata.currentRangeIndex);
            startOffset = lastRangeMetadata.endOffset() + 1;
        }
        // range create record
        records.add(new ApiMessageAndVersion(new RangeRecord()
            .setStreamId(streamId)
            .setBrokerId(brokerId)
            .setStartOffset(startOffset)
            // default end offset set to (startOffset - 1)
            .setEndOffset(startOffset - 1)
            .setEpoch(newEpoch)
            .setRangeIndex(newRangeIndex), (short) 0));
        resp.setStartOffset(startOffset);
        return ControllerResult.of(records, resp);
    }

    public ControllerResult<CloseStreamResponseData> closeStream(CloseStreamRequestData data) {
        throw new UnsupportedOperationException();
    }

    public ControllerResult<DeleteStreamResponseData> deleteStream(DeleteStreamRequestData data) {
        throw new UnsupportedOperationException();
    }

    public ControllerResult<CommitWALObjectResponseData> commitWALObject(CommitWALObjectRequestData data) {
        throw new UnsupportedOperationException();
    }

    public ControllerResult<CommitCompactObjectResponseData> commitCompactObject(CommitCompactObjectRequestData data) {
        throw new UnsupportedOperationException();
    }

    public ControllerResult<CommitStreamObjectResponseData> commitStreamObject(CommitStreamObjectRequestData data) {
        throw new UnsupportedOperationException();
    }


    public void replay(S3StreamRecord record) {
        long streamId = record.streamId();
        // already exist, update the stream's self metadata
        if (this.streamsMetadata.containsKey(streamId)) {
            S3StreamMetadata streamMetadata = this.streamsMetadata.get(streamId);
            streamMetadata.startOffset = record.startOffset();
            streamMetadata.currentEpoch = record.epoch();
            streamMetadata.currentRangeIndex = record.rangeIndex();
            return;
        }
        // not exist, create a new stream
        S3StreamMetadata streamMetadata = new S3StreamMetadata(record.epoch(), record.rangeIndex(),
            record.startOffset(), this.snapshotRegistry);
        this.streamsMetadata.put(streamId, streamMetadata);
    }

    public void replay(RemoveS3StreamRecord record) {
        long streamId = record.streamId();
        this.streamsMetadata.remove(streamId);
    }

    public void replay(RangeRecord record) {
        long streamId = record.streamId();
        S3StreamMetadata streamMetadata = this.streamsMetadata.get(streamId);
        if (streamMetadata == null) {
            // should not happen
            log.error("stream {} not exist when replay range record {}", streamId, record);
            return;
        }
        streamMetadata.ranges.put(record.rangeIndex(), RangeMetadata.of(record));
    }

    public void replay(RemoveRangeRecord record) {
        long streamId = record.streamId();
        S3StreamMetadata streamMetadata = this.streamsMetadata.get(streamId);
        if (streamMetadata == null) {
            // should not happen
            log.error("stream {} not exist when replay remove range record {}", streamId, record);
            return;
        }
        streamMetadata.ranges.remove(record.rangeIndex());
    }


    public Map<Long, S3StreamMetadata> streamsMetadata() {
        return streamsMetadata;
    }

    public Map<Integer, BrokerS3WALMetadata> brokersMetadata() {
        return brokersMetadata;
    }

    @Override
    public String toString() {
        return "StreamControlManager{" +
            "snapshotRegistry=" + snapshotRegistry +
            ", s3ObjectControlManager=" + s3ObjectControlManager +
            ", streamsMetadata=" + streamsMetadata +
            ", brokersMetadata=" + brokersMetadata +
            '}';
    }
}
