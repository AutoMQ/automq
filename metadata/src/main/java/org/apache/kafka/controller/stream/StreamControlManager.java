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
import java.util.stream.Collectors;
import org.apache.kafka.common.message.CloseStreamRequestData;
import org.apache.kafka.common.message.CloseStreamResponseData;
import org.apache.kafka.common.message.CommitStreamObjectRequestData;
import org.apache.kafka.common.message.CommitStreamObjectResponseData;
import org.apache.kafka.common.message.CommitWALObjectRequestData;
import org.apache.kafka.common.message.CommitWALObjectRequestData.ObjectStreamRange;
import org.apache.kafka.common.message.CommitWALObjectRequestData.StreamObject;
import org.apache.kafka.common.message.CommitWALObjectResponseData;
import org.apache.kafka.common.message.CreateStreamRequestData;
import org.apache.kafka.common.message.CreateStreamResponseData;
import org.apache.kafka.common.message.DeleteStreamRequestData;
import org.apache.kafka.common.message.DeleteStreamResponseData;
import org.apache.kafka.common.message.GetStreamsOffsetRequestData;
import org.apache.kafka.common.message.GetStreamsOffsetResponseData;
import org.apache.kafka.common.message.GetStreamsOffsetResponseData.StreamOffset;
import org.apache.kafka.common.message.OpenStreamRequestData;
import org.apache.kafka.common.message.OpenStreamResponseData;
import org.apache.kafka.common.metadata.AssignedStreamIdRecord;
import org.apache.kafka.common.metadata.BrokerWALMetadataRecord;
import org.apache.kafka.common.metadata.RangeRecord;
import org.apache.kafka.common.metadata.RemoveBrokerWALMetadataRecord;
import org.apache.kafka.common.metadata.RemoveRangeRecord;
import org.apache.kafka.common.metadata.RemoveS3StreamObjectRecord;
import org.apache.kafka.common.metadata.RemoveS3StreamRecord;
import org.apache.kafka.common.metadata.RemoveWALObjectRecord;
import org.apache.kafka.common.metadata.S3StreamObjectRecord;
import org.apache.kafka.common.metadata.S3StreamRecord;
import org.apache.kafka.common.metadata.WALObjectRecord;
import org.apache.kafka.common.metadata.WALObjectRecord.StreamIndex;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.controller.ControllerResult;
import org.apache.kafka.metadata.stream.RangeMetadata;
import org.apache.kafka.metadata.stream.StreamOffsetRange;
import org.apache.kafka.metadata.stream.S3StreamConstant;
import org.apache.kafka.metadata.stream.S3StreamObject;
import org.apache.kafka.metadata.stream.S3WALObject;
import org.apache.kafka.metadata.stream.StreamState;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.apache.kafka.timeline.TimelineInteger;
import org.apache.kafka.timeline.TimelineLong;
import org.apache.kafka.timeline.TimelineObject;
import org.slf4j.Logger;

/**
 * The StreamControlManager manages all Stream's lifecycle, such as create, open, delete, etc.
 */
public class StreamControlManager {

    public static class S3StreamMetadata {

        // current epoch, when created but not open, use -1 represent
        private TimelineLong currentEpoch;
        // rangeIndex, when created but not open, there is no range, use -1 represent
        private TimelineInteger currentRangeIndex;
        private TimelineLong startOffset;
        private TimelineObject<StreamState> currentState;
        private TimelineHashMap<Integer/*rangeIndex*/, RangeMetadata> ranges;
        private TimelineHashMap<Long/*objectId*/, S3StreamObject> streamObjects;

        public S3StreamMetadata(long currentEpoch, int currentRangeIndex, long startOffset,
            StreamState currentState, SnapshotRegistry registry) {
            this.currentEpoch = new TimelineLong(registry);
            this.currentEpoch.set(currentEpoch);
            this.currentRangeIndex = new TimelineInteger(registry);
            this.currentRangeIndex.set(currentRangeIndex);
            this.startOffset = new TimelineLong(registry);
            this.startOffset.set(startOffset);
            this.currentState = new TimelineObject<StreamState>(registry, currentState);
            this.ranges = new TimelineHashMap<>(registry, 0);
            this.streamObjects = new TimelineHashMap<>(registry, 0);
        }

        public long currentEpoch() {
            return currentEpoch.get();
        }

        public int currentRangeIndex() {
            return currentRangeIndex.get();
        }

        public long startOffset() {
            return startOffset.get();
        }

        public StreamState currentState() {
            return currentState.get();
        }

        public Map<Integer, RangeMetadata> ranges() {
            return ranges;
        }

        public RangeMetadata currentRangeMetadata() {
            return ranges.get(currentRangeIndex.get());
        }

        public Map<Long, S3StreamObject> streamObjects() {
            return streamObjects;
        }

        @Override
        public String toString() {
            return "S3StreamMetadata{" +
                "currentEpoch=" + currentEpoch.get() +
                ", currentState=" + currentState.get() +
                ", currentRangeIndex=" + currentRangeIndex.get() +
                ", startOffset=" + startOffset.get() +
                ", ranges=" + ranges +
                ", streamObjects=" + streamObjects +
                '}';
        }
    }

    public static class BrokerS3WALMetadata {

        private int brokerId;
        private TimelineHashMap<Long/*objectId*/, S3WALObject> walObjects;

        public BrokerS3WALMetadata(int brokerId, SnapshotRegistry registry) {
            this.brokerId = brokerId;
            this.walObjects = new TimelineHashMap<>(registry, 0);
        }

        public int getBrokerId() {
            return brokerId;
        }

        public TimelineHashMap<Long, S3WALObject> walObjects() {
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

    /**
     * The next stream id to be assigned.
     */
    private final TimelineLong nextAssignedStreamId;

    private final TimelineHashMap<Long/*streamId*/, S3StreamMetadata> streamsMetadata;

    private final TimelineHashMap<Integer/*brokerId*/, BrokerS3WALMetadata> brokersMetadata;

    public StreamControlManager(
        SnapshotRegistry snapshotRegistry,
        LogContext logContext,
        S3ObjectControlManager s3ObjectControlManager) {
        this.snapshotRegistry = snapshotRegistry;
        this.log = logContext.logger(StreamControlManager.class);
        this.s3ObjectControlManager = s3ObjectControlManager;
        this.nextAssignedStreamId = new TimelineLong(snapshotRegistry);
        this.streamsMetadata = new TimelineHashMap<>(snapshotRegistry, 0);
        this.brokersMetadata = new TimelineHashMap<>(snapshotRegistry, 0);
    }

    public ControllerResult<CreateStreamResponseData> createStream(CreateStreamRequestData data) {
        // TODO: pre assigned a batch of stream id in controller
        CreateStreamResponseData resp = new CreateStreamResponseData();
        long streamId = nextAssignedStreamId.get();
        // update assigned id
        ApiMessageAndVersion record0 = new ApiMessageAndVersion(new AssignedStreamIdRecord()
            .setAssignedStreamId(streamId), (short) 0);
        // create stream
        ApiMessageAndVersion record = new ApiMessageAndVersion(new S3StreamRecord()
            .setStreamId(streamId)
            .setEpoch(S3StreamConstant.INIT_EPOCH)
            .setStartOffset(S3StreamConstant.INIT_START_OFFSET)
            .setRangeIndex(S3StreamConstant.INIT_RANGE_INDEX), (short) 0);
        resp.setStreamId(streamId);
        log.info("[CreateStream]: create stream {} success", streamId);
        return ControllerResult.atomicOf(Arrays.asList(record0, record), resp);
    }

    public ControllerResult<OpenStreamResponseData> openStream(OpenStreamRequestData data) {
        OpenStreamResponseData resp = new OpenStreamResponseData();
        long streamId = data.streamId();
        int brokerId = data.brokerId();
        long epoch = data.streamEpoch();
        // verify stream exist
        if (!this.streamsMetadata.containsKey(streamId)) {
            resp.setErrorCode(Errors.STREAM_NOT_EXIST.code());
            log.warn("[OpenStream]: stream {} not exist", streamId);
            return ControllerResult.of(Collections.emptyList(), resp);
        }
        // verify epoch match
        S3StreamMetadata streamMetadata = this.streamsMetadata.get(streamId);
        if (streamMetadata.currentEpoch.get() > epoch) {
            resp.setErrorCode(Errors.STREAM_FENCED.code());
            log.warn("[OpenStream]: stream {}'s epoch {} is larger than request epoch {}", streamId,
                streamMetadata.currentEpoch.get(), epoch);
            return ControllerResult.of(Collections.emptyList(), resp);
        }
        if (streamMetadata.currentEpoch.get() == epoch) {
            if (streamMetadata.currentState() == StreamState.CLOSED) {
                resp.setErrorCode(Errors.STREAM_FENCED.code());
                return ControllerResult.of(Collections.emptyList(), resp);
            }
            // verify broker
            RangeMetadata rangeMetadata = streamMetadata.ranges.get(streamMetadata.currentRangeIndex());
            if (rangeMetadata == null) {
                // should not happen
                log.error("[OpenStream]: stream {}'s current range {} not exist when open stream with epoch: {}", streamId,
                    streamMetadata.currentRangeIndex(), epoch);
                resp.setErrorCode(Errors.STREAM_INNER_ERROR.code());
                return ControllerResult.of(Collections.emptyList(), resp);
            }
            if (rangeMetadata.brokerId() != brokerId) {
                log.warn("[OpenStream]: stream {}'s current range {}'s broker {} is not equal to request broker {}",
                    streamId, streamMetadata.currentRangeIndex(), rangeMetadata.brokerId(), brokerId);
                resp.setErrorCode(Errors.STREAM_FENCED.code());
                return ControllerResult.of(Collections.emptyList(), resp);
            }
            // epoch equals, broker equals, regard it as redundant open operation, just return success
            resp.setStartOffset(streamMetadata.startOffset());
            resp.setNextOffset(rangeMetadata.endOffset());
            return ControllerResult.of(Collections.emptyList(), resp);
        }
        if (streamMetadata.currentState() == StreamState.OPENED) {
            // stream still in opened state, can't open until it is closed
            log.warn("[OpenStream]: stream {}'s state still is OPENED at epoch: {}", streamId, streamMetadata.currentEpoch());
            resp.setErrorCode(Errors.STREAM_NOT_CLOSED.code());
            return ControllerResult.of(Collections.emptyList(), resp);
        }
        // now the request in valid, update the stream's epoch and create a new range for this broker
        List<ApiMessageAndVersion> records = new ArrayList<>();
        long newEpoch = epoch;
        int newRangeIndex = streamMetadata.currentRangeIndex() + 1;
        // stream update record
        records.add(new ApiMessageAndVersion(new S3StreamRecord()
            .setStreamId(streamId)
            .setEpoch(newEpoch)
            .setRangeIndex(newRangeIndex)
            .setStartOffset(streamMetadata.startOffset())
            .setStreamState(StreamState.OPENED.toByte()), (short) 0));
        // get new range's start offset
        // default regard this range is the first range in stream, use 0 as start offset
        long startOffset = 0;
        if (newRangeIndex > 0) {
            // means that the new range is not the first range in stream, get the last range's end offset
            RangeMetadata lastRangeMetadata = streamMetadata.ranges.get(streamMetadata.currentRangeIndex.get());
            startOffset = lastRangeMetadata.endOffset();
        }
        // range create record
        records.add(new ApiMessageAndVersion(new RangeRecord()
            .setStreamId(streamId)
            .setBrokerId(brokerId)
            .setStartOffset(startOffset)
            .setEndOffset(startOffset)
            .setEpoch(newEpoch)
            .setRangeIndex(newRangeIndex), (short) 0));
        resp.setStartOffset(streamMetadata.startOffset());
        resp.setNextOffset(startOffset);
        log.info("[OpenStream]: broker: {} open stream: {} with epoch: {} success", brokerId, streamId, epoch);
        return ControllerResult.atomicOf(records, resp);
    }

    public ControllerResult<CloseStreamResponseData> closeStream(CloseStreamRequestData data) {
        CloseStreamResponseData resp = new CloseStreamResponseData();
        long streamId = data.streamId();
        int brokerId = data.brokerId();
        long epoch = data.streamEpoch();
        if (!this.streamsMetadata.containsKey(streamId)) {
            resp.setErrorCode(Errors.STREAM_NOT_EXIST.code());
            log.warn("[CloseStream]: stream {} not exist", streamId);
            return ControllerResult.of(Collections.emptyList(), resp);
        }
        // verify epoch match
        S3StreamMetadata streamMetadata = this.streamsMetadata.get(streamId);
        if (streamMetadata.currentEpoch() > epoch) {
            resp.setErrorCode(Errors.STREAM_FENCED.code());
            log.warn("[CloseStream]: stream {}'s epoch {} is larger than request epoch {}", streamId,
                streamMetadata.currentEpoch.get(), epoch);
            return ControllerResult.of(Collections.emptyList(), resp);
        }
        if (streamMetadata.currentEpoch() < epoch) {
            // should not happen
            log.error("[CloseStream]: stream {}'s epoch {} is smaller than request epoch {}", streamId,
                streamMetadata.currentEpoch.get(), epoch);
            resp.setErrorCode(Errors.STREAM_INNER_ERROR.code());
        }
        // verify broker
        RangeMetadata rangeMetadata = streamMetadata.ranges.get(streamMetadata.currentRangeIndex());
        if (rangeMetadata == null) {
            // should not happen
            log.error("[CloseStream]: stream {}'s current range {} not exist when close stream with epoch: {}", streamId,
                streamMetadata.currentRangeIndex(), epoch);
            resp.setErrorCode(Errors.STREAM_INNER_ERROR.code());
            return ControllerResult.of(Collections.emptyList(), resp);
        }
        if (rangeMetadata.brokerId() != brokerId) {
            log.warn("[CloseStream]: stream {}'s current range {}'s broker {} is not equal to request broker {}",
                streamId, streamMetadata.currentRangeIndex(), rangeMetadata.brokerId(), brokerId);
            resp.setErrorCode(Errors.STREAM_FENCED.code());
            return ControllerResult.of(Collections.emptyList(), resp);
        }
        if (streamMetadata.currentState() == StreamState.CLOSED) {
            // regard it as redundant close operation, just return success
            return ControllerResult.of(Collections.emptyList(), resp);
        }
        // now the request in valid, update the stream's state
        // stream update record
        List<ApiMessageAndVersion> records = List.of(
            new ApiMessageAndVersion(new S3StreamRecord()
                .setStreamId(streamId)
                .setEpoch(epoch)
                .setRangeIndex(streamMetadata.currentRangeIndex())
                .setStartOffset(streamMetadata.startOffset())
                .setStreamState(StreamState.CLOSED.toByte()), (short) 0));
        log.info("[CloseStream]: broker: {} close stream: {} with epoch: {} success", brokerId, streamId, epoch);
        return ControllerResult.atomicOf(records, resp);
    }

    public ControllerResult<DeleteStreamResponseData> deleteStream(DeleteStreamRequestData data) {
        throw new UnsupportedOperationException();
    }

    public ControllerResult<CommitWALObjectResponseData> commitWALObject(CommitWALObjectRequestData data) {
        // TODO: deal with compacted objects, mark delete compacted object
        // TODO: deal with stream objects, replay streamObjectRecord to advance stream's end offset
        CommitWALObjectResponseData resp = new CommitWALObjectResponseData();
        List<ApiMessageAndVersion> records = new ArrayList<>();
        long objectId = data.objectId();
        int brokerId = data.brokerId();
        long objectSize = data.objectSize();
        long orderId = data.orderId();
        List<ObjectStreamRange> streamRanges = data.objectStreamRanges();
        // commit object
        ControllerResult<Boolean> commitResult = this.s3ObjectControlManager.commitObject(objectId, objectSize);
        if (!commitResult.response()) {
            log.error("object {} not exist when commit wal object", objectId);
            resp.setErrorCode(Errors.OBJECT_NOT_EXIST.code());
            return ControllerResult.of(Collections.emptyList(), resp);
        }
        records.addAll(commitResult.records());
        // mark destroy compacted object
        if (data.compactedObjectIds() != null && !data.compactedObjectIds().isEmpty()) {
            ControllerResult<Boolean> destroyResult = this.s3ObjectControlManager.markDestroyObjects(data.compactedObjectIds());
            if (!destroyResult.response()) {
                log.error("Mark destroy compacted objects {} failed", String.join(",", data.compactedObjectIds().toArray(new String[0])));
                resp.setErrorCode(Errors.STREAM_INNER_ERROR.code());
                return ControllerResult.of(Collections.emptyList(), resp);
            }
            records.addAll(destroyResult.records());
        }

        List<StreamOffsetRange> indexes = streamRanges.stream()
            .map(range -> new StreamOffsetRange(range.streamId(), range.startOffset(), range.endOffset()))
            .collect(Collectors.toList());
        // update broker's wal object
        BrokerS3WALMetadata brokerMetadata = this.brokersMetadata.get(brokerId);
        if (brokerMetadata == null) {
            // first time commit wal object, generate broker's metadata record
            records.add(new ApiMessageAndVersion(new BrokerWALMetadataRecord()
                .setBrokerId(brokerId), (short) 0));
        }
        // generate broker's wal object record
        records.add(new ApiMessageAndVersion(new WALObjectRecord()
            .setObjectId(objectId)
            .setOrderId(orderId)
            .setBrokerId(brokerId)
            .setStreamsIndex(
                indexes.stream()
                    .map(StreamOffsetRange::toRecordStreamIndex)
                    .collect(Collectors.toList())), (short) 0));
        // generate compacted objects' remove record
        data.compactedObjectIds().forEach(id -> records.add(new ApiMessageAndVersion(new RemoveWALObjectRecord()
            .setObjectId(id), (short) 0)));
        // create stream object records
        // TODO: deal with the lifecycle of stream object's source objects, when and how to delete them ?
        List<StreamObject> streamObjects = data.streamObjects();
        streamObjects.stream().forEach(obj -> {
            long streamId = obj.streamId();
            long startOffset = obj.startOffset();
            long endOffset = obj.endOffset();
            records.add(new S3StreamObject(obj.objectId(), obj.objectSize(), streamId, startOffset, endOffset).toRecord());
        });
        log.info("[CommitWALObject]: broker: {} commit wal object {} success", brokerId, objectId);
        return ControllerResult.atomicOf(records, resp);
    }

    public ControllerResult<CommitStreamObjectResponseData> commitStreamObject(CommitStreamObjectRequestData data) {
        throw new UnsupportedOperationException();
    }

    public GetStreamsOffsetResponseData getStreamsOffset(GetStreamsOffsetRequestData data) {
        List<Long> streamIds = data.streamIds();
        GetStreamsOffsetResponseData resp = new GetStreamsOffsetResponseData();
        List<StreamOffset> streamOffsets = streamIds.stream()
            .filter(this.streamsMetadata::containsKey)
            .map(id -> {
                S3StreamMetadata streamMetadata = this.streamsMetadata.get(id);
                RangeMetadata range = streamMetadata.ranges().get(streamMetadata.currentRangeIndex());
                long startOffset = streamMetadata.startOffset();
                long endOffset = range == null ? startOffset : range.endOffset();
                return new StreamOffset()
                    .setStreamId(id)
                    .setStartOffset(startOffset)
                    .setEndOffset(endOffset);
            }).collect(Collectors.toList());
        resp.setStreamsOffset(streamOffsets);
        return resp;
    }

    public void replay(AssignedStreamIdRecord record) {
        this.nextAssignedStreamId.set(record.assignedStreamId() + 1);
    }

    public void replay(S3StreamRecord record) {
        long streamId = record.streamId();
        // already exist, update the stream's self metadata
        if (this.streamsMetadata.containsKey(streamId)) {
            S3StreamMetadata streamMetadata = this.streamsMetadata.get(streamId);
            streamMetadata.startOffset.set(record.startOffset());
            streamMetadata.currentEpoch.set(record.epoch());
            streamMetadata.currentRangeIndex.set(record.rangeIndex());
            streamMetadata.currentState.set(StreamState.fromByte(record.streamState()));
            return;
        }
        // not exist, create a new stream
        S3StreamMetadata streamMetadata = new S3StreamMetadata(record.epoch(), record.rangeIndex(),
            record.startOffset(), StreamState.fromByte(record.streamState()), this.snapshotRegistry);
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

    public void replay(BrokerWALMetadataRecord record) {
        int brokerId = record.brokerId();
        this.brokersMetadata.computeIfAbsent(brokerId, id -> new BrokerS3WALMetadata(id, this.snapshotRegistry));
    }

    public void replay(WALObjectRecord record) {
        long objectId = record.objectId();
        int brokerId = record.brokerId();
        long orderId = record.orderId();
        List<StreamIndex> streamIndexes = record.streamsIndex();
        BrokerS3WALMetadata brokerMetadata = this.brokersMetadata.get(brokerId);
        if (brokerMetadata == null) {
            // should not happen
            log.error("broker {} not exist when replay wal object record {}", brokerId, record);
            return;
        }

        // create wal object
        Map<Long, List<StreamOffsetRange>> indexMap = streamIndexes
            .stream()
            .map(StreamOffsetRange::of)
            .collect(Collectors.groupingBy(StreamOffsetRange::getStreamId));
        brokerMetadata.walObjects.put(objectId, new S3WALObject(objectId, brokerId, indexMap, orderId));

        // update range
        record.streamsIndex().forEach(index -> {
            long streamId = index.streamId();
            S3StreamMetadata metadata = this.streamsMetadata.get(streamId);
            if (metadata == null) {
                // ignore it
                return;
            }
            RangeMetadata rangeMetadata = metadata.currentRangeMetadata();
            if (rangeMetadata == null) {
                // ignore it
                return;
            }
            if (rangeMetadata.endOffset() != index.startOffset()) {
                // ignore it
                return;
            }
            rangeMetadata.setEndOffset(index.endOffset());
        });
    }

    public void replay(RemoveWALObjectRecord record) {
        long objectId = record.objectId();
        BrokerS3WALMetadata walMetadata = this.brokersMetadata.get(record.brokerId());
        if (walMetadata == null) {
            // should not happen
            log.error("broker {} not exist when replay remove wal object record {}", record.brokerId(), record);
            return;
        }
        walMetadata.walObjects.remove(objectId);
    }
    public void replay(S3StreamObjectRecord record) {
        long objectId = record.objectId();
        long streamId = record.streamId();
        long startOffset = record.startOffset();
        long endOffset = record.endOffset();
        long objectSize = record.objectSize();

        S3StreamMetadata streamMetadata = this.streamsMetadata.get(streamId);
        if (streamMetadata == null) {
            // should not happen
            log.error("stream {} not exist when replay stream object record {}", streamId, record);
            return;
        }
        streamMetadata.streamObjects.put(objectId, new S3StreamObject(objectId, objectSize, streamId, startOffset, endOffset));
        // update range
        RangeMetadata rangeMetadata = streamMetadata.currentRangeMetadata();
        if (rangeMetadata == null) {
            // ignore it
            return;
        }
        if (rangeMetadata.endOffset() != startOffset) {
            // ignore it
            return;
        }
        rangeMetadata.setEndOffset(endOffset);
    }

    public void replay(RemoveS3StreamObjectRecord record) {
        long streamId = record.streamId();
        long objectId = record.objectId();
        S3StreamMetadata streamMetadata = this.streamsMetadata.get(streamId);
        if (streamMetadata == null) {
            // should not happen
            log.error("stream {} not exist when replay remove stream object record {}", streamId, record);
            return;
        }
        streamMetadata.streamObjects.remove(objectId);
    }

    public void replay(RemoveBrokerWALMetadataRecord record) {
        int brokerId = record.brokerId();
        this.brokersMetadata.remove(brokerId);
    }


    public Map<Long, S3StreamMetadata> streamsMetadata() {
        return streamsMetadata;
    }

    public Map<Integer, BrokerS3WALMetadata> brokersMetadata() {
        return brokersMetadata;
    }

    public Long nextAssignedStreamId() {
        return nextAssignedStreamId.get();
    }

    private boolean verifyWalStreamRanges(ObjectStreamRange range, long brokerId) {
        long streamId = range.streamId();
        long epoch = range.streamEpoch();
        // verify
        S3StreamMetadata streamMetadata = this.streamsMetadata.get(streamId);
        if (streamMetadata == null) {
            return false;
        }
        // compare epoch
        if (streamMetadata.currentEpoch() > epoch) {
            return false;
        }
        if (streamMetadata.currentEpoch() < epoch) {
            return false;
        }
        RangeMetadata rangeMetadata = streamMetadata.ranges.get(streamMetadata.currentRangeIndex.get());
        if (rangeMetadata == null) {
            return false;
        }
        // compare broker
        if (rangeMetadata.brokerId() != brokerId) {
            return false;
        }
        // compare offset
        if (rangeMetadata.endOffset() != range.startOffset()) {
            return false;
        }
        return true;
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
