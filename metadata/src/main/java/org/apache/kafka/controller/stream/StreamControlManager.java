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

import com.automq.stream.s3.metadata.S3StreamConstant;
import com.automq.stream.s3.metadata.StreamOffsetRange;
import com.automq.stream.s3.metadata.StreamState;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.CloseStreamsRequestData.CloseStreamRequest;
import org.apache.kafka.common.message.CloseStreamsResponseData.CloseStreamResponse;
import org.apache.kafka.common.message.CommitStreamObjectRequestData;
import org.apache.kafka.common.message.CommitStreamObjectResponseData;
import org.apache.kafka.common.message.CommitStreamSetObjectRequestData;
import org.apache.kafka.common.message.CommitStreamSetObjectRequestData.ObjectStreamRange;
import org.apache.kafka.common.message.CommitStreamSetObjectRequestData.StreamObject;
import org.apache.kafka.common.message.CommitStreamSetObjectResponseData;
import org.apache.kafka.common.message.CreateStreamsRequestData.CreateStreamRequest;
import org.apache.kafka.common.message.CreateStreamsResponseData.CreateStreamResponse;
import org.apache.kafka.common.message.DeleteStreamsRequestData.DeleteStreamRequest;
import org.apache.kafka.common.message.DeleteStreamsResponseData.DeleteStreamResponse;
import org.apache.kafka.common.message.DescribeStreamsRequestData;
import org.apache.kafka.common.message.DescribeStreamsResponseData;
import org.apache.kafka.common.message.GetOpeningStreamsRequestData;
import org.apache.kafka.common.message.GetOpeningStreamsResponseData;
import org.apache.kafka.common.message.GetOpeningStreamsResponseData.StreamMetadata;
import org.apache.kafka.common.message.OpenStreamsRequestData.OpenStreamRequest;
import org.apache.kafka.common.message.OpenStreamsResponseData.OpenStreamResponse;
import org.apache.kafka.common.message.TrimStreamsRequestData.TrimStreamRequest;
import org.apache.kafka.common.message.TrimStreamsResponseData.TrimStreamResponse;
import org.apache.kafka.common.metadata.AssignedStreamIdRecord;
import org.apache.kafka.common.metadata.NodeWALMetadataRecord;
import org.apache.kafka.common.metadata.RangeRecord;
import org.apache.kafka.common.metadata.RemoveNodeWALMetadataRecord;
import org.apache.kafka.common.metadata.RemoveRangeRecord;
import org.apache.kafka.common.metadata.RemoveS3StreamObjectRecord;
import org.apache.kafka.common.metadata.RemoveS3StreamRecord;
import org.apache.kafka.common.metadata.RemoveStreamSetObjectRecord;
import org.apache.kafka.common.metadata.S3StreamObjectRecord;
import org.apache.kafka.common.metadata.S3StreamRecord;
import org.apache.kafka.common.metadata.S3StreamSetObjectRecord;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.controller.ClusterControlManager;
import org.apache.kafka.controller.ControllerResult;
import org.apache.kafka.controller.FeatureControlManager;
import org.apache.kafka.controller.QuorumController;
import org.apache.kafka.controller.ReplicationControlManager;
import org.apache.kafka.metadata.stream.RangeMetadata;
import org.apache.kafka.metadata.stream.S3StreamObject;
import org.apache.kafka.metadata.stream.S3StreamSetObject;
import org.apache.kafka.metadata.stream.StreamTags;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.automq.AutoMQVersion;
import org.apache.kafka.server.metrics.s3stream.S3StreamKafkaMetricsManager;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.apache.kafka.timeline.TimelineLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.automq.stream.s3.metadata.ObjectUtils.NOOP_OBJECT_ID;

/**
 * The StreamControlManager manages all Stream's lifecycle, such as create, open, delete, etc.
 */
@SuppressWarnings("all")
public class StreamControlManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamControlManager.class);

    private final Logger log;

    /**
     * The next stream id to be assigned.
     */
    private final TimelineLong nextAssignedStreamId;

    private final TimelineHashMap<Long/*streamId*/, S3StreamMetadata> streamsMetadata;

    private final TimelineHashMap<Integer/*nodeId*/, NodeMetadata> nodesMetadata;

    private final QuorumController quorumController;

    private final SnapshotRegistry snapshotRegistry;

    private final S3ObjectControlManager s3ObjectControlManager;

    private final ClusterControlManager clusterControlManager;

    private final FeatureControlManager featureControlManager;
    private final ReplicationControlManager replicationControlManager;

    public StreamControlManager(
        QuorumController quorumController,
        SnapshotRegistry snapshotRegistry,
        LogContext logContext,
        S3ObjectControlManager s3ObjectControlManager,
        ClusterControlManager clusterControlManager,
        FeatureControlManager featureControlManager,
        ReplicationControlManager replicationControlManager) {
        this.snapshotRegistry = snapshotRegistry;
        this.log = logContext.logger(StreamControlManager.class);
        this.nextAssignedStreamId = new TimelineLong(snapshotRegistry);
        this.streamsMetadata = new TimelineHashMap<>(snapshotRegistry, 0);
        this.nodesMetadata = new TimelineHashMap<>(snapshotRegistry, 0);

        ScheduledExecutorService cleanupScheduler = Executors.newSingleThreadScheduledExecutor(
            ThreadUtils.createThreadFactory("stream-cleanup-scheduler", true));

        this.quorumController = quorumController;
        this.s3ObjectControlManager = s3ObjectControlManager;
        this.clusterControlManager = clusterControlManager;
        this.featureControlManager = featureControlManager;
        this.replicationControlManager = replicationControlManager;

        cleanupScheduler.scheduleWithFixedDelay(this::triggerCleanupScaleInNodes, 30, 30, TimeUnit.MINUTES);

        S3StreamKafkaMetricsManager.setStreamSetObjectNumSupplier(() -> {
            Map<String, Integer> numMap = new HashMap<>();
            for (NodeMetadata nodeMetadata : nodesMetadata.values()) {
                numMap.put(String.valueOf(nodeMetadata.getNodeId()), nodeMetadata.streamSetObjects().size());
            }
            return numMap;
        });
        S3StreamKafkaMetricsManager.setStreamObjectNumSupplier(() -> streamsMetadata.values().stream().mapToInt(it -> it.streamObjects().size()).sum());
    }

    public ControllerResult<CreateStreamResponse> createStream(int nodeId, long nodeEpoch,
        CreateStreamRequest request) {
        CreateStreamResponse resp = new CreateStreamResponse();
        // verify node epoch
        Errors nodeEpochCheckResult = nodeEpochCheck(nodeId, nodeEpoch);
        if (nodeEpochCheckResult != Errors.NONE) {
            resp.setErrorCode(nodeEpochCheckResult.code());
            log.warn("[CreateStream] invalid node epoch. nodeId={}, nodeEpoch={}, error={}",
                nodeId, nodeEpoch, nodeEpochCheckResult);
            return ControllerResult.of(Collections.emptyList(), resp);
        }
        // TODO: pre assigned a batch of stream id in controller
        long streamId = nextAssignedStreamId.get();
        // update assigned id
        ApiMessageAndVersion record0 = new ApiMessageAndVersion(new AssignedStreamIdRecord()
            .setAssignedStreamId(streamId), (short) 0);
        // create stream
        S3StreamRecord s3StreamRecord = new S3StreamRecord()
            .setStreamId(streamId)
            .setEpoch(S3StreamConstant.INIT_EPOCH)
            .setStartOffset(S3StreamConstant.INIT_START_OFFSET)
            .setRangeIndex(S3StreamConstant.INIT_RANGE_INDEX);
        AutoMQVersion autoMQVersion = featureControlManager.autoMQVersion();
        if (autoMQVersion.isStreamTagsSupported()) {
            S3StreamRecord.TagCollection tags = new S3StreamRecord.TagCollection();
            request.tags().forEach(tag -> tags.add(new S3StreamRecord.Tag().setKey(tag.key()).setValue(tag.value())));
            s3StreamRecord.setTags(tags);
        }
        ApiMessageAndVersion record = new ApiMessageAndVersion(s3StreamRecord, autoMQVersion.streamRecordVersion());
        resp.setStreamId(streamId);
        log.info("[CreateStream] successfully create a stream. streamId={}, nodeId={}, nodeEpoch={}", streamId, nodeId, nodeEpoch);
        return ControllerResult.atomicOf(Arrays.asList(record0, record), resp);
    }

    /**
     * Open stream.
     * <p>
     * <b>Response Errors Enum:</b>
     * <ul>
     *     <li>
     *         <code>STREAM_FENCED</code>:
     *          <ol>
     *              <li> stream's epoch is larger than request epoch </li>
     *              <li> stream's current range's node is not equal to request node </li>
     *              <li> stream's epoch matched, but stream's state is <code>CLOSED</code> </li>
     *          </ol>
     *     </li>
     *     <li>
     *         <code>STREAM_NOT_EXIST</code>
     *         <ol>
     *             <li> stream's id not exist in current stream-metadata </li>
     *         </ol>
     *     </li>
     *     <li>
     *         <code>STREAM_NOT_CLOSED</code>
     *         <ol>
     *             <li> request with higher epoch but stream's state is <code>OPENED</code> </li>
     *         </ol>
     *     </li>
     *     <li>
     *         <code>STREAM_INNER_ERROR</code>
     *         <ol>
     *             <li> stream's current range not exist when stream has been opened </li>
     *         </ol>
     *     </li>
     * </ul>
     */
    public ControllerResult<OpenStreamResponse> openStream(int nodeId, long nodeEpoch, OpenStreamRequest request) {
        OpenStreamResponse resp = new OpenStreamResponse();
        long streamId = request.streamId();
        long epoch = request.streamEpoch();

        AutoMQVersion version = featureControlManager.autoMQVersion();

        // verify node epoch
        Errors nodeEpochCheckResult = nodeEpochCheck(nodeId, nodeEpoch);
        if (nodeEpochCheckResult != Errors.NONE) {
            resp.setErrorCode(nodeEpochCheckResult.code());
            log.warn("[OpenStream] invalid node epoch. streamId={}, nodeId={}, nodeEpoch={}, error={}",
                streamId, nodeId, nodeEpoch, nodeEpochCheckResult);
            return ControllerResult.of(Collections.emptyList(), resp);
        }

        // verify stream exist
        if (!this.streamsMetadata.containsKey(streamId)) {
            resp.setErrorCode(Errors.STREAM_NOT_EXIST.code());
            log.warn("[OpenStream] stream not exist. streamId={}, nodeId={}, nodeEpoch={}", streamId, nodeId, nodeEpoch);
            return ControllerResult.of(Collections.emptyList(), resp);
        }
        // verify epoch match
        S3StreamMetadata streamMetadata = this.streamsMetadata.get(streamId);
        if (streamMetadata.currentEpoch() > epoch) {
            resp.setErrorCode(Errors.STREAM_FENCED.code());
            log.warn("[OpenStream] steam has been fenced. streamId={}, streamEpoch={}, requestEpoch={}, nodeId={}, nodeEpoch={}",
                streamId, streamMetadata.currentEpoch(), epoch, nodeId, nodeEpoch);
            return ControllerResult.of(Collections.emptyList(), resp);
        }
        if (streamMetadata.currentEpoch() == epoch) {
            // node may use the same epoch to open -> close -> open stream.
            // verify node
            RangeMetadata rangeMetadata = streamMetadata.ranges().get(streamMetadata.currentRangeIndex());
            if (rangeMetadata == null) {
                // should not happen
                log.error("[OpenStream] the current range not exist. streamId={}, streamEpoch={}, currentRangeIndex={}, nodeId={}, nodeEpoch={}",
                    streamId, streamMetadata.currentEpoch(), streamMetadata.currentRangeIndex(), nodeId, nodeEpoch);
                resp.setErrorCode(Errors.STREAM_INNER_ERROR.code());
                return ControllerResult.of(Collections.emptyList(), resp);
            }
            if (rangeMetadata.nodeId() != nodeId) {
                log.warn("[OpenStream] the current range owner mismatch. streamId={}, streamEpoch={}, currentRangeIndex={}, ownerId={}, nodeId={}, nodeEpoch={}",
                    streamId, streamMetadata.currentEpoch(), streamMetadata.currentRangeIndex(), rangeMetadata.nodeId(), nodeId, nodeEpoch);
                resp.setErrorCode(Errors.STREAM_FENCED.code());
                return ControllerResult.of(Collections.emptyList(), resp);
            }
            // epoch equals, node equals, regard it as redundant open operation, just return success
            resp.setStartOffset(streamMetadata.startOffset());
            resp.setNextOffset(rangeMetadata.endOffset());
            List<ApiMessageAndVersion> records = new ArrayList<>();
            if (streamMetadata.currentState() == StreamState.CLOSED) {
                records.add(new ApiMessageAndVersion(new S3StreamRecord()
                    .setStreamId(streamId)
                    .setEpoch(epoch)
                    .setRangeIndex(streamMetadata.currentRangeIndex())
                    .setStartOffset(streamMetadata.startOffset())
                    .setStreamState(StreamState.OPENED.toByte()), version.streamRecordVersion()));
            }
            return ControllerResult.of(records, resp);
        }
        if (streamMetadata.currentState() == StreamState.OPENED) {
            // the stream still in opened state, so it can't open until it is closed
            log.warn("[OpenStream] stream still in opened state. streamId={}, streamEpoch={}, ownerId={}, nodeId={}, nodeEpoch={}",
                streamId, streamMetadata.currentEpoch(), streamMetadata.currentRangeOwner(), nodeId, nodeEpoch);
            resp.setErrorCode(Errors.STREAM_NOT_CLOSED.code());
            return ControllerResult.of(Collections.emptyList(), resp);
        }
        // now the request is valid, update the stream's epoch and create a new range for this node
        List<ApiMessageAndVersion> records = new ArrayList<>();
        int newRangeIndex = streamMetadata.currentRangeIndex() + 1;
        // stream update record
        AutoMQVersion autoMQVersion = featureControlManager.autoMQVersion();
        S3StreamRecord s3StreamRecord = new S3StreamRecord()
            .setStreamId(streamId)
            .setEpoch(epoch)
            .setRangeIndex(newRangeIndex)
            .setStartOffset(streamMetadata.startOffset())
            .setStreamState(StreamState.OPENED.toByte());
        if (request.tags().size() > 0 && autoMQVersion.isStreamTagsSupported()) {
            // Compatible with the stream created in the old version, add missing tags for the stream.
            S3StreamRecord.TagCollection tags = new S3StreamRecord.TagCollection();
            request.tags().forEach(tag -> tags.add(new S3StreamRecord.Tag().setKey(tag.key()).setValue(tag.value())));
            s3StreamRecord.setTags(tags);
        }
        records.add(new ApiMessageAndVersion(s3StreamRecord, autoMQVersion.streamRecordVersion()));
        // get new range's start offset
        // default regard this range is the first range in stream, use 0 as start offset
        long startOffset = 0;
        if (newRangeIndex > 0) {
            // means that the new range is not the first range in stream, get the last range's end offset
            RangeMetadata lastRangeMetadata = streamMetadata.ranges().get(streamMetadata.currentRangeIndex());
            startOffset = lastRangeMetadata.endOffset();
            // the RangeMetadata in S3StreamMetadataImage is only update when create, rollToNext and trim
            records.add(new ApiMessageAndVersion(new RangeRecord()
                .setStreamId(streamId)
                .setNodeId(lastRangeMetadata.nodeId())
                .setStartOffset(lastRangeMetadata.startOffset())
                .setEndOffset(lastRangeMetadata.endOffset())
                .setEpoch(lastRangeMetadata.epoch())
                .setRangeIndex(lastRangeMetadata.rangeIndex()), (short) 0));
        }
        // Fix https://github.com/AutoMQ/automq/issues/1222
        // Handle the case: trim offset beyond the range end offset
        startOffset = Math.max(streamMetadata.startOffset(), startOffset);
        // range create record
        records.add(new ApiMessageAndVersion(new RangeRecord()
            .setStreamId(streamId)
            .setNodeId(nodeId)
            .setStartOffset(startOffset)
            .setEndOffset(startOffset)
            .setEpoch(epoch)
            .setRangeIndex(newRangeIndex), (short) 0));
        resp.setStartOffset(streamMetadata.startOffset());
        resp.setNextOffset(startOffset);
        log.info("[OpenStream] successfully open the stream. streamId={}, streamEpoch={}, nodeId={}, nodeEpoch={}",
            streamId, epoch, nodeId, nodeEpoch);
        return ControllerResult.atomicOf(records, resp);
    }

    /**
     * Close stream.
     * <p>
     * <b>Response Errors Enum:</b>
     * <ul>
     *     <li>
     *         <code>STREAM_FENCED</code>:
     *         <ol>
     *             <li> stream's epoch is larger than request epoch </li>
     *             <li> stream's current range's node is not equal to request node </li>
     *         </ol>
     *     </li>
     *     <li>
     *         <code>STREAM_NOT_EXIST</code>
     *         <ol>
     *             <li> stream's id not exist in current stream-metadata </li>
     *         </ol>
     *     </li>
     *     <li>
     *         <code>STREAM_INNER_ERROR</code>
     *         <ol>
     *             <li> stream's current range not exist when stream has been opened </li>
     *             <li> close stream with higher epoch </li>
     *         </ol>
     *     </li>
     * </ul>
     */
    public ControllerResult<CloseStreamResponse> closeStream(int nodeId, long nodeEpoch, CloseStreamRequest request) {
        CloseStreamResponse resp = new CloseStreamResponse();
        long streamId = request.streamId();
        long epoch = request.streamEpoch();

        // verify node epoch
        Errors nodeEpochCheckResult = nodeEpochCheck(nodeId, nodeEpoch, false);
        if (nodeEpochCheckResult != Errors.NONE) {
            resp.setErrorCode(nodeEpochCheckResult.code());
            log.warn("[CloseStream] invalid node epoch. streamId={}, streamEpoch={}, nodeId={}, nodeEpoch={}, error={}",
                streamId, epoch, nodeId, nodeEpoch, nodeEpochCheckResult);
            return ControllerResult.of(Collections.emptyList(), resp);
        }

        // verify ownership
        Errors authResult = streamOwnershipCheck(streamId, epoch, nodeId, "CloseStream");
        if (authResult != Errors.NONE) {
            resp.setErrorCode(authResult.code());
            return ControllerResult.of(Collections.emptyList(), resp);
        }
        S3StreamMetadata streamMetadata = this.streamsMetadata.get(streamId);
        if (streamMetadata.currentState() == StreamState.CLOSED) {
            // regard it as a redundant close operation, just return success
            return ControllerResult.of(Collections.emptyList(), resp);
        }
        // now the request is valid, update the stream's state
        // stream update record
        List<ApiMessageAndVersion> records = List.of(
            new ApiMessageAndVersion(new S3StreamRecord()
                .setStreamId(streamId)
                .setEpoch(epoch)
                .setRangeIndex(streamMetadata.currentRangeIndex())
                .setStartOffset(streamMetadata.startOffset())
                .setStreamState(StreamState.CLOSED.toByte()),
                featureControlManager.autoMQVersion().streamRecordVersion()));
        log.info("[CloseStream] successfully close the stream. streamId={}, streamEpoch={}, nodeId={}, nodeEpoch={}",
            streamId, epoch, nodeId, nodeEpoch);
        return ControllerResult.atomicOf(records, resp);
    }

    public ControllerResult<TrimStreamResponse> trimStream(int nodeId, long nodeEpoch, TrimStreamRequest request) {
        long epoch = request.streamEpoch();
        long streamId = request.streamId();
        long newStartOffset = request.newStartOffset();
        TrimStreamResponse resp = new TrimStreamResponse();

        // verify node epoch
        Errors nodeEpochCheckResult = nodeEpochCheck(nodeId, nodeEpoch);
        if (nodeEpochCheckResult != Errors.NONE) {
            resp.setErrorCode(nodeEpochCheckResult.code());
            log.warn("[TrimStream] invalid node epoch. streamId={}, streamEpoch={}, nodeId={}, nodeEpoch={}, error={}",
                streamId, epoch, nodeId, nodeEpoch, nodeEpochCheckResult);
            return ControllerResult.of(Collections.emptyList(), resp);
        }

        // verify ownership
        Errors authResult = streamOwnershipCheck(streamId, epoch, nodeId, "TrimStream");
        if (authResult != Errors.NONE) {
            resp.setErrorCode(authResult.code());
            return ControllerResult.of(Collections.emptyList(), resp);
        }
        S3StreamMetadata streamMetadata = this.streamsMetadata.get(streamId);
        if (streamMetadata.currentState() == StreamState.CLOSED) {
            log.warn("[TrimStream] can't trim a closed stream. streamId={}, streamEpoch={}, nodeId={}, nodeEpoch={}",
                streamId, epoch, nodeId, nodeEpoch);
            resp.setErrorCode(Errors.STREAM_NOT_OPENED.code());
            return ControllerResult.of(Collections.emptyList(), resp);
        }
        if (streamMetadata.startOffset() > newStartOffset) {
            log.warn("[TrimStream] trim offset less than start offset. streamId={}, streamEpoch={}, trimOffset={}, startOffset={}, nodeId={}, nodeEpoch={}",
                streamId, epoch, newStartOffset, streamMetadata.startOffset(), nodeId, nodeEpoch);
            resp.setErrorCode(Errors.OFFSET_NOT_MATCHED.code());
            return ControllerResult.of(Collections.emptyList(), resp);
        }
        if (streamMetadata.startOffset() == newStartOffset) {
            // regard it as a redundant trim operation, just return success
            return ControllerResult.of(Collections.emptyList(), resp);
        }
        // now the request is valid
        // update the stream metadata start offset
        List<ApiMessageAndVersion> records = new ArrayList<>();
        records.add(new ApiMessageAndVersion(new S3StreamRecord()
            .setStreamId(streamId)
            .setEpoch(epoch)
            .setRangeIndex(streamMetadata.currentRangeIndex())
            .setStartOffset(newStartOffset)
            .setStreamState(streamMetadata.currentState().toByte()), featureControlManager.autoMQVersion().streamRecordVersion()));
        // remove range or update range's start offset
        for (Map.Entry<Integer, RangeMetadata> entry : streamMetadata.ranges().entrySet()) {
            Integer rangeIndex = entry.getKey();
            RangeMetadata range = entry.getValue();
            if (newStartOffset <= range.startOffset()) {
                continue;
            }
            if (rangeIndex == streamMetadata.currentRangeIndex()) {
                // current range, update start offset
                // if current range is [50, 100)
                // 1. try to trim to 60, then the current range will be [60, 100)
                // 2. try to trim to 100, then the current range will be [100, 100)
                // 3. try to trim to 110, then current range will be [100, 100)
                long newRangeStartOffset = Math.min(newStartOffset, range.endOffset());
                records.add(new ApiMessageAndVersion(new RangeRecord()
                    .setStreamId(streamId)
                    .setRangeIndex(rangeIndex)
                    .setNodeId(range.nodeId())
                    .setEpoch(range.epoch())
                    .setStartOffset(newRangeStartOffset)
                    .setEndOffset(range.endOffset()), (short) 0));
                continue;
            }
            if (newStartOffset >= range.endOffset()) {
                // remove range
                records.add(new ApiMessageAndVersion(new RemoveRangeRecord()
                    .setStreamId(streamId)
                    .setRangeIndex(rangeIndex), (short) 0));
                continue;
            }
            // update range's start offset
            records.add(new ApiMessageAndVersion(new RangeRecord()
                .setStreamId(streamId)
                .setNodeId(range.nodeId())
                .setStartOffset(newStartOffset)
                .setEndOffset(range.endOffset())
                .setEpoch(range.epoch())
                .setRangeIndex(rangeIndex), (short) 0));
        }
        if (resp.errorCode() != Errors.NONE.code()) {
            return ControllerResult.of(Collections.emptyList(), resp);
        }
        // the data in stream set object will be removed by compaction
        if (resp.errorCode() != Errors.NONE.code()) {
            return ControllerResult.of(Collections.emptyList(), resp);
        }
        log.info("[TrimStream] successfully trim the stream. streamId={}, streamEpoch={}, trimOffset={}, nodeId={}, nodeEpoch={}",
            streamId, epoch, newStartOffset, nodeId, nodeEpoch);
        return ControllerResult.atomicOf(records, resp);
    }

    public ControllerResult<DeleteStreamResponse> deleteStream(DeleteStreamRequest request) {
        DeleteStreamResponse resp = new DeleteStreamResponse();

        long streamId = request.streamId();

        S3StreamMetadata streamMetadata = this.streamsMetadata.get(streamId);
        if (streamMetadata == null) {
            log.warn("[DELETE_STREAM],[FAIL]: stream not exist. streamId={}", streamId);
            return ControllerResult.of(Collections.emptyList(), resp);
        }
        if (streamMetadata.currentState() != StreamState.CLOSED) {
            log.warn("[DELETE_STREAM],[FAIL]: stream is not closed. streamId={}", streamId);
            resp.setErrorCode(Errors.STREAM_NOT_CLOSED.code());
            return ControllerResult.of(Collections.emptyList(), resp);
        }

        // generate remove stream record
        List<ApiMessageAndVersion> records = new ArrayList<>();
        records.add(new ApiMessageAndVersion(new RemoveS3StreamRecord()
            .setStreamId(streamId), (short) 0));
        // generate stream objects destroy records
        List<Long> streamObjectIds = new ArrayList<>(streamMetadata.streamObjects().keySet());
        ControllerResult<Boolean> markDestroyResult = this.s3ObjectControlManager.markDestroyObjects(streamObjectIds);
        if (!markDestroyResult.response()) {
            log.error("[DELETE_STREAM],[FAIL]: failed to mark destroy stream objects. streamId={}, objects={}", streamId, streamObjectIds);
            resp.setErrorCode(Errors.STREAM_INNER_ERROR.code());
            return ControllerResult.of(Collections.emptyList(), resp);
        }
        records.addAll(markDestroyResult.records());
        // the data in stream set object will be removed by compaction
        if (resp.errorCode() != Errors.NONE.code()) {
            return ControllerResult.of(Collections.emptyList(), resp);
        }
        log.info("[DELETE_STREAM]: successfully delete the stream. streamId={}", streamId);
        return ControllerResult.atomicOf(records, resp);
    }

    /**
     * Commit stream set object.
     * <p>
     * <b>Response Errors Enum:</b>
     * <ul>
     *     <li>
     *         <code>OBJECT_NOT_EXIST</code>
     *         <ol>
     *             <li> stream set object not exist when commit </li>
     *             <li> stream object not exist when commit </li>
     *         </ol>
     *     </li>
     *     <li>
     *         <code>COMPACTED_OBJECTS_NOT_FOUND</code>
     *         <ol>
     *             <li> compacted objects not found when mark destroy </li>
     *         </ol>
     *     </li>
     * </ul>
     */
    @SuppressWarnings("all")
    public ControllerResult<CommitStreamSetObjectResponseData> commitStreamSetObject(
        CommitStreamSetObjectRequestData data) {
        CommitStreamSetObjectResponseData resp = new CommitStreamSetObjectResponseData();
        long objectId = data.objectId();
        int nodeId = data.nodeId();
        long nodeEpoch = data.nodeEpoch();
        long objectSize = data.objectSize();
        long orderId = data.orderId();

        // verify node epoch
        Errors nodeEpochCheckResult = nodeEpochCheck(nodeId, nodeEpoch, !data.failoverMode());
        if (nodeEpochCheckResult != Errors.NONE) {
            resp.setErrorCode(nodeEpochCheckResult.code());
            log.warn("[CommitStreamSetObject] invalid node epoch. streamSetObjectId={}, nodeId={}, nodeEpoch={}, error={}",
                objectId, nodeId, nodeEpoch, nodeEpochCheckResult);
            return ControllerResult.of(Collections.emptyList(), resp);
        }

        List<ObjectStreamRange> streamRanges = data.objectStreamRanges();
        List<Long> compactedObjectIds = data.compactedObjectIds();
        List<StreamObject> streamObjects = data.streamObjects();
        long committedTs = System.currentTimeMillis();

        // commit object
        ControllerResult<Errors> commitResult = this.s3ObjectControlManager.commitObject(objectId, objectSize, committedTs);
        if (commitResult.response() == Errors.OBJECT_NOT_EXIST) {
            log.error("[CommitStreamSetObject] stream set object id not exist. streamSetObjectId={}, nodeId={}, nodeEpoch={}", objectId, nodeId, nodeEpoch);
            resp.setErrorCode(Errors.OBJECT_NOT_EXIST.code());
            return ControllerResult.of(Collections.emptyList(), resp);
        }
        if (commitResult.response() == Errors.REDUNDANT_OPERATION) {
            // regard it as redundant commit operation, return success
            log.warn("[CommitStreamSetObject] stream set object already committed. streamSetObjectId={}, nodeId={}, nodeEpoch={}", objectId, nodeId, nodeEpoch);
            return ControllerResult.of(Collections.emptyList(), resp);
        }
        List<ApiMessageAndVersion> records = new ArrayList<>(commitResult.records());
        long dataTs = committedTs;
        // mark destroy compacted object
        if (compactedObjectIds != null && !compactedObjectIds.isEmpty()) {
            ControllerResult<Boolean> destroyResult = this.s3ObjectControlManager.markDestroyObjects(compactedObjectIds);
            if (!destroyResult.response()) {
                log.error("[CommitStreamSetObject]: failed to mark destroy compacted objects. compactedObjects={}, streamSetObjectId={}, nodeId={}, nodeEpoch={}",
                    compactedObjectIds, objectId, nodeId, nodeEpoch);
                resp.setErrorCode(Errors.COMPACTED_OBJECTS_NOT_FOUND.code());
                return ControllerResult.of(Collections.emptyList(), resp);
            }
            records.addAll(destroyResult.records());
            // update dataTs to the min compacted object's dataTs
            //noinspection OptionalGetWithoutIsPresent
            dataTs = compactedObjectIds.stream()
                .map(id -> this.nodesMetadata.get(nodeId).streamSetObjects().get(id))
                .map(S3StreamSetObject::dataTimeInMs)
                .min(Long::compareTo).get();
        }
        List<StreamOffsetRange> indexes = streamRanges.stream()
            .map(range -> new StreamOffsetRange(range.streamId(), range.startOffset(), range.endOffset()))
            .collect(Collectors.toList());
        if (objectId != NOOP_OBJECT_ID) {
            // generate node's stream set object record
            S3StreamSetObject s3StreamSetObject = new S3StreamSetObject(objectId, nodeId, indexes, orderId, dataTs);
            records.add(s3StreamSetObject.toRecord());
        }
        // commit stream objects
        if (streamObjects != null && !streamObjects.isEmpty()) {
            // commit objects
            for (StreamObject streamObject : streamObjects) {
                if (streamsMetadata.containsKey(streamObject.streamId())) {
                    ControllerResult<Errors> streamObjectCommitResult = this.s3ObjectControlManager.commitObject(streamObject.objectId(),
                        streamObject.objectSize(), committedTs);
                    if (streamObjectCommitResult.response() == Errors.REDUNDANT_OPERATION) {
                        // regard it as redundant commit operation, return success
                        log.warn("[CommitStreamSetObject]: stream object already committed. streamObjectId={}, streamSetObjectId={}, nodeId={}, nodeEpoch={}",
                            streamObject.objectId(), objectId, nodeId, nodeEpoch);
                        return ControllerResult.of(Collections.emptyList(), resp);
                    }
                    if (streamObjectCommitResult.response() != Errors.NONE) {
                        log.error("[CommitStreamSetObject]: failed to commit srteam object. streamObjectId={}, streamSetObjectId={}, nodeId={}, nodeEpoch={}, error={}",
                            streamObject.objectId(), objectId, nodeId, nodeEpoch, streamObjectCommitResult.response());
                        resp.setErrorCode(streamObjectCommitResult.response().code());
                        return ControllerResult.of(Collections.emptyList(), resp);
                    }
                    records.addAll(streamObjectCommitResult.records());
                    records.add(new S3StreamObject(streamObject.objectId(), streamObject.streamId(), streamObject.startOffset(), streamObject.endOffset(), committedTs).toRecord());
                } else {
                    log.info("stream already deleted, then fast delete the stream object from compaction. streamId={}, streamObject={}, streamSetObjectId={}, nodeId={}, nodeEpoch={}",
                        streamObject.streamId(), streamObject, objectId, nodeId, nodeEpoch);
                    ControllerResult<Boolean> deleteRst = this.s3ObjectControlManager.markDestroyObjects(List.of(streamObject.objectId()));
                    records.addAll(deleteRst.records());
                }
            }
        }
        // generate compacted objects' remove record
        if (compactedObjectIds != null && !compactedObjectIds.isEmpty()) {
            compactedObjectIds.forEach(id -> records.add(new ApiMessageAndVersion(new RemoveStreamSetObjectRecord()
                .setNodeId(nodeId)
                .setObjectId(id), (short) 0)));
        } else {
            // verify stream continuity
            List<StreamOffsetRange> offsetRanges = Stream.concat(
                    streamRanges
                        .stream()
                        .map(range -> new StreamOffsetRange(range.streamId(), range.startOffset(), range.endOffset())),
                    streamObjects
                        .stream()
                        .map(obj -> new StreamOffsetRange(obj.streamId(), obj.startOffset(), obj.endOffset())))
                .collect(Collectors.toList());
            Errors continuityCheckResult = streamAdvanceCheck(offsetRanges, data.nodeId());
            if (continuityCheckResult != Errors.NONE) {
                log.error("[CommitStreamSetObject] advance check failed. streamSetObjectId={}, nodeId={}, nodeEpoch={}, error={}",
                    objectId, nodeId, nodeEpoch, continuityCheckResult);
                resp.setErrorCode(continuityCheckResult.code());
                return ControllerResult.of(Collections.emptyList(), resp);
            }
        }
        logCommitStreamSetObject(data);
        return ControllerResult.atomicOf(records, resp);
    }

    /**
     * Commit stream object.
     * <p>
     * <b>Response Errors Enum:</b>
     * <ul>
     *     <li>
     *         <code>OBJECT_NOT_EXIST</code>
     *         <ol>
     *             <li> stream object not exist when commit </li>
     *         </ol>
     *     </li>
     *     <li>
     *         <code>COMPACTED_OBJECTS_NOT_FOUND</code>
     *         <ol>
     *             <li> compacted objects not found when mark destroy </li>
     *         </ol>
     *     </li>
     * </ul>
     */
    public ControllerResult<CommitStreamObjectResponseData> commitStreamObject(CommitStreamObjectRequestData data) {
        int nodeId = data.nodeId();
        long nodeEpoch = data.nodeEpoch();
        long streamObjectId = data.objectId();
        long streamId = data.streamId();
        long streamEpoch = data.streamEpoch();
        long startOffset = data.startOffset();
        long endOffset = data.endOffset();
        long objectSize = data.objectSize();
        List<Long> sourceObjectIds = data.sourceObjectIds();
        CommitStreamObjectResponseData resp = new CommitStreamObjectResponseData();
        long committedTs = System.currentTimeMillis();

        // verify node epoch
        Errors nodeEpochCheckResult = nodeEpochCheck(nodeId, nodeEpoch);
        if (nodeEpochCheckResult != Errors.NONE) {
            resp.setErrorCode(nodeEpochCheckResult.code());
            log.warn("[CommitStreamObject]: invalid node epoch. streamObjectId={}, nodeId={}, nodeEpoch={}, error={}, req={}",
                streamObjectId, nodeId, nodeEpoch, nodeEpochCheckResult, data);
            return ControllerResult.of(Collections.emptyList(), resp);
        }

        // skip outdated request
        if (streamEpoch != -1L) {
            // verify stream ownership
            Errors authResult = streamOwnershipCheck(streamId, streamEpoch, nodeId, "CommitStreamObject");
            if (authResult != Errors.NONE) {
                resp.setErrorCode(authResult.code());
                return ControllerResult.of(Collections.emptyList(), resp);
            }
        }

        // commit object
        ControllerResult<Errors> commitResult = this.s3ObjectControlManager.commitObject(streamObjectId, objectSize, committedTs);
        if (commitResult.response() == Errors.OBJECT_NOT_EXIST) {
            log.error("[CommitStreamObject]: stream object not exist. streamObjectId={}, req={}", streamObjectId, data);
            resp.setErrorCode(Errors.OBJECT_NOT_EXIST.code());
            return ControllerResult.of(Collections.emptyList(), resp);
        }
        if (commitResult.response() == Errors.REDUNDANT_OPERATION) {
            // regard it as a redundant commit operation, return success
            log.warn("[CommitStreamObject]: stream object already committed. streamObjectId={}, req={}", streamObjectId, data);
            return ControllerResult.of(Collections.emptyList(), resp);
        }
        List<ApiMessageAndVersion> records = new ArrayList<>(commitResult.records());

        long dataTs = committedTs;
        // mark destroy compacted object
        if (sourceObjectIds != null && !sourceObjectIds.isEmpty()) {
            ControllerResult<Boolean> destroyResult = this.s3ObjectControlManager.markDestroyObjects(sourceObjectIds);
            if (!destroyResult.response()) {
                log.error("[CommitStreamObject]: failed to mark destroy compacted objects. compactedObjects={}, req={}",
                    sourceObjectIds, data);
                resp.setErrorCode(Errors.COMPACTED_OBJECTS_NOT_FOUND.code());
                return ControllerResult.of(Collections.emptyList(), resp);
            }
            records.addAll(destroyResult.records());
            // update dataTs to the min compacted object's dataTs
            //noinspection OptionalGetWithoutIsPresent
            dataTs = sourceObjectIds.stream()
                .map(id -> this.streamsMetadata.get(streamId).streamObjects().get(id))
                .map(S3StreamObject::dataTimeInMs)
                .min(Long::compareTo).get();
        }

        if (streamObjectId != NOOP_OBJECT_ID) {
            // generate stream object record
            records.add(new ApiMessageAndVersion(new S3StreamObjectRecord()
                .setObjectId(streamObjectId)
                .setStreamId(streamId)
                .setStartOffset(startOffset)
                .setEndOffset(endOffset)
                .setDataTimeInMs(dataTs), (short) 0));
        }

        // generate compacted objects' remove record
        if (sourceObjectIds != null && !sourceObjectIds.isEmpty()) {
            sourceObjectIds.forEach(id -> records.add(new ApiMessageAndVersion(new RemoveS3StreamObjectRecord()
                .setObjectId(id)
                .setStreamId(streamId), (short) 0)));
        }
        log.info("[CommitStreamObject]: successfully commit stream object. streamObjectId={}, streamId={}, streamEpoch={}, nodeId={}, nodeEpoch={}, compactedObjects={}",
            streamObjectId, streamId, streamEpoch, nodeId, nodeEpoch, sourceObjectIds);
        return ControllerResult.atomicOf(records, resp);
    }

    private DescribeStreamsResponseData bulidDescribeStreamsResponseData(List<S3StreamMetadata> s3StreamMetadataList) {
        List<DescribeStreamsResponseData.StreamMetadata> metadataList = s3StreamMetadataList.stream()
            .map(streamMetadata -> {
                List<DescribeStreamsResponseData.Tag> tagList = streamMetadata.tags().entrySet().stream()
                    .map(entry -> {
                        DescribeStreamsResponseData.Tag tag = new DescribeStreamsResponseData.Tag();
                        tag.setKey(entry.getKey());
                        tag.setValue(entry.getValue());
                        return tag;
                    })
                    .collect(Collectors.toList());

                int nodeId = -1;
                long endOffset = -1;
                if (streamMetadata.currentRangeIndex() >= 0) {
                    RangeMetadata rangeMetadata = streamMetadata.ranges().get(streamMetadata.currentRangeIndex());
                    nodeId = rangeMetadata.nodeId();
                    endOffset = rangeMetadata.endOffset();
                }

                Uuid topicId = Uuid.ZERO_UUID;
                String topicName = "";
                if (streamMetadata.tags().containsKey(StreamTags.Topic.KEY)) {
                    topicId = StreamTags.Topic.decode(streamMetadata.tags().get(StreamTags.Topic.KEY));
                    ReplicationControlManager.TopicControlInfo topicInfo = replicationControlManager.getTopic(topicId);
                    if (topicInfo != null) {
                        topicName = topicInfo.name();
                    }
                }
                int partition = -1;
                if (streamMetadata.tags().containsKey(StreamTags.Partition.KEY)) {
                    partition = StreamTags.Partition.decode(streamMetadata.tags().get(StreamTags.Partition.KEY));
                }

                return new DescribeStreamsResponseData.StreamMetadata()
                    .setStreamId(streamMetadata.streamId())
                    .setNodeId(nodeId)
                    .setState(streamMetadata.currentState().name())
                    .setTopicId(topicId)
                    .setTopicName(topicName)
                    .setPartitionIndex(partition)
                    .setEpoch(streamMetadata.currentEpoch())
                    .setStartOffset(streamMetadata.startOffset())
                    .setEndOffset(endOffset)
                    .setTags(new DescribeStreamsResponseData.TagCollection(tagList.iterator()));
            }).collect(Collectors.toList());

        DescribeStreamsResponseData data = new DescribeStreamsResponseData();
        data.setStreamMetadataList(metadataList);
        return data;
    }

    public DescribeStreamsResponseData describeStreams(DescribeStreamsRequestData data) {
        long streamId = data.streamId();
        if (streamId >= 0) {
            S3StreamMetadata metadata = streamsMetadata.get(streamId);
            if (metadata == null) {
                return bulidDescribeStreamsResponseData(Collections.emptyList());
            }
            return bulidDescribeStreamsResponseData(List.of(metadata));
        }

        int nodeId = data.nodeId();
        if (nodeId >= 0) {
            List<S3StreamMetadata> metadataList = streamsMetadata.values().stream()
                .filter(metadata -> {
                    int rangeIndex = metadata.currentRangeIndex();
                    if (rangeIndex < 0) {
                        return false;
                    }
                    RangeMetadata rangeMetadata = metadata.ranges().get(rangeIndex);
                    return rangeMetadata.nodeId() == nodeId;
                })
                .collect(Collectors.toList());
            return bulidDescribeStreamsResponseData(metadataList);
        }

        List<DescribeStreamsRequestData.TopicPartitionData> topicPartitionDataList = data.topicPartitions();
        if (topicPartitionDataList.isEmpty()) {
            // No stream id, node id and topic partition data, return invalid request
            DescribeStreamsResponseData response = new DescribeStreamsResponseData();
            response.setErrorCode(Errors.INVALID_REQUEST.code());
            return response;
        }

        Map<String, Set<Integer>> topicPartitionMap = topicPartitionDataList.stream()
            .map(topicData -> {
                String topicName = topicData.topicName();
                Set<Integer> partitions = topicData.partitions()
                    .stream()
                    .mapToInt(DescribeStreamsRequestData.PartitionData::partitionIndex)
                    .boxed()
                    .collect(Collectors.toSet());
                return Map.entry(topicName, partitions);
            })
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        List<S3StreamMetadata> metadataList = streamsMetadata.values().stream()
            .filter(metadata -> {
                if (!metadata.tags().containsKey(StreamTags.Topic.KEY) || !metadata.tags().containsKey(StreamTags.Partition.KEY)) {
                    return false;
                }

                Uuid topicId = StreamTags.Topic.decode(metadata.tags().get(StreamTags.Topic.KEY));
                ReplicationControlManager.TopicControlInfo topicInfo = replicationControlManager.getTopic(topicId);
                if (topicInfo == null) {
                    return false;
                }
                String topicName = topicInfo.name();
                int partition = StreamTags.Partition.decode(metadata.tags().get(StreamTags.Partition.KEY));

                if (topicPartitionMap.containsKey(topicName)) {
                    Set<Integer> partitionSet = topicPartitionMap.get(topicName);
                    return partitionSet.isEmpty() || partitionSet.contains(partition);
                }

                return false;
            })
            .collect(Collectors.toList());
        return bulidDescribeStreamsResponseData(metadataList);
    }

    public ControllerResult<GetOpeningStreamsResponseData> getOpeningStreams(GetOpeningStreamsRequestData data) {
        GetOpeningStreamsResponseData resp = new GetOpeningStreamsResponseData();
        int nodeId = data.nodeId();
        long nodeEpoch = data.nodeEpoch();
        boolean failoverMode = data.failoverMode();

        List<ApiMessageAndVersion> records = new ArrayList<>();

        NodeMetadata nodeMetadata = this.nodesMetadata.get(nodeId);
        if (nodeMetadata == null) {
            // create a new node metadata if absent
            records.add(new ApiMessageAndVersion(
                new NodeWALMetadataRecord().setNodeId(nodeId).setNodeEpoch(nodeEpoch).setFailoverMode(failoverMode),
                (short) 0));
        }

        // verify and update node epoch
        if (nodeMetadata != null && nodeEpoch < nodeMetadata.getNodeEpoch()) {
            // node epoch has been expired
            resp.setErrorCode(Errors.NODE_EPOCH_EXPIRED.code());
            log.warn("[GetOpeningStreams]: expired node epoch. nodeId={}, nodeEpoch={}", nodeId, nodeEpoch);
            return ControllerResult.of(Collections.emptyList(), resp);
        }

        if (nodeMetadata != null) {
            // update node epoch
            records.add(new ApiMessageAndVersion(
                new NodeWALMetadataRecord().setNodeId(nodeId).setNodeEpoch(nodeEpoch).setFailoverMode(failoverMode),
                (short) 0));
        }

        // The getOpeningStreams is invoked when node startup, so we just iterate all streams to get the node opening streams.
        List<StreamMetadata> streamStatusList = this.streamsMetadata.entrySet().stream().filter(entry -> {
            S3StreamMetadata streamMetadata = entry.getValue();
            if (!StreamState.OPENED.equals(streamMetadata.currentState())) {
                return false;
            }
            int rangeIndex = streamMetadata.currentRangeIndex();
            if (rangeIndex < 0) {
                return false;
            }
            RangeMetadata rangeMetadata = streamMetadata.ranges().get(rangeIndex);
            return rangeMetadata.nodeId() == nodeId;
        }).map(e -> {
            S3StreamMetadata streamMetadata = e.getValue();
            RangeMetadata rangeMetadata = streamMetadata.ranges().get(streamMetadata.currentRangeIndex());
            return new StreamMetadata()
                .setStreamId(e.getKey())
                .setEpoch(streamMetadata.currentEpoch())
                .setStartOffset(streamMetadata.startOffset())
                // Fix https://github.com/AutoMQ/automq/issues/1222#issuecomment-2132812938
                .setEndOffset(Math.max(rangeMetadata.endOffset(), streamMetadata.startOffset()));
        }).collect(Collectors.toList());
        resp.setStreamMetadataList(streamStatusList);
        return ControllerResult.atomicOf(records, resp);
    }

    // private check methods

    /**
     * Check whether this node is the owner of this stream.
     */
    private Errors streamOwnershipCheck(long streamId, long epoch, int nodeId, String operationName) {
        if (!this.streamsMetadata.containsKey(streamId)) {
            log.warn("[{}]: streamId={} not exist", operationName, streamId);
            return Errors.STREAM_NOT_EXIST;
        }
        S3StreamMetadata streamMetadata = this.streamsMetadata.get(streamId);
        if (streamMetadata.currentEpoch() > epoch) {
            log.warn("[{}]: streamId={}'s epoch={} is larger than request epoch={}", operationName, streamId,
                streamMetadata.currentEpoch(), epoch);
            return Errors.STREAM_FENCED;
        }
        if (streamMetadata.currentEpoch() < epoch) {
            // should not happen
            log.error("[{}]: streamId={}'s epoch={} is smaller than request epoch={}", operationName, streamId,
                streamMetadata.currentEpoch(), epoch);
            return Errors.STREAM_INNER_ERROR;
        }
        // verify node
        RangeMetadata rangeMetadata = streamMetadata.ranges().get(streamMetadata.currentRangeIndex());
        if (rangeMetadata == null) {
            // should not happen
            log.error("[{}]: streamId={}'s current range={} not exist when trim stream with epoch={}", operationName, streamId,
                streamMetadata.currentRangeIndex(), epoch);
            return Errors.STREAM_INNER_ERROR;
        }
        if (rangeMetadata.nodeId() != nodeId) {
            log.warn("[{}]: streamId={}'s current range={}'s nodeId={} is not equal to request nodeId={}", operationName,
                streamId, streamMetadata.currentRangeIndex(), rangeMetadata.nodeId(), nodeId);
            return Errors.STREAM_FENCED;
        }
        return Errors.NONE;
    }

    /**
     * Check whether these stream-offset-ranges can be committed to advance the streams' offset
     * <p>
     * <b>Check List:</b>
     * <ul>
     *      <li>
     *          <code>Stream Exist Check</code>
     *      </li>
     *      <li>
     *          <code>Stream Open Check</code>
     *      </li>
     *      <li>
     *          <code>Stream Continuity Check</code>
     *      </li>
     * <ul/>
     */
    private Errors streamAdvanceCheck(List<StreamOffsetRange> ranges, int nodeId) {
        if (ranges == null || ranges.isEmpty()) {
            return Errors.NONE;
        }
        for (StreamOffsetRange range : ranges) {
            // verify stream exist
            S3StreamMetadata streamMetadata = this.streamsMetadata.get(range.streamId());
            if (streamMetadata == null) {
                log.warn("[streamAdvanceCheck]: streamId={} not exist", range.streamId());
                return Errors.STREAM_NOT_EXIST;
            }
            // check if this stream open
            if (streamMetadata.currentState() != StreamState.OPENED) {
                log.warn("[streamAdvanceCheck]: streamId={} not opened", range.streamId());
                return Errors.STREAM_NOT_OPENED;
            }
            RangeMetadata rangeMetadata = streamMetadata.currentRangeMetadata();
            if (rangeMetadata == null) {
                // should not happen
                log.error("[streamAdvanceCheck]: streamId={}'s current range={} not exist when stream has been ",
                    range.streamId(), this.streamsMetadata.get(range.streamId()).currentRangeIndex());
                return Errors.STREAM_INNER_ERROR;
            } else if (rangeMetadata.nodeId() != nodeId) {
                // should not happen
                log.error("[streamAdvanceCheck]: streamId={}'s current range node id not match expected nodeId={} but nodeId={}",
                    range.streamId(), rangeMetadata.nodeId(), nodeId);
                return Errors.STREAM_INNER_ERROR;
            }
            if (!(rangeMetadata.endOffset() == range.startOffset() || range.startOffset() <= streamMetadata.startOffset())) {
                // Fix https://github.com/AutoMQ/automq/issues/1222#issuecomment-2132812938
                log.warn("[streamAdvanceCheck]: streamId={}'s current range={}'s end offset {} is not equal to request start offset {}, stream's startOffset={}",
                    range.streamId(), this.streamsMetadata.get(range.streamId()).currentRangeIndex(),
                    rangeMetadata.endOffset(), range.startOffset(), streamMetadata.startOffset());
                return Errors.OFFSET_NOT_MATCHED;
            }
        }
        return Errors.NONE;
    }

    private Errors nodeEpochCheck(int nodeId, long nodeEpoch) {
        return nodeEpochCheck(nodeId, nodeEpoch, true);
    }

    /**
     * Check whether this node is valid to operate the stream related resources.
     */
    private Errors nodeEpochCheck(int nodeId, long nodeEpoch, boolean checkFailover) {
        NodeMetadata nodeMetadata = this.nodesMetadata.get(nodeId);
        if (nodeMetadata == null) {
            // should not happen
            log.error("[NodeEpochCheck]: nodeId={} not exist when check node epoch", nodeId);
            return Errors.NODE_EPOCH_NOT_EXIST;
        }
        if (nodeMetadata.getNodeEpoch() > nodeEpoch) {
            log.warn("[NodeEpochCheck]: nodeId={}'s epoch={} is larger than request epoch={}", nodeId,
                this.nodesMetadata.get(nodeId).getNodeEpoch(), nodeEpoch);
            return Errors.NODE_EPOCH_EXPIRED;
        }
        if (checkFailover && nodeMetadata.getFailoverMode()) {
            log.warn("[NodeEpochCheck]: nodeId={} epoch={} is fenced", nodeId, nodeEpoch);
            return Errors.NODE_FENCED;
        }
        return Errors.NONE;
    }

    public void triggerCleanupScaleInNodes() {
        if (!quorumController.isActive()) {
            return;
        }
        quorumController.appendWriteEvent("cleanupScaleInNodes", OptionalLong.empty(), this::cleanupScaleInNodes);
    }

    public ControllerResult<Void> cleanupScaleInNodes() {
        List<ApiMessageAndVersion> records = new LinkedList<>();
        List<S3StreamSetObject> cleanupObjects = new LinkedList<>();
        nodesMetadata.forEach((nodeId, nodeMetadata) -> {
            if (!clusterControlManager.isActive(nodeId)) {
                Collection<S3StreamSetObject> objects = nodeMetadata.streamSetObjects().values();
                boolean alive = false;
                for (S3StreamSetObject object : objects) {
                    if (alive) {
                        // if the last object is not expired, the likelihood of the subsequent object expiring is also quite low.
                        break;
                    }
                    long objectId = object.objectId();
                    AtomicBoolean expired = new AtomicBoolean(true);
                    List<StreamOffsetRange> streamOffsetRanges = object.offsetRangeList();
                    for (StreamOffsetRange streamOffsetRange : streamOffsetRanges) {
                        S3StreamMetadata stream = streamsMetadata.get(streamOffsetRange.streamId());
                        if (stream != null && stream.startOffset() < streamOffsetRange.endOffset()) {
                            expired.set(false);
                            alive = true;
                        }
                    }
                    if (expired.get()) {
                        cleanupObjects.add(object);
                        records.add(new ApiMessageAndVersion(new RemoveStreamSetObjectRecord()
                            .setNodeId(nodeId)
                            .setObjectId(objectId), (short) 0));
                        records.addAll(this.s3ObjectControlManager.markDestroyObjects(List.of(objectId)).records());
                    }
                }
            }
        });
        if (!cleanupObjects.isEmpty()) {
            LOGGER.info("clean up scaled-in nodes objects: {}", cleanupObjects);
        } else {
            LOGGER.debug("clean up scaled-in nodes objects: []");
        }
        return ControllerResult.of(records, null);
    }

    public void replay(AssignedStreamIdRecord record) {
        this.nextAssignedStreamId.set(record.assignedStreamId() + 1);
    }

    public void replay(S3StreamRecord record) {
        long streamId = record.streamId();
        // already exist, update the stream's metadata
        if (this.streamsMetadata.containsKey(streamId)) {
            S3StreamMetadata streamMetadata = this.streamsMetadata.get(streamId);
            streamMetadata.startOffset(record.startOffset());
            streamMetadata.currentEpoch(record.epoch());
            streamMetadata.currentRangeIndex(record.rangeIndex());
            streamMetadata.currentState(StreamState.fromByte(record.streamState()));
            if (streamMetadata.tags().isEmpty() && record.tags().size() > 0) {
                Map<String, String> tags = new HashMap<>();
                record.tags().forEach(tag -> tags.put(tag.key(), tag.value()));
                streamMetadata.setTags(tags);
            }
            return;
        }
        Map<String, String> tags = new HashMap<>();
        record.tags().forEach(tag -> tags.put(tag.key(), tag.value()));
        // not exist, create a new stream
        S3StreamMetadata streamMetadata = new S3StreamMetadata(record.streamId(), record.epoch(), record.rangeIndex(),
            record.startOffset(), StreamState.fromByte(record.streamState()), tags, this.snapshotRegistry);
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
            log.error("streamId={} not exist when replay range record {}", streamId, record);
            return;
        }
        streamMetadata.ranges().put(record.rangeIndex(), RangeMetadata.of(record));
    }

    public void replay(RemoveRangeRecord record) {
        long streamId = record.streamId();
        S3StreamMetadata streamMetadata = this.streamsMetadata.get(streamId);
        if (streamMetadata == null) {
            // should not happen
            log.error("streamId={} not exist when replay remove range record {}", streamId, record);
            return;
        }
        streamMetadata.ranges().remove(record.rangeIndex());
    }

    public void replay(NodeWALMetadataRecord record) {
        int nodeId = record.nodeId();
        long nodeEpoch = record.nodeEpoch();
        // already exist, update the node's metadata
        if (this.nodesMetadata.containsKey(nodeId)) {
            NodeMetadata nodeMetadata = this.nodesMetadata.get(nodeId);
            nodeMetadata.setNodeEpoch(nodeEpoch);
            nodeMetadata.setFailoverMode(record.failoverMode());
            return;
        }
        // not exist, create a new node
        this.nodesMetadata.put(nodeId, new NodeMetadata(nodeId, nodeEpoch, record.failoverMode(), this.snapshotRegistry));
    }

    public void replay(S3StreamSetObjectRecord record) {
        long objectId = record.objectId();
        int nodeId = record.nodeId();
        long orderId = record.orderId();
        long dataTs = record.dataTimeInMs();
        NodeMetadata nodeMetadata = this.nodesMetadata.get(nodeId);
        if (nodeMetadata == null) {
            // should not happen
            log.error("nodeId={} not exist when replay stream set object record {}", nodeId, record);
            return;
        }
        S3StreamSetObject s3StreamSetObject = new S3StreamSetObject(objectId, nodeId, record.ranges(), orderId, dataTs);
        nodeMetadata.streamSetObjects().put(objectId, s3StreamSetObject);

        // update range
        s3StreamSetObject.offsetRangeList().forEach(index -> {
            long streamId = index.streamId();
            S3StreamMetadata metadata = this.streamsMetadata.get(streamId);
            if (metadata == null) {
                // ignore it, the stream may be deleted
                return;
            }
            // the offset continuous is ensured by the process layer
            // when replay from checkpoint, the record may be out of order, so we need to update the end offset to the largest end offset.
            metadata.updateEndOffset(index.endOffset());
        });
    }

    public void replay(RemoveStreamSetObjectRecord record) {
        long objectId = record.objectId();
        NodeMetadata walMetadata = this.nodesMetadata.get(record.nodeId());
        if (walMetadata == null) {
            // should not happen
            log.error("node {} not exist when replay remove stream set object record {}", record.nodeId(), record);
            return;
        }
        walMetadata.streamSetObjects().remove(objectId);
    }

    public void replay(S3StreamObjectRecord record) {
        long objectId = record.objectId();
        long streamId = record.streamId();
        long startOffset = record.startOffset();
        long endOffset = record.endOffset();
        long dataTs = record.dataTimeInMs();

        S3StreamMetadata streamMetadata = this.streamsMetadata.get(streamId);
        if (streamMetadata == null) {
            // should not happen
            log.error("streamId={} not exist when replay stream object record {}", streamId, record);
            return;
        }
        streamMetadata.streamObjects().put(objectId, new S3StreamObject(objectId, streamId, startOffset, endOffset, dataTs));
        // the offset continuous is ensured by the process layer
        // when replay from checkpoint, the record may be out of order, so we need to update the end offset to the largest end offset.
        streamMetadata.updateEndOffset(endOffset);
    }

    public void replay(RemoveS3StreamObjectRecord record) {
        long streamId = record.streamId();
        long objectId = record.objectId();
        S3StreamMetadata streamMetadata = this.streamsMetadata.get(streamId);
        if (streamMetadata == null) {
            // should not happen
            log.error("streamId={} not exist when replay remove stream object record {}", streamId, record);
            return;
        }
        streamMetadata.streamObjects().remove(objectId);
    }

    public void replay(RemoveNodeWALMetadataRecord record) {
        int nodeId = record.nodeId();
        this.nodesMetadata.remove(nodeId);
    }

    public TimelineHashMap<Long, S3StreamMetadata> streamsMetadata() {
        return streamsMetadata;
    }

    public Map<Integer, NodeMetadata> nodesMetadata() {
        return nodesMetadata;
    }

    public Long nextAssignedStreamId() {
        return nextAssignedStreamId.get();
    }

    @Override
    public String toString() {
        return "StreamControlManager{" +
               "snapshotRegistry=" + snapshotRegistry +
               ", s3ObjectControlManager=" + s3ObjectControlManager +
               ", streamsMetadata=" + streamsMetadata +
               ", nodesMetadata=" + nodesMetadata +
               '}';
    }

    private void logCommitStreamSetObject(CommitStreamSetObjectRequestData req) {
        if (!log.isInfoEnabled()) {
            return;
        }
        StringBuilder sb = new StringBuilder();
        sb.append("[CommitStreamSetObject]: successfully commit stream set object, ");
        sb.append("streamSetObjectId=").append(req.objectId()).append(", nodeId=").append(req.nodeId());
        sb.append(", nodeEpoch=").append(req.nodeEpoch()).append(", compactedObjects=").append(req.compactedObjectIds());
        sb.append(", \n\tstreamRanges=");
        req.objectStreamRanges().forEach(range -> {
            sb.append("(si=").append(range.streamId()).append(", so=").append(range.startOffset()).append(", eo=")
                .append(range.endOffset()).append("), ");
        });
        sb.append(", \n\tstreamObjects=");
        req.streamObjects().forEach(obj -> {
            sb.append("(si=").append(obj.streamId()).append(", so=").append(obj.startOffset()).append(", eo=")
                .append(obj.endOffset()).append(", oi=").append(obj.objectId()).append("), ");
        });
        log.info(sb.toString());
    }

}
