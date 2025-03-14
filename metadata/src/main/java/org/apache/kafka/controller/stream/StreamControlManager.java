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
import org.apache.kafka.common.metadata.S3StreamEndOffsetsRecord;
import org.apache.kafka.common.metadata.S3StreamObjectRecord;
import org.apache.kafka.common.metadata.S3StreamRecord;
import org.apache.kafka.common.metadata.S3StreamSetObjectRecord;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.controller.ClusterControlManager;
import org.apache.kafka.controller.ControllerResult;
import org.apache.kafka.controller.FeatureControlManager;
import org.apache.kafka.controller.QuorumController;
import org.apache.kafka.controller.ReplicationControlManager;
import org.apache.kafka.image.DeltaList;
import org.apache.kafka.metadata.stream.RangeMetadata;
import org.apache.kafka.metadata.stream.S3StreamEndOffsetsCodec;
import org.apache.kafka.metadata.stream.S3StreamObject;
import org.apache.kafka.metadata.stream.S3StreamSetObject;
import org.apache.kafka.metadata.stream.StreamEndOffset;
import org.apache.kafka.metadata.stream.StreamTags;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.automq.AutoMQVersion;
import org.apache.kafka.server.metrics.s3stream.S3StreamKafkaMetricsManager;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.apache.kafka.timeline.TimelineLong;

import com.automq.stream.s3.ObjectReader;
import com.automq.stream.s3.compact.CompactOperations;
import com.automq.stream.s3.metadata.S3StreamConstant;
import com.automq.stream.s3.metadata.StreamOffsetRange;
import com.automq.stream.s3.metadata.StreamState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.automq.stream.s3.metadata.ObjectUtils.NOOP_OBJECT_ID;

/**
 * The StreamControlManager manages all Stream's lifecycle, such as create, open, delete, etc.
 */
@SuppressWarnings({"all", "this-escape"})
public class StreamControlManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamControlManager.class);

    private final Logger log;

    /**
     * The next stream id to be assigned.
     */
    private final TimelineLong nextAssignedStreamId;

    private final TimelineHashMap<Long/*streamId*/, StreamRuntimeMetadata> streamsMetadata;

    private final TimelineHashMap<Integer/*nodeId*/, NodeRuntimeMetadata> nodesMetadata;

    private final TimelineHashMap<Long, Integer> stream2node;
    private final TimelineHashMap<Integer/* nodeId */, /* streams */DeltaList<Long>> node2streams;

    private Set<Integer> cleaningUpNodes = Collections.newSetFromMap(new ConcurrentHashMap<>());

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
        this.streamsMetadata = new TimelineHashMap<>(snapshotRegistry, 100000);
        this.nodesMetadata = new TimelineHashMap<>(snapshotRegistry, 0);
        this.stream2node = new TimelineHashMap<>(snapshotRegistry, 100000);
        this.node2streams = new TimelineHashMap<>(snapshotRegistry, 100);

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
            if (!quorumController.isActive()) {
                return numMap;
            }
            for (NodeRuntimeMetadata nodeRuntimeMetadata : nodesMetadata.values()) {
                numMap.put(String.valueOf(nodeRuntimeMetadata.getNodeId()), nodeRuntimeMetadata.streamSetObjects().size());
            }
            return numMap;
        });
        S3StreamKafkaMetricsManager.setStreamObjectNumSupplier(() -> {
            if (!quorumController.isActive()) {
                return 0;
            }
            return streamsMetadata.values().stream().mapToInt(it -> it.streamObjects().size()).sum();
        });
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
        StreamRuntimeMetadata streamMetadata = this.streamsMetadata.get(streamId);
        if (streamMetadata.currentEpoch() > epoch) {
            resp.setErrorCode(Errors.STREAM_FENCED.code());
            log.warn("[OpenStream] steam has been fenced. streamId={}, streamEpoch={}, requestEpoch={}, nodeId={}, nodeEpoch={}",
                streamId, streamMetadata.currentEpoch(), epoch, nodeId, nodeEpoch);
            return ControllerResult.of(Collections.emptyList(), resp);
        }
        if (streamMetadata.currentEpoch() == epoch) {
            return retryOpen(streamMetadata, nodeId, nodeEpoch, request, resp, version);
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
        long nextRangeStartOffset = 0;
        if (newRangeIndex > 0) {
            // means that the new range is not the first range in stream, get the last range's end offset
            RangeMetadata lastRangeMetadata = streamMetadata.ranges().get(streamMetadata.currentRangeIndex());
            // the RangeMetadata in S3StreamMetadataImage is only update when create, rollToNext and trim
            records.add(new ApiMessageAndVersion(new RangeRecord()
                .setStreamId(streamId)
                .setNodeId(lastRangeMetadata.nodeId())
                .setStartOffset(lastRangeMetadata.startOffset())
                .setEndOffset(streamMetadata.endOffset())
                .setEpoch(lastRangeMetadata.epoch())
                .setRangeIndex(lastRangeMetadata.rangeIndex()), (short) 0));
            nextRangeStartOffset = streamMetadata.endOffset();
        }
        // Fix https://github.com/AutoMQ/automq/issues/1222
        // Handle the case: trim offset beyond the range end offset
        nextRangeStartOffset = Math.max(streamMetadata.startOffset(), nextRangeStartOffset);
        // range create record
        records.add(new ApiMessageAndVersion(new RangeRecord()
            .setStreamId(streamId)
            .setNodeId(nodeId)
            .setStartOffset(nextRangeStartOffset)
            .setEndOffset(nextRangeStartOffset)
            .setEpoch(epoch)
            .setRangeIndex(newRangeIndex), (short) 0));
        resp.setStartOffset(streamMetadata.startOffset());
        resp.setNextOffset(nextRangeStartOffset);

        for (RangeMetadata removableRange : streamMetadata.checkRemovableRanges()) {
            records.add(new ApiMessageAndVersion(new RemoveRangeRecord()
                .setStreamId(streamId)
                .setRangeIndex(removableRange.rangeIndex()), (short) 0));
        }

        log.info("[OpenStream] successfully open the stream. streamId={}, streamEpoch={}, nodeId={}, nodeEpoch={}",
            streamId, epoch, nodeId, nodeEpoch);
        return ControllerResult.atomicOf(records, resp);
    }

    private ControllerResult<OpenStreamResponse> retryOpen(StreamRuntimeMetadata streamMetadata, long nodeId,
        long nodeEpoch,
        OpenStreamRequest req, OpenStreamResponse resp, AutoMQVersion version) {
        long streamId = req.streamId();
        long epoch = req.streamEpoch();
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
        // Fix https://github.com/AutoMQ/automq/issues/1222
        // Handle the case: trim offset beyond the end offset
        resp.setNextOffset(Math.max(streamMetadata.startOffset(), streamMetadata.endOffset()));
        List<ApiMessageAndVersion> records = new ArrayList<>();
        if (streamMetadata.currentState() == StreamState.CLOSED) {
            records.add(new ApiMessageAndVersion(new S3StreamRecord()
                .setStreamId(streamMetadata.streamId())
                .setEpoch(epoch)
                .setRangeIndex(streamMetadata.currentRangeIndex())
                .setStartOffset(streamMetadata.startOffset())
                .setStreamState(StreamState.OPENED.toByte()), version.streamRecordVersion()));
        }
        return ControllerResult.of(records, resp);
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
        StreamRuntimeMetadata streamMetadata = this.streamsMetadata.get(streamId);
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
        StreamRuntimeMetadata streamMetadata = this.streamsMetadata.get(streamId);
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
                long newRangeStartOffset = Math.min(newStartOffset, streamMetadata.endOffset());
                records.add(new ApiMessageAndVersion(new RangeRecord()
                    .setStreamId(streamId)
                    .setRangeIndex(rangeIndex)
                    .setNodeId(range.nodeId())
                    .setEpoch(range.epoch())
                    .setStartOffset(newRangeStartOffset)
                    .setEndOffset(streamMetadata.endOffset()), (short) 0));
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
        return ControllerResult.atomicOf(records, resp);
    }

    public ControllerResult<DeleteStreamResponse> deleteStream(DeleteStreamRequest request) {
        DeleteStreamResponse resp = new DeleteStreamResponse();

        long streamId = request.streamId();

        StreamRuntimeMetadata streamMetadata = this.streamsMetadata.get(streamId);
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
        // deep delete the composite object: delete the composite object and it's linked objects
        ControllerResult<Boolean> markDestroyResult = this.s3ObjectControlManager.markDestroyObjects(streamObjectIds, Collections.nCopies(streamObjectIds.size(), CompactOperations.DEEP_DELETE));
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

        AutoMQVersion version = featureControlManager.autoMQVersion();
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
        ControllerResult<Errors> commitResult = this.s3ObjectControlManager.commitObject(objectId, objectSize, committedTs, data.attributes());
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
        if (!compactedObjectIds.isEmpty()) {
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
        if (objectId != NOOP_OBJECT_ID) {
            // generate node's stream set object record
            S3StreamSetObject s3StreamSetObject;
            if (version.isHugeClusterSupported()) {
                s3StreamSetObject = new S3StreamSetObject(objectId, nodeId, Bytes.EMPTY, orderId, dataTs);
                records.add(s3StreamSetObject.toRecord(version));
            } else {
                List<StreamOffsetRange> indexes = streamRanges.stream()
                    .map(range -> new StreamOffsetRange(range.streamId(), range.startOffset(), range.endOffset()))
                    .collect(Collectors.toList());
                s3StreamSetObject = new S3StreamSetObject(objectId, nodeId, indexes, orderId, dataTs);
                records.add(s3StreamSetObject.toRecord(version));
            }
        }
        if (compactedObjectIds.isEmpty() && version.isHugeClusterSupported()) {
            // generate S3StreamEndOffsetsRecord to move stream endOffset
            S3StreamEndOffsetsRecord record = new S3StreamEndOffsetsRecord().setEndOffsets(
                S3StreamEndOffsetsCodec.encode(
                    Stream.concat(
                            streamRanges.stream().map(s -> new StreamEndOffset(s.streamId(), s.endOffset())),
                            streamObjects.stream().map(s -> new StreamEndOffset(s.streamId(), s.endOffset()))
                        )
                        .collect(Collectors.toList()))
            );
            records.add(new ApiMessageAndVersion(record, (short) 0));
        }
        // commit stream objects
        if (!streamObjects.isEmpty()) {
            // commit objects
            ControllerResult<CommitStreamSetObjectResponseData> ret = generateStreamObject(streamObjects, records, data, resp, committedTs);
            if (ret != null) {
                return ret;
            }
        }
        // generate compacted objects' remove record
        if (!compactedObjectIds.isEmpty()) {
            compactedObjectIds.forEach(id -> records.add(new ApiMessageAndVersion(new RemoveStreamSetObjectRecord()
                .setNodeId(nodeId)
                .setObjectId(id), (short) 0)));
        } else {
            // verify stream continuity
            ControllerResult<CommitStreamSetObjectResponseData> ret = verifyStreamContinuous(streamRanges, streamObjects, data, resp);
            if (ret != null) {
                return ret;
            }
        }
        logCommitStreamSetObject(data);
        return ControllerResult.atomicOf(records, resp);
    }

    private ControllerResult<CommitStreamSetObjectResponseData> generateStreamObject(List<StreamObject> streamObjects,
        List<ApiMessageAndVersion> records,
        CommitStreamSetObjectRequestData req, CommitStreamSetObjectResponseData resp, long committedTs) {
        for (StreamObject streamObject : streamObjects) {
            if (streamsMetadata.containsKey(streamObject.streamId())) {
                ControllerResult<Errors> streamObjectCommitResult = this.s3ObjectControlManager.commitObject(streamObject.objectId(),
                    streamObject.objectSize(), committedTs, streamObject.attributes());
                if (streamObjectCommitResult.response() == Errors.REDUNDANT_OPERATION) {
                    // regard it as redundant commit operation, return success
                    log.warn("[CommitStreamSetObject]: stream object already committed. streamObjectId={}, streamSetObjectId={}, nodeId={}, nodeEpoch={}",
                        streamObject.objectId(), req.objectId(), req.nodeId(), req.nodeEpoch());
                    return ControllerResult.of(Collections.emptyList(), resp);
                }
                if (streamObjectCommitResult.response() != Errors.NONE) {
                    log.error("[CommitStreamSetObject]: failed to commit srteam object. streamObjectId={}, streamSetObjectId={}, nodeId={}, nodeEpoch={}, error={}",
                        streamObject.objectId(), req.objectId(), req.nodeId(), req.nodeEpoch(), streamObjectCommitResult.response());
                    resp.setErrorCode(streamObjectCommitResult.response().code());
                    return ControllerResult.of(Collections.emptyList(), resp);
                }
                records.addAll(streamObjectCommitResult.records());
                records.add(new S3StreamObject(streamObject.objectId(), streamObject.streamId(), streamObject.startOffset(), streamObject.endOffset()).toRecord(featureControlManager.autoMQVersion()));
            } else {
                log.info("stream already deleted, then fast delete the stream object from compaction. streamId={}, streamObject={}, streamSetObjectId={}, nodeId={}, nodeEpoch={}",
                    streamObject.streamId(), streamObject, req.objectId(), req.nodeId(), req.nodeEpoch());
                ControllerResult<Boolean> deleteRst = this.s3ObjectControlManager.markDestroyObjects(List.of(streamObject.objectId()), List.of(CompactOperations.DEEP_DELETE));
                records.addAll(deleteRst.records());
            }
        }
        return null;
    }

    private ControllerResult<CommitStreamSetObjectResponseData> verifyStreamContinuous(
        List<ObjectStreamRange> streamRanges, List<StreamObject> streamObjects,
        CommitStreamSetObjectRequestData req, CommitStreamSetObjectResponseData resp) {
        List<StreamOffsetRange> offsetRanges = Stream.concat(
                streamRanges
                    .stream()
                    .map(range -> new StreamOffsetRange(range.streamId(), range.startOffset(), range.endOffset())),
                streamObjects
                    .stream()
                    .map(obj -> new StreamOffsetRange(obj.streamId(), obj.startOffset(), obj.endOffset())))
            .collect(Collectors.toList());
        Errors continuityCheckResult = streamAdvanceCheck(offsetRanges, req.nodeId());
        if (continuityCheckResult != Errors.NONE) {
            log.error("[CommitStreamSetObject] advance check failed. streamSetObjectId={}, nodeId={}, nodeEpoch={}, error={}",
                req.objectId(), req.nodeId(), req.nodeEpoch(), continuityCheckResult);
            resp.setErrorCode(continuityCheckResult.code());
            return ControllerResult.of(Collections.emptyList(), resp);
        }
        return null;
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
        AutoMQVersion version = featureControlManager.autoMQVersion();

        int nodeId = data.nodeId();
        long nodeEpoch = data.nodeEpoch();
        long streamObjectId = data.objectId();
        long streamId = data.streamId();
        long streamEpoch = data.streamEpoch();
        long startOffset = data.startOffset();
        long endOffset = data.endOffset();
        long objectSize = data.objectSize();
        int attributes = data.attributes();
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
        ControllerResult<Errors> commitResult = this.s3ObjectControlManager.commitObject(streamObjectId, objectSize, committedTs, attributes);
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

        // mark destroy compacted object
        if (sourceObjectIds != null && !sourceObjectIds.isEmpty()) {
            List<CompactOperations> operations;
            if (data.operations().isEmpty()) {
                operations = Collections.nCopies(sourceObjectIds.size(), CompactOperations.DELETE);
            } else {
                operations = data.operations().stream().map(v -> CompactOperations.fromValue(v)).collect(Collectors.toList());
            }
            ControllerResult<Boolean> destroyResult = this.s3ObjectControlManager.markDestroyObjects(sourceObjectIds, operations);
            if (!destroyResult.response()) {
                log.error("[CommitStreamObject]: failed to mark destroy compacted objects. compactedObjects={}, req={}",
                    sourceObjectIds, data);
                resp.setErrorCode(Errors.COMPACTED_OBJECTS_NOT_FOUND.code());
                return ControllerResult.of(Collections.emptyList(), resp);
            }
            records.addAll(destroyResult.records());
        }

        if (streamObjectId != NOOP_OBJECT_ID) {
            // generate stream object record
            records.add(new ApiMessageAndVersion(new S3StreamObjectRecord()
                .setObjectId(streamObjectId)
                .setStreamId(streamId)
                .setStartOffset(startOffset)
                .setEndOffset(endOffset), version.streamObjectRecordVersion()));
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

    private DescribeStreamsResponseData bulidDescribeStreamsResponseData(
        List<StreamRuntimeMetadata> streamRuntimeMetadataList) {
        List<DescribeStreamsResponseData.StreamMetadata> metadataList = streamRuntimeMetadataList.stream()
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
                    endOffset = streamMetadata.endOffset();
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
            StreamRuntimeMetadata metadata = streamsMetadata.get(streamId);
            if (metadata == null) {
                return bulidDescribeStreamsResponseData(Collections.emptyList());
            }
            return bulidDescribeStreamsResponseData(List.of(metadata));
        }

        int nodeId = data.nodeId();
        if (nodeId >= 0) {
            List<StreamRuntimeMetadata> metadataList = streamsMetadata.values().stream()
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

        List<StreamRuntimeMetadata> metadataList = streamsMetadata.values().stream()
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

        NodeRuntimeMetadata nodeRuntimeMetadata = this.nodesMetadata.get(nodeId);
        if (nodeRuntimeMetadata == null) {
            // create a new node metadata if absent
            log.info("[GetOpeningStreams]: create new node metadata. nodeId={}, nodeEpoch={}, failoverMode={}",
                nodeId, nodeEpoch, failoverMode);
            records.add(new ApiMessageAndVersion(
                new NodeWALMetadataRecord().setNodeId(nodeId).setNodeEpoch(nodeEpoch).setFailoverMode(failoverMode),
                (short) 0));
        }

        // verify and update node epoch
        if (nodeRuntimeMetadata != null && nodeEpoch < nodeRuntimeMetadata.getNodeEpoch()) {
            // node epoch has been expired
            resp.setErrorCode(Errors.NODE_EPOCH_EXPIRED.code());
            log.warn("[GetOpeningStreams]: expired node epoch. nodeId={}, nodeEpoch={}, requestNodeEpoch={}",
                nodeId, nodeRuntimeMetadata.getNodeEpoch(), nodeEpoch);
            return ControllerResult.of(Collections.emptyList(), resp);
        }

        if (nodeRuntimeMetadata != null) {
            // update node epoch
            log.info("[GetOpeningStreams]: update node epoch. nodeId={}, oldNodeEpoch={}, newNodeEpoch={}, failoverMode={}",
                nodeId, nodeRuntimeMetadata.getNodeEpoch(), nodeEpoch, failoverMode);
            records.add(new ApiMessageAndVersion(
                new NodeWALMetadataRecord().setNodeId(nodeId).setNodeEpoch(nodeEpoch).setFailoverMode(failoverMode),
                (short) 0));
        }

        // The getOpeningStreams is invoked when node startup, so we just iterate all streams to get the node opening streams.
        List<StreamMetadata> streamStatusList = this.streamsMetadata.entrySet().stream().filter(entry -> {
            StreamRuntimeMetadata streamMetadata = entry.getValue();
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
            StreamRuntimeMetadata streamMetadata = e.getValue();
            RangeMetadata rangeMetadata = streamMetadata.ranges().get(streamMetadata.currentRangeIndex());
            return new StreamMetadata()
                .setStreamId(e.getKey())
                .setEpoch(streamMetadata.currentEpoch())
                .setStartOffset(streamMetadata.startOffset())
                // Fix https://github.com/AutoMQ/automq/issues/1222#issuecomment-2132812938
                .setEndOffset(Math.max(streamMetadata.endOffset(), streamMetadata.startOffset()));
        }).collect(Collectors.toList());
        // TODO: replace scan with the #getOpeningStreams(nodeId)
        doubleCheckOpeningStreams(streamStatusList, nodeId);
        resp.setStreamMetadataList(streamStatusList);
        return ControllerResult.atomicOf(records, resp);
    }

    private void doubleCheckOpeningStreams(List<StreamMetadata> openingStreams, int nodeId) {
        List<Long> left = openingStreams.stream().map(s -> s.streamId()).sorted().collect(Collectors.toList());
        List<Long> right = getOpeningStreams(nodeId).stream().map(s -> s.streamId()).sorted().collect(Collectors.toList());
        if (!left.equals(right)) {
            RuntimeException e = new IllegalStateException(String.format("The opening streams are inconsistent, left: %s, right: %s", left, right));
            LOGGER.error("doubleCheckOpeningStreams", e);
            throw e;
        }
    }

    public List<StreamRuntimeMetadata> getOpeningStreams(int nodeId) {
        List<Long> streamIdList = Optional.ofNullable(node2streams.get(nodeId)).map(l -> l.toList()).orElse(Collections.emptyList());
        List<StreamRuntimeMetadata> streams = new ArrayList<>(streamIdList.size());
        for (Long streamId: streamIdList) {
            StreamRuntimeMetadata streamRuntimeMetadata = streamsMetadata.get(streamId);
            if (streamRuntimeMetadata == null) {
                continue;
            }
            if (streamRuntimeMetadata.currentState() == StreamState.OPENED) {
                streams.add(streamRuntimeMetadata);
            }
        }
        return streams;
    }

    public boolean hasOpeningStreams(int nodeId) {
        DeltaList<Long> streamIdList = node2streams.get(nodeId);
        if (streamIdList == null) {
            return false;
        }
        AtomicBoolean hasOpeningStreams = new AtomicBoolean(false);
        streamIdList.reverseForEachWithBreak(new Function<Long, Boolean>() {
            @Override
            public Boolean apply(Long streamId) {
                StreamRuntimeMetadata streamRuntimeMetadata = streamsMetadata.get(streamId);
                if (streamRuntimeMetadata == null) {
                    return false;
                }
                if (streamRuntimeMetadata.currentState() == StreamState.OPENED) {
                    hasOpeningStreams.set(true);
                    return true;
                }
                return false;
            }
        });
        return hasOpeningStreams.get();
    }

    /**
     * Check whether this node is the owner of this stream.
     */
    private Errors streamOwnershipCheck(long streamId, long epoch, int nodeId, String operationName) {
        if (!this.streamsMetadata.containsKey(streamId)) {
            log.warn("[{}]: streamId={} not exist", operationName, streamId);
            return Errors.STREAM_NOT_EXIST;
        }
        StreamRuntimeMetadata streamMetadata = this.streamsMetadata.get(streamId);
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
            StreamRuntimeMetadata streamMetadata = this.streamsMetadata.get(range.streamId());
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
            long streamEndOffset = streamMetadata.endOffset();
            if (!(streamEndOffset == range.startOffset() || range.startOffset() <= streamMetadata.startOffset())) {
                // Fix https://github.com/AutoMQ/automq/issues/1222#issuecomment-2132812938
                log.warn("[streamAdvanceCheck]: streamId={}'s end offset {} is not equal to request start offset {}, stream's startOffset={}",
                    range.streamId(), streamEndOffset, range.startOffset(), streamMetadata.startOffset());
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
        NodeRuntimeMetadata nodeRuntimeMetadata = this.nodesMetadata.get(nodeId);
        if (nodeRuntimeMetadata == null) {
            // should not happen
            log.error("[NodeEpochCheck]: nodeId={} not exist when check node epoch", nodeId);
            return Errors.NODE_EPOCH_NOT_EXIST;
        }
        if (nodeRuntimeMetadata.getNodeEpoch() > nodeEpoch) {
            log.warn("[NodeEpochCheck]: nodeId={}'s epoch={} is larger than request epoch={}", nodeId,
                this.nodesMetadata.get(nodeId).getNodeEpoch(), nodeEpoch);
            return Errors.NODE_EPOCH_EXPIRED;
        }
        if (checkFailover && nodeRuntimeMetadata.getFailoverMode()) {
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
        nodesMetadata.forEach((nodeId, nodeRuntimeMetadata) -> {
            if (clusterControlManager.isActive(nodeId) || cleaningUpNodes.contains(nodeId)) {
                return;
            }
            List<S3StreamSetObject> objects = new ArrayList<>(nodeRuntimeMetadata.streamSetObjects().values());
            if (objects.isEmpty()) {
                return;
            }
            CleanUpScaleInNodeContext ctx = new CleanUpScaleInNodeContext(nodeId, objects);
            cleaningUpNodes.add(nodeId);
            cleanupScaleInNode0(ctx);
            ctx.cf.whenComplete((nil, ex) -> {
                if (ex != null) {
                    log.error("cleanupScaleInNode failed", ex);
                }
                cleaningUpNodes.remove(nodeId);
            });

        });
        return ControllerResult.of(records, null);
    }

    public void replay(AssignedStreamIdRecord record) {
        this.nextAssignedStreamId.set(record.assignedStreamId() + 1);
    }

    public void replay(S3StreamRecord record) {
        long streamId = record.streamId();
        // already exist, update the stream's metadata
        if (this.streamsMetadata.containsKey(streamId)) {
            StreamRuntimeMetadata streamMetadata = this.streamsMetadata.get(streamId);
            StreamState newState = StreamState.fromByte(record.streamState());
            streamMetadata.startOffset(record.startOffset());
            streamMetadata.currentEpoch(record.epoch());
            streamMetadata.currentRangeIndex(record.rangeIndex());
            streamMetadata.currentState(newState);
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
        StreamRuntimeMetadata streamMetadata = new StreamRuntimeMetadata(record.streamId(), record.epoch(), record.rangeIndex(),
            record.startOffset(), StreamState.fromByte(record.streamState()), tags, this.snapshotRegistry);
        this.streamsMetadata.put(streamId, streamMetadata);

    }

    public void replay(RemoveS3StreamRecord record) {
        long streamId = record.streamId();
        StreamRuntimeMetadata streamMetadata = this.streamsMetadata.remove(streamId);
        if (streamMetadata == null) {
            return;
        }
        streamMetadata.ranges().values().forEach(rangeMetadata -> {
            node2streams.computeIfPresent(rangeMetadata.nodeId(), (k, v) -> {
                v = v.copy();
                v.remove(m -> streamId == m);
                return v;
            });
        });
    }

    public void replay(RangeRecord record) {
        long streamId = record.streamId();
        StreamRuntimeMetadata streamMetadata = this.streamsMetadata.get(streamId);
        if (streamMetadata == null) {
            // should not happen
            log.error("streamId={} not exist when replay range record {}", streamId, record);
            return;
        }
        RangeMetadata rangeMetadata = RangeMetadata.of(record);

        streamMetadata.ranges().put(record.rangeIndex(), rangeMetadata);

        // When load from image the ranges are not orderly replayed.
        boolean islastRange = rangeMetadata.rangeIndex() == streamMetadata.currentRangeIndex();
        // The stream trim also generate RangeRecord
        if (islastRange) {
            Integer lastNodeId = stream2node.get(streamId);
            if (lastNodeId == null || lastNodeId != record.nodeId()) {
                node2streams.compute(record.nodeId(), (k, v) -> {
                    if (v == null) {
                        v = new DeltaList<>();
                    } else {
                        v = v.copy();
                    }
                    v.add(streamId);
                    return v;
                });
                if (lastNodeId != null) {
                    node2streams.compute(lastNodeId, (k, v) -> {
                        if (v == null) {
                            return null;
                        } else {
                            v = v.copy();
                        }
                        v.remove(m -> m == streamId);
                        return v;
                    });
                }
            }
            stream2node.put(streamId, rangeMetadata.nodeId());
        }
    }

    public void replay(RemoveRangeRecord record) {
        long streamId = record.streamId();
        StreamRuntimeMetadata streamMetadata = this.streamsMetadata.get(streamId);
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
            NodeRuntimeMetadata nodeRuntimeMetadata = this.nodesMetadata.get(nodeId);
            nodeRuntimeMetadata.setNodeEpoch(nodeEpoch);
            nodeRuntimeMetadata.setFailoverMode(record.failoverMode());
            return;
        }
        // not exist, create a new node
        this.nodesMetadata.put(nodeId, new NodeRuntimeMetadata(nodeId, nodeEpoch, record.failoverMode(), this.snapshotRegistry));
    }

    public void replay(S3StreamSetObjectRecord record) {
        long objectId = record.objectId();
        int nodeId = record.nodeId();
        long orderId = record.orderId();
        long dataTs = record.dataTimeInMs();
        NodeRuntimeMetadata nodeRuntimeMetadata = this.nodesMetadata.get(nodeId);
        if (nodeRuntimeMetadata == null) {
            // should not happen
            log.error("nodeId={} not exist when replay stream set object record {}", nodeId, record);
            return;
        }
        S3StreamSetObject s3StreamSetObject = new S3StreamSetObject(objectId, nodeId, record.ranges(), orderId, dataTs);
        nodeRuntimeMetadata.streamSetObjects().put(objectId, s3StreamSetObject);

        // update range
        s3StreamSetObject.offsetRangeList().forEach(index -> {
            long streamId = index.streamId();
            StreamRuntimeMetadata metadata = this.streamsMetadata.get(streamId);
            if (metadata == null) {
                // ignore it, the stream may be deleted
                return;
            }
            // the offset continuous is ensured by the process layer
            // when replay from checkpoint, the record may be out of order, so we need to update the end offset to the largest end offset.
            metadata.endOffset(index.endOffset());
        });
    }

    public void replay(RemoveStreamSetObjectRecord record) {
        long objectId = record.objectId();
        NodeRuntimeMetadata walMetadata = this.nodesMetadata.get(record.nodeId());
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

        StreamRuntimeMetadata streamMetadata = this.streamsMetadata.get(streamId);
        if (streamMetadata == null) {
            // should not happen
            log.error("streamId={} not exist when replay stream object record {}", streamId, record);
            return;
        }
        streamMetadata.streamObjects().put(objectId, new S3StreamObject(objectId, streamId, startOffset, endOffset));
        // the offset continuous is ensured by the process layer
        // when replay from checkpoint, the record may be out of order, so we need to update the end offset to the largest end offset.
        streamMetadata.endOffset(endOffset);
    }

    public void replay(RemoveS3StreamObjectRecord record) {
        long streamId = record.streamId();
        long objectId = record.objectId();
        StreamRuntimeMetadata streamMetadata = this.streamsMetadata.get(streamId);
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

    public void replay(S3StreamEndOffsetsRecord record) {
        for (StreamEndOffset streamEndOffset : S3StreamEndOffsetsCodec.decode(record.endOffsets())) {
            StreamRuntimeMetadata streamMetadata = this.streamsMetadata.get(streamEndOffset.streamId());
            if (streamMetadata == null) {
                // should not happen
                log.error("streamId={} not exist when replay S3StreamEndOffsetsRecord", streamEndOffset.streamId());
                continue;
            }
            streamMetadata.endOffset(streamEndOffset.endOffset());
        }
    }

    public TimelineHashMap<Long, StreamRuntimeMetadata> streamsMetadata() {
        return streamsMetadata;
    }

    public Map<Integer, NodeRuntimeMetadata> nodesMetadata() {
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

    static class CleanUpScaleInNodeContext {
        int nodeId;
        List<S3StreamSetObject> objects;
        int index;
        CompletableFuture<Void> cf = new CompletableFuture<>();

        public CleanUpScaleInNodeContext(int nodeId, List<S3StreamSetObject> objects) {
            this.nodeId = nodeId;
            this.objects = objects;
            this.index = 0;
        }
    }

    private void logCommitStreamSetObject(CommitStreamSetObjectRequestData req) {
        if (!log.isInfoEnabled()) {
            return;
        }
        StringBuilder sb = new StringBuilder();
        sb.append("[CommitStreamSetObject]: successfully commit stream set object, ");
        sb.append("streamSetObjectId=").append(req.objectId()).append(", nodeId=").append(req.nodeId());
        sb.append(", nodeEpoch=").append(req.nodeEpoch()).append(", compactedObjects=").append(req.compactedObjectIds());
        log.info(sb.toString());
    }

    void cleanupScaleInNode0(CleanUpScaleInNodeContext ctx) {
        if (ctx.index >= ctx.objects.size()) {
            ctx.cf.complete(null);
            return;
        }
        S3StreamSetObject object = ctx.objects.get(ctx.index);

        Optional<ObjectReader> objectReaderOpt = s3ObjectControlManager.objectReader(object.objectId());
        if (objectReaderOpt.isEmpty()) {
            ctx.cf.complete(null);
            return;
        }

        ObjectReader objectReader = objectReaderOpt.get();
        objectReader.basicObjectInfo().thenAccept(info -> {
            List<StreamOffsetRange> streamOffsetRanges = info.indexBlock().streamOffsetRanges();
            quorumController.appendWriteEvent("checkStreamSetObjectExpired", OptionalLong.empty(), () -> {
                return checkStreamSetObjectExpired(object, streamOffsetRanges);
            }).thenAccept(rst -> {
                // try clean up the node next object
                ctx.index = ctx.index + 1;
                quorumController.appendWriteEvent("cleanupScaleInNode0", OptionalLong.empty(), () -> {
                    cleanupScaleInNode0(ctx);
                    return ControllerResult.of(Collections.emptyList(), null);
                });
            }).exceptionally(ex -> {
                ctx.cf.completeExceptionally(ex);
                return null;
            }).whenComplete((nil, ex) -> {
                objectReader.release();
            });
        }).exceptionally(ex -> {
            ctx.cf.completeExceptionally(ex);
            return null;
        });
    }

    ControllerResult<Boolean> checkStreamSetObjectExpired(S3StreamSetObject object,
        List<StreamOffsetRange> streamOffsetRanges) {
        boolean alive = false;
        for (StreamOffsetRange streamOffsetRange : streamOffsetRanges) {
            StreamRuntimeMetadata stream = streamsMetadata.get(streamOffsetRange.streamId());
            if (stream != null && stream.startOffset() < streamOffsetRange.endOffset()) {
                return ControllerResult.of(Collections.emptyList(), false);
            }
        }
        List<ApiMessageAndVersion> records = new ArrayList<>(2);
        records.add(new ApiMessageAndVersion(new RemoveStreamSetObjectRecord()
            .setNodeId(object.nodeId())
            .setObjectId(object.objectId()), (short) 0));
        records.addAll(this.s3ObjectControlManager.markDestroyObjects(List.of(object.objectId())).records());
        LOGGER.info("clean up scaled-in node={} object={}", object.nodeId(), object.objectId());
        return ControllerResult.of(records, true);
    }
}
