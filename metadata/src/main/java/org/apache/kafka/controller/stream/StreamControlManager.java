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
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData;
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
import org.apache.kafka.common.message.UpdateStreamArchiveRequestData.StreamArchiveOperation;
import org.apache.kafka.common.message.UpdateStreamArchiveResponseData.UpdateStreamResponse;
import org.apache.kafka.common.metadata.AssignedStreamIdRecord;
import org.apache.kafka.common.metadata.NodeWALMetadataRecord;
import org.apache.kafka.common.metadata.NodeWALUncommittedOffsetsRecord;
import org.apache.kafka.common.metadata.RangeRecord;
import org.apache.kafka.common.metadata.RemoveNodeWALMetadataRecord;
import org.apache.kafka.common.metadata.RemoveRangeRecord;
import org.apache.kafka.common.metadata.RemoveS3StreamArchiveRecord;
import org.apache.kafka.common.metadata.RemoveS3StreamObjectRecord;
import org.apache.kafka.common.metadata.RemoveS3StreamRecord;
import org.apache.kafka.common.metadata.RemoveStreamSetObjectRecord;
import org.apache.kafka.common.metadata.S3StreamArchiveRecord;
import org.apache.kafka.common.metadata.S3StreamEndOffsetsRecord;
import org.apache.kafka.common.metadata.S3StreamObjectRecord;
import org.apache.kafka.common.metadata.S3StreamRecord;
import org.apache.kafka.common.metadata.S3StreamSetObjectRecord;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.controller.ClusterControlManager;
import org.apache.kafka.controller.ControllerRequestContext;
import org.apache.kafka.controller.ControllerResult;
import org.apache.kafka.controller.FeatureControlManager;
import org.apache.kafka.controller.QuorumController;
import org.apache.kafka.controller.ReplicationControlManager;
import org.apache.kafka.image.DeltaList;
import org.apache.kafka.metadata.stream.NodeWALUncommittedOffset;
import org.apache.kafka.metadata.stream.NodeWALUncommittedOffsetsRecords;
import org.apache.kafka.metadata.stream.RangeMetadata;
import org.apache.kafka.metadata.stream.S3StreamArchiveMetadata;
import org.apache.kafka.metadata.stream.S3StreamEndOffsetsCodec;
import org.apache.kafka.metadata.stream.S3StreamObject;
import org.apache.kafka.metadata.stream.S3StreamSetObject;
import org.apache.kafka.metadata.stream.StreamEndOffset;
import org.apache.kafka.metadata.stream.StreamTags;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.automq.AutoMQVersion;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.apache.kafka.timeline.TimelineHashSet;
import org.apache.kafka.timeline.TimelineLong;

import com.automq.stream.s3.ObjectReader;
import com.automq.stream.s3.compact.CompactOperations;
import com.automq.stream.s3.metadata.S3StreamConstant;
import com.automq.stream.s3.metadata.StreamOffsetRange;
import com.automq.stream.s3.metadata.StreamState;
import com.automq.stream.s3.metrics.Metrics;
import com.automq.stream.s3.metrics.MetricsLevel;
import com.automq.stream.s3.objects.ObjectAttributes;
import com.automq.stream.s3.operator.LocalFileObjectStorage;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.utils.AsyncLogger;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;

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

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;

import static com.automq.stream.s3.metadata.ObjectUtils.NOOP_OBJECT_ID;

/**
 * The StreamControlManager manages all Stream's lifecycle, such as create, open, delete, etc.
 */
@SuppressWarnings({"all", "this-escape"})
public class StreamControlManager {
    /*
     * Offset and ownership model
     *
     * Persisted state:
     * - StreamRuntimeMetadata.startOffset is the visible lower bound and advances through trim.
     * - StreamRuntimeMetadata.endOffset is the logical end. It is the next current-owner commit
     *   start and the next owner append start; it does not imply ObjectStorage durability.
     * - RangeMetadata defines a node's ownership interval. The end of a closed range is its fixed
     *   handoff boundary, while an opened range may temporarily lag the stream logical end.
     * - NodeWALUncommittedOffset is a node-scoped historical WAL responsibility. Its raw start
     *   advances through historical object commits, and its fixed end is the sealed range end.
     *
     * Derived state:
     * - effectiveHistoricalStart = max(stream.startOffset, entry.startOffset).
     * - An entry is active exactly when effectiveHistoricalStart < entry.endOffset.
     * - Object metadata ranges, rather than any offset above, determine ObjectStorage read coverage.
     *
     * State transitions:
     * - A current-owner commit advances stream.endOffset.
     * - A historical commit advances or removes only that node's uncommitted entry.
     * - A fast close seals the current range, advances the logical end, and records any historical
     *   WAL interval between the pre-close logical end and the broker's close end offset.
     * - Trim advances the visible start without scanning raw node entries; their effective starts
     *   change lazily through the formula above.
     * - getOpeningStreams returns stream.endOffset for a current owner and the effective historical
     *   start for a node recovering an old ownership range.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamControlManager.class);
    private static final Metrics.LongGaugeBundle STREAM_SET_OBJECT_NUM = Metrics.instance()
        .longGauge("kafka_stream_stream_set_object_num", "The total number of stream set objects", "");
    private static final Metrics.LongGaugeBundle STREAM_OBJECT_NUM = Metrics.instance()
        .longGauge("kafka_stream_stream_object_num", "The total number of stream objects", "");
    private static final AttributeKey<String> LABEL_NODE_ID = AttributeKey.stringKey("node_id");
    private static final long ARCHIVE_METADATA_RECONCILIATION_INTERVAL_MINUTES = 1L;

    private final Logger log;

    /**
     * The next stream id to be assigned.
     */
    private final TimelineLong nextAssignedStreamId;

    private final TimelineHashMap<Long/*streamId*/, StreamRuntimeMetadata> streamsMetadata;

    private final TimelineHashMap<Integer/*nodeId*/, NodeRuntimeMetadata> nodesMetadata;
    private final TimelineHashSet<Integer> lockedNodes;

    private final TimelineHashMap<Long, Integer> stream2node;
    private final TimelineHashMap<Integer/* nodeId */, /* streams */DeltaList<Long>> node2streams;
    private final Metrics.LongGaugeBundle.LongGauge streamSetObjectNumMetric;
    private final Metrics.LongGaugeBundle.LongGauge streamObjectNumMetric;

    private Set<Integer> cleaningUpNodes = Collections.newSetFromMap(new ConcurrentHashMap<>());

    private final QuorumController quorumController;

    private final SnapshotRegistry snapshotRegistry;

    private final S3ObjectControlManager s3ObjectControlManager;
    private final StreamArchiveControlManager streamArchiveControlManager;

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
        ReplicationControlManager replicationControlManager,
        ObjectStorage objectStorage,
        Time time) {
        this.snapshotRegistry = snapshotRegistry;
        this.log = AsyncLogger.wrap(logContext.logger(StreamControlManager.class));
        this.nextAssignedStreamId = new TimelineLong(snapshotRegistry);
        this.streamsMetadata = new TimelineHashMap<>(snapshotRegistry, 100000);
        this.nodesMetadata = new TimelineHashMap<>(snapshotRegistry, 0);
        this.lockedNodes = new TimelineHashSet<>(snapshotRegistry, 100);
        this.stream2node = new TimelineHashMap<>(snapshotRegistry, 100000);
        this.node2streams = new TimelineHashMap<>(snapshotRegistry, 100);

        ScheduledExecutorService cleanupScheduler = Executors.newSingleThreadScheduledExecutor(
            ThreadUtils.createThreadFactory("stream-cleanup-scheduler", true));

        this.quorumController = quorumController;
        this.s3ObjectControlManager = s3ObjectControlManager;
        this.clusterControlManager = clusterControlManager;
        this.featureControlManager = featureControlManager;
        this.replicationControlManager = replicationControlManager;
        this.streamArchiveControlManager = new StreamArchiveControlManager(logContext, quorumController,
            s3ObjectControlManager, objectStorage, time, featureControlManager::autoMQVersion,
            (nodeId, nodeEpoch) -> nodeEpochCheck(nodeId, nodeEpoch),
            streamId -> streamsMetadata.get(streamId), snapshotRegistry);

        cleanupScheduler.scheduleWithFixedDelay(this::triggerCleanupScaleInNodes, 30, 30, TimeUnit.MINUTES);
        cleanupScheduler.scheduleWithFixedDelay(streamArchiveControlManager::reconcile,
            ARCHIVE_METADATA_RECONCILIATION_INTERVAL_MINUTES,
            ARCHIVE_METADATA_RECONCILIATION_INTERVAL_MINUTES,
            TimeUnit.MINUTES);

        this.streamSetObjectNumMetric = STREAM_SET_OBJECT_NUM.register(MetricsLevel.INFO, Attributes.empty(), result -> {
            if (!quorumController.isActive()) {
                return;
            }
            for (NodeRuntimeMetadata nodeRuntimeMetadata : nodesMetadata.values()) {
                result.record(nodeRuntimeMetadata.streamSetObjects().size(), Attributes.builder()
                    .put(LABEL_NODE_ID, String.valueOf(nodeRuntimeMetadata.getNodeId()))
                    .build());
            }
        });
        this.streamObjectNumMetric = STREAM_OBJECT_NUM.register(MetricsLevel.INFO, Attributes.empty(), result -> {
            if (!quorumController.isActive()) {
                return;
            }
            result.record(streamsMetadata.values().stream().mapToInt(it -> it.streamObjects().size()).sum());
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
     * Validates and persists one complete Broker-proposed Stream Archive state.
     *
     * <p>Each invocation is one Controller event and one transaction. Archive prepare validates
     * the complete current Composite sequence before its protected boundary is persisted.</p>
     */
    public ControllerResult<UpdateStreamResponse> updateStreamArchive(int nodeId, long nodeEpoch,
        StreamArchiveOperation request) {
        return streamArchiveControlManager.update(nodeId, nodeEpoch, request);
    }

    /**
     * Returns the retained logical bytes represented by Stream Archive records.
     */
    public long streamArchiveSize() {
        return streamArchiveControlManager.totalSize();
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
     *             <li> request node has active historical WAL responsibility for the stream </li>
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
        if (hasActiveWALResponsibility(streamMetadata, nodeId)) {
            resp.setErrorCode(Errors.STREAM_NOT_CLOSED.code());
            log.warn("[OpenStream] node has active historical WAL responsibility. streamId={}, nodeId={}, nodeEpoch={}",
                streamId, nodeId, nodeEpoch);
            return ControllerResult.of(Collections.emptyList(), resp);
        }
        int currentRangeOwner = streamMetadata.currentRangeOwner();
        if (nodeId != currentRangeOwner && lockedNodes.contains(currentRangeOwner)) {
            // Forbidden other nodes to open the stream if the last range is owned by a locked node
            resp.setErrorCode(Errors.NODE_LOCKED.code());
            log.warn("[OpenStream] the stream's last range is owned by a locked node {}. streamId={}, streamEpoch={}, requestEpoch={}, nodeId={}, nodeEpoch={}",
                currentRangeOwner, streamId, streamMetadata.currentEpoch(), epoch, nodeId, nodeEpoch);
            tryReassignPartitionBack(streamMetadata);
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
        long nextRangeStartOffset = streamMetadata.endOffset();
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
        }
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
        if (hasActiveWALResponsibility(streamMetadata, (int) nodeId)) {
            resp.setErrorCode(Errors.STREAM_NOT_CLOSED.code());
            log.warn("[OpenStream] node has active historical WAL responsibility. streamId={}, nodeId={}, nodeEpoch={}",
                streamId, nodeId, nodeEpoch);
            return ControllerResult.of(Collections.emptyList(), resp);
        }
        // epoch equals, node equals, regard it as redundant open operation, just return success
        resp.setStartOffset(streamMetadata.startOffset());
        resp.setNextOffset(streamMetadata.endOffset());
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
     *     <li>
     *         <code>OFFSET_NOT_MATCHED</code>:
     *         <ol>
     *             <li> fast close end offset is below the stream logical end </li>
     *             <li> fast close retries a closed range with a different end offset </li>
     *         </ol>
     *     </li>
     * </ul>
     */
    public ControllerResult<CloseStreamResponse> closeStream(int nodeId, long nodeEpoch, CloseStreamRequest request) {
        CloseStreamResponse resp = new CloseStreamResponse();
        long streamId = request.streamId();
        long epoch = request.streamEpoch();

        if (request.endOffset() >= 0
            && !featureControlManager.autoMQVersion().isFastPartitionReassignmentSupported()) {
            resp.setErrorCode(Errors.UNSUPPORTED_VERSION.code());
            return ControllerResult.of(Collections.emptyList(), resp);
        }

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
            if (request.endOffset() >= 0
                && request.endOffset() != streamMetadata.currentRangeMetadata().endOffset()) {
                resp.setErrorCode(Errors.OFFSET_NOT_MATCHED.code());
            }
            return ControllerResult.of(Collections.emptyList(), resp);
        }
        long closeEndOffset = request.endOffset();
        if (closeEndOffset >= 0 && closeEndOffset < streamMetadata.endOffset()) {
            resp.setErrorCode(Errors.OFFSET_NOT_MATCHED.code());
            return ControllerResult.of(Collections.emptyList(), resp);
        }

        // now the request is valid, update the stream's state
        List<ApiMessageAndVersion> records = new ArrayList<>();
        records.add(new ApiMessageAndVersion(new S3StreamRecord()
                .setStreamId(streamId)
                .setEpoch(epoch)
                .setRangeIndex(streamMetadata.currentRangeIndex())
                .setStartOffset(streamMetadata.startOffset())
                .setStreamState(StreamState.CLOSED.toByte()),
                featureControlManager.autoMQVersion().streamRecordVersion()));
        /*
         * A valid fast-close end offset is the broker's local append tail after writes are frozen.
         * Atomically seal the current ownership range at that boundary. RangeRecord replay advances
         * the stream logical end, while the node entry preserves responsibility for the part not
         * covered by object commits before close.
         */
        RangeMetadata range = streamMetadata.currentRangeMetadata();
        if (closeEndOffset >= 0 && range.endOffset() != closeEndOffset) {
            records.add(new ApiMessageAndVersion(new RangeRecord()
                .setStreamId(streamId)
                .setNodeId(range.nodeId())
                .setStartOffset(range.startOffset())
                .setEndOffset(closeEndOffset)
                .setEpoch(range.epoch())
                .setRangeIndex(range.rangeIndex()), (short) 0));
        }
        if (closeEndOffset > streamMetadata.endOffset()) {
            records.addAll(NodeWALUncommittedOffsetsRecords.create(nodeId, List.of(
                new NodeWALUncommittedOffset(streamId, streamMetadata.endOffset(), closeEndOffset))));
        }
        log.info("[CloseStream] successfully close the stream. streamId={}, streamEpoch={}, nodeId={}, nodeEpoch={}",
            streamId, epoch, nodeId, nodeEpoch);
        return ControllerResult.atomicOf(records, resp);
    }

    private boolean hasActiveWALResponsibility(StreamRuntimeMetadata streamMetadata, int nodeId) {
        return historicalWALResponsibility(streamMetadata, nodeId) != null;
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
        /*
         * Trim advances the visible lower bound and keeps the logical end and current range end at
         * or above it. Raw NodeWALUncommittedOffset entries are intentionally not rewritten here.
         * Commit, open, and recovery paths apply trim lazily with
         * max(stream.startOffset, entry.startOffset).
         */
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
        List<RangeMetadata> ranges = streamMetadata.ranges().values().stream()
            .sorted()
            .toList();
        for (RangeMetadata range : ranges) {
            int rangeIndex = range.rangeIndex();
            if (newStartOffset <= range.startOffset()) {
                break;
            }
            if (rangeIndex == streamMetadata.currentRangeIndex()) {
                long newEndOffset = Math.max(newStartOffset, streamMetadata.endOffset());
                records.add(new ApiMessageAndVersion(new RangeRecord()
                    .setStreamId(streamId)
                    .setRangeIndex(rangeIndex)
                    .setNodeId(range.nodeId())
                    .setEpoch(range.epoch())
                    .setStartOffset(newStartOffset)
                    .setEndOffset(newEndOffset), (short) 0));
                break;
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
            break;
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

        if (data.compactedObjectIds().size() == 1 && data.objectId() == data.compactedObjectIds().get(0)) {
            // replace the stream set object
            return replace(data);
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
            NodeRuntimeMetadata nodeMetadata = this.nodesMetadata.get(nodeId);
            dataTs = compactedObjectIds.stream()
                .map(id -> nodeMetadata.streamSetObjects().get(id))
                .map(S3StreamSetObject::dataTimeInMs)
                .min(Long::compareTo).get();
            if (orderId == -1L && !data.compactedObjectIds().isEmpty()) {
                orderId = data.compactedObjectIds().stream().mapToLong(id -> nodeMetadata.streamSetObjects().get(id).orderId()).min().getAsLong();
            }
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
            List<StreamEndOffset> endOffsets = streamOffsetRanges(streamRanges, streamObjects).stream()
                .filter(range -> isCurrentOwnerCommit(range.streamId(), nodeId))
                .map(range -> new StreamEndOffset(range.streamId(), range.endOffset()))
                .collect(Collectors.toList());
            if (!endOffsets.isEmpty()) {
                S3StreamEndOffsetsRecord record = new S3StreamEndOffsetsRecord()
                    .setEndOffsets(S3StreamEndOffsetsCodec.encode(endOffsets));
                records.add(new ApiMessageAndVersion(record, (short) 0));
            }
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
            ControllerResult<CommitStreamSetObjectResponseData> ret = verifyStreamContinuous(
                streamRanges, streamObjects, data, resp);
            if (ret != null) {
                return ret;
            }
            records.addAll(generateNodeWALUncommittedOffsetsRecords(streamRanges, streamObjects, nodeId));
        }
        logCommitStreamSetObject(data);
        return ControllerResult.atomicOf(records, resp);
    }

    private ControllerResult<CommitStreamSetObjectResponseData> replace(CommitStreamSetObjectRequestData data) {
        CommitStreamSetObjectResponseData resp = new CommitStreamSetObjectResponseData();
        List<ApiMessageAndVersion> records = new ArrayList<>(1);
        long objectId = data.objectId();
        ControllerResult<Errors> rst = s3ObjectControlManager.replaceCommittedObject(objectId, data.attributes());
        if (rst.response() == Errors.NONE) {
            records.addAll(rst.records());
            return ControllerResult.of(records, resp);
        } else {
            resp.setErrorCode(rst.response().code());
            return ControllerResult.of(Collections.emptyList(), resp);
        }
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
        Errors continuityCheckResult = Errors.NONE;
        for (StreamOffsetRange range : streamOffsetRanges(streamRanges, streamObjects)) {
            StreamRuntimeMetadata streamMetadata = streamsMetadata.get(range.streamId());
            if (streamMetadata == null) {
                continue;
            }
            long streamStartOffset = streamMetadata.startOffset();
            // An upload may finish after trim has removed all of its data. Keep its object metadata,
            // but do not advance either current-owner or historical WAL progress.
            if (range.endOffset() <= streamStartOffset) {
                continue;
            }
            NodeWALUncommittedOffset uncommittedOffsetRange =
                historicalWALResponsibility(streamMetadata, req.nodeId());
            if (uncommittedOffsetRange != null) {
                // The sealed end prevents an old owner from committing into the next owner's range.
                if (range.endOffset() > uncommittedOffsetRange.endOffset()) {
                    continuityCheckResult = Errors.OFFSET_NOT_MATCHED;
                    break;
                }

                // Normal historical uploads continue from raw WAL progress. After source restart,
                // getOpeningStreams instead starts WAL recovery at max(visible start, raw progress),
                // so the first recovered upload also enters this branch.
                long expectedStartOffset = Math.max(
                    streamStartOffset, uncommittedOffsetRange.startOffset());
                if (range.startOffset() == expectedStartOffset) {
                    continue;
                }

                // Trim may advance the visible start while an old-owner object is already uploading.
                // Accept that object crossing the trim point only before historical WAL progress
                // itself moves past the new visible start.
                if (uncommittedOffsetRange.startOffset() <= streamStartOffset
                    && range.startOffset() < streamStartOffset) {
                    continue;
                }

                continuityCheckResult = Errors.OFFSET_NOT_MATCHED;
                break;
            }
            if (streamMetadata.currentState() != StreamState.OPENED) {
                continuityCheckResult = Errors.STREAM_NOT_OPENED;
                break;
            }
            RangeMetadata currentRange = streamMetadata.currentRangeMetadata();
            if (currentRange == null) {
                continuityCheckResult = Errors.STREAM_INNER_ERROR;
                break;
            }
            if (currentRange.nodeId() != req.nodeId()) {
                continuityCheckResult = Errors.STREAM_FENCED;
                break;
            }

            if (streamMetadata.endOffset() > streamStartOffset) {
                // Normal current-owner uploads, including uploads after a trim below WAL progress,
                // continue exactly from the logical end without overlap.
                if (range.startOffset() != streamMetadata.endOffset()) {
                    continuityCheckResult = Errors.OFFSET_NOT_MATCHED;
                    break;
                }
                continue;
            }

            // Trim has reached the logical end. A new upload, including one rebuilt after broker
            // restart from the getOpeningStreams end offset, starts at the visible offset.
            if (range.startOffset() == streamStartOffset) {
                continue;
            }

            // A current-owner object formed before trim may contain data on both sides of the trim
            // boundary. Its end is above the visible start because fully trimmed objects were
            // handled earlier.
            if (range.startOffset() < streamStartOffset) {
                continue;
            }

            continuityCheckResult = Errors.OFFSET_NOT_MATCHED;
            break;
        }
        if (continuityCheckResult != Errors.NONE) {
            log.error("[CommitStreamSetObject] advance check failed. streamSetObjectId={}, nodeId={}, nodeEpoch={}, error={}",
                req.objectId(), req.nodeId(), req.nodeEpoch(), continuityCheckResult);
            resp.setErrorCode(continuityCheckResult.code());
            return ControllerResult.of(Collections.emptyList(), resp);
        }
        return null;
    }

    private List<ApiMessageAndVersion> generateNodeWALUncommittedOffsetsRecords(
        List<ObjectStreamRange> streamRanges, List<StreamObject> streamObjects, int nodeId
    ) {
        NodeRuntimeMetadata nodeMetadata = nodesMetadata.get(nodeId);
        if (nodeMetadata == null) {
            return Collections.emptyList();
        }
        Map<Long, Long> committedEndOffsets = streamOffsetRanges(streamRanges, streamObjects).stream()
            .collect(Collectors.toMap(StreamOffsetRange::streamId, StreamOffsetRange::endOffset, Math::max));
        List<NodeWALUncommittedOffset> offsets = new ArrayList<>();
        // Historical responsibilities are normally empty and remain non-empty only until the old
        // owner's WAL is committed. Scan them all so any later normal WAL commit by this node also
        // removes entries made inactive by trim or stream deletion.
        for (NodeWALUncommittedOffset uncommittedOffsetRange : nodeMetadata.uncommittedOffsets().values()) {
            StreamRuntimeMetadata streamMetadata = streamsMetadata.get(uncommittedOffsetRange.streamId());
            if (streamMetadata == null || historicalWALResponsibility(streamMetadata, nodeId) == null) {
                offsets.add(new NodeWALUncommittedOffset(uncommittedOffsetRange.streamId(),
                    uncommittedOffsetRange.endOffset(), uncommittedOffsetRange.endOffset()));
                continue;
            }
            Long committedEndOffset = committedEndOffsets.get(uncommittedOffsetRange.streamId());
            if (committedEndOffset == null) {
                continue;
            }
            if (committedEndOffset == uncommittedOffsetRange.endOffset()) {
                offsets.add(new NodeWALUncommittedOffset(uncommittedOffsetRange.streamId(),
                    uncommittedOffsetRange.endOffset(), uncommittedOffsetRange.endOffset()));
            } else if (committedEndOffset > streamMetadata.startOffset()) {
                offsets.add(new NodeWALUncommittedOffset(uncommittedOffsetRange.streamId(), committedEndOffset,
                    uncommittedOffsetRange.endOffset()));
            }
        }
        return NodeWALUncommittedOffsetsRecords.create(nodeId, offsets);
    }

    private boolean isCurrentOwnerCommit(long streamId, int nodeId) {
        StreamRuntimeMetadata streamMetadata = streamsMetadata.get(streamId);
        RangeMetadata currentRange = streamMetadata == null ? null : streamMetadata.currentRangeMetadata();
        return currentRange != null && currentRange.nodeId() == nodeId
            && historicalWALResponsibility(streamMetadata, nodeId) == null;
    }

    private NodeWALUncommittedOffset historicalWALResponsibility(
        StreamRuntimeMetadata streamMetadata, int nodeId
    ) {
        NodeRuntimeMetadata nodeMetadata = nodesMetadata.get(nodeId);
        if (nodeMetadata == null) {
            return null;
        }
        NodeWALUncommittedOffset uncommittedOffsetRange =
            nodeMetadata.uncommittedOffsets().get(streamMetadata.streamId());
        if (uncommittedOffsetRange == null
            || Math.max(streamMetadata.startOffset(), uncommittedOffsetRange.startOffset())
                >= uncommittedOffsetRange.endOffset()) {
            return null;
        }
        return uncommittedOffsetRange;
    }

    private static List<StreamOffsetRange> streamOffsetRanges(
        List<ObjectStreamRange> streamRanges, List<StreamObject> streamObjects
    ) {
        return Stream.concat(
                streamRanges.stream().map(range -> new StreamOffsetRange(
                    range.streamId(), range.startOffset(), range.endOffset())),
                streamObjects.stream().map(object -> new StreamOffsetRange(
                    object.streamId(), object.startOffset(), object.endOffset())))
            .collect(Collectors.toList());
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

        if (overlapsArchiveProtectedRange(data)) {
            resp.setErrorCode(Errors.STREAM_ARCHIVE_STATE_CONFLICT.code());
            return ControllerResult.of(Collections.emptyList(), resp);
        }

        if (data.sourceObjectIds().size() == 1 && streamObjectId == data.sourceObjectIds().get(0)) {
            return replace(data);
        }

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

    private boolean overlapsArchiveProtectedRange(CommitStreamObjectRequestData data) {
        StreamRuntimeMetadata stream = streamsMetadata.get(data.streamId());
        S3StreamArchiveMetadata archive = getStreamArchiveMetadata(data.streamId());
        if (stream == null || archive == null
            || archive.archiveMetadataEndOffset() == archive.archivePreparedEndOffset()) {
            return false;
        }
        long protectedStart = archive.archiveMetadataEndOffset();
        long protectedEnd = archive.archivePreparedEndOffset();
        if (data.objectId() != NOOP_OBJECT_ID
            && rangesOverlap(data.startOffset(), data.endOffset(), protectedStart, protectedEnd)) {
            return true;
        }
        return data.sourceObjectIds().stream()
            .map(stream.streamObjects()::get)
            .filter(java.util.Objects::nonNull)
            .anyMatch(object -> rangesOverlap(object.startOffset(), object.endOffset(), protectedStart, protectedEnd));
    }

    private boolean rangesOverlap(long firstStart, long firstEnd, long secondStart, long secondEnd) {
        return firstStart < secondEnd && firstEnd > secondStart;
    }

    private ControllerResult<CommitStreamObjectResponseData> replace(CommitStreamObjectRequestData data) {
        CommitStreamObjectResponseData resp = new CommitStreamObjectResponseData();
        List<ApiMessageAndVersion> records = new ArrayList<>(1);
        long objectId = data.objectId();
        ControllerResult<Errors> rst = s3ObjectControlManager.replaceCommittedObject(objectId, data.attributes());
        if (rst.response() == Errors.NONE) {
            records.addAll(rst.records());
            return ControllerResult.of(records, resp);
        } else {
            resp.setErrorCode(rst.response().code());
            return ControllerResult.of(Collections.emptyList(), resp);
        }
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
                .setEpoch(rangeMetadata.epoch())
                .setStartOffset(streamMetadata.startOffset())
                // Fix https://github.com/AutoMQ/automq/issues/1222#issuecomment-2132812938
                .setEndOffset(Math.max(streamMetadata.endOffset(), streamMetadata.startOffset()));
        }).collect(Collectors.toList());

        if (featureControlManager.autoMQVersion().isFastPartitionReassignmentSupported()) {
            Errors historicalResult = appendHistoricalOpeningStreams(nodeId, streamStatusList);
            if (historicalResult != Errors.NONE) {
                resp.setErrorCode(historicalResult.code());
                return ControllerResult.of(Collections.emptyList(), resp);
            }
        }
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
        for (Long streamId : streamIdList) {
            StreamRuntimeMetadata streamRuntimeMetadata = streamsMetadata.get(streamId);
            if (streamRuntimeMetadata == null) {
                continue;
            }
            if (streamRuntimeMetadata.currentState() == StreamState.OPENED) {
                streams.add(streamRuntimeMetadata);
            }
        }
        Set<Long> openingStreamIds = streams.stream()
            .map(StreamRuntimeMetadata::streamId)
            .collect(Collectors.toSet());
        for (StreamRuntimeMetadata historicalStream : getActiveHistoricalStreams(nodeId)) {
            if (openingStreamIds.add(historicalStream.streamId())) {
                streams.add(historicalStream);
            }
        }
        return streams;
    }

    public boolean hasOpeningStreams(int nodeId) {
        DeltaList<Long> streamIdList = node2streams.get(nodeId);
        if (streamIdList == null) {
            return !getActiveHistoricalStreams(nodeId).isEmpty();
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
        if (hasOpeningStreams.get()) {
            return true;
        }
        return !getActiveHistoricalStreams(nodeId).isEmpty();
    }

    private List<StreamRuntimeMetadata> getActiveHistoricalStreams(int nodeId) {
        NodeRuntimeMetadata nodeMetadata = nodesMetadata.get(nodeId);
        if (nodeMetadata == null) {
            return Collections.emptyList();
        }
        List<StreamRuntimeMetadata> activeStreams = new ArrayList<>();
        for (Long streamId : nodeMetadata.uncommittedOffsets().keySet()) {
            StreamRuntimeMetadata streamMetadata = streamsMetadata.get(streamId);
            if (streamMetadata != null && hasActiveWALResponsibility(streamMetadata, nodeId)) {
                activeStreams.add(streamMetadata);
            }
        }
        return activeStreams;
    }

    private Errors appendHistoricalOpeningStreams(int nodeId, List<StreamMetadata> streamStatusList) {
        NodeRuntimeMetadata nodeMetadata = nodesMetadata.get(nodeId);
        if (nodeMetadata == null) {
            return Errors.NONE;
        }
        /*
         * Historical entries make a closed old ownership range recoverable by its former node.
         * For these response items, StreamMetadata.endOffset is the WAL recovery/next commit start,
         * not the stream logical end. The raw entry may lag a trim, so derive that start from both
         * pieces of state and require one uniquely matching sealed ownership range.
         */
        Set<Long> returnedStreamIds = streamStatusList.stream()
            .map(StreamMetadata::streamId)
            .collect(Collectors.toSet());
        for (NodeWALUncommittedOffset uncommittedOffsetRange : nodeMetadata.uncommittedOffsets().values()) {
            StreamRuntimeMetadata streamMetadata = streamsMetadata.get(uncommittedOffsetRange.streamId());
            if (streamMetadata == null) {
                continue;
            }
            long recoveryStartOffset = Math.max(streamMetadata.startOffset(), uncommittedOffsetRange.startOffset());
            if (recoveryStartOffset >= uncommittedOffsetRange.endOffset()) {
                continue;
            }
            List<RangeMetadata> matchingRanges = streamMetadata.ranges().values().stream()
                .filter(range -> range.nodeId() == nodeId)
                .filter(range -> range.startOffset() <= recoveryStartOffset
                    && recoveryStartOffset < range.endOffset())
                .filter(range -> range.endOffset() == uncommittedOffsetRange.endOffset())
                .collect(Collectors.toList());
            if (matchingRanges.size() != 1 || !returnedStreamIds.add(uncommittedOffsetRange.streamId())) {
                log.error("[GetOpeningStreams] historical WAL responsibility has no unique range. "
                        + "nodeId={}, streamId={}, recoveryStartOffset={}, entryEndOffset={}, matches={}",
                    nodeId, uncommittedOffsetRange.streamId(), recoveryStartOffset,
                    uncommittedOffsetRange.endOffset(), matchingRanges);
                streamStatusList.clear();
                return Errors.STREAM_INNER_ERROR;
            }
            RangeMetadata range = matchingRanges.get(0);
            streamStatusList.add(new StreamMetadata()
                .setStreamId(uncommittedOffsetRange.streamId())
                .setEpoch(range.epoch())
                .setStartOffset(streamMetadata.startOffset())
                .setEndOffset(recoveryStartOffset));
        }
        return Errors.NONE;
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
            boolean inMainStorageCircuitBreakerOpenStatus = objects.stream().anyMatch(sso -> {
                return Optional.ofNullable(s3ObjectControlManager.getObject(sso.objectId()))
                    .map(o -> ObjectAttributes.from(o.getAttributes()).bucket() == LocalFileObjectStorage.BUCKET_ID)
                    .orElse(false);
            });
            if (objects.isEmpty() || inMainStorageCircuitBreakerOpenStatus) {
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

    public void lock(int nodeId) {
        lockedNodes.add(nodeId);
    }

    public void unlock(int nodeId) {
        lockedNodes.remove(nodeId);
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
            streamMetadata.endOffset(record.startOffset());
            streamMetadata.currentEpoch(record.epoch());
            streamMetadata.currentRangeIndex(record.rangeIndex());
            streamMetadata.currentState(newState);
            if (streamMetadata.tags().isEmpty() && record.tags().size() > 0) {
                Map<String, String> tags = new HashMap<>();
                record.tags().forEach(tag -> tags.put(tag.key(), tag.value()));
                streamMetadata.setTags(tags);
            }
            streamArchiveControlManager.onStreamCreated(streamId);
            return;
        }
        Map<String, String> tags = new HashMap<>();
        record.tags().forEach(tag -> tags.put(tag.key(), tag.value()));
        // not exist, create a new stream
        StreamRuntimeMetadata streamMetadata = new StreamRuntimeMetadata(record.streamId(), record.epoch(), record.rangeIndex(),
            record.startOffset(), StreamState.fromByte(record.streamState()), tags, this.snapshotRegistry);
        this.streamsMetadata.put(streamId, streamMetadata);
        streamArchiveControlManager.onStreamCreated(streamId);

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
        streamArchiveControlManager.onStreamDeleted(streamId);
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
        streamMetadata.endOffset(record.endOffset());

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
        streamMetadata.streamObjects().put(objectId,
            new S3StreamObject(objectId, streamId, startOffset, endOffset));
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

    /**
     * Replay node WAL responsibility entry upserts and equal-boundary tombstones. This method is
     * invoked on the Controller event thread, and mutations participate in timeline snapshots.
     */
    public void replay(NodeWALUncommittedOffsetsRecord record) {
        NodeRuntimeMetadata nodeMetadata = nodesMetadata.get(record.nodeId());
        if (nodeMetadata == null) {
            log.error("nodeId={} not exist when replay NodeWALUncommittedOffsetsRecord", record.nodeId());
            return;
        }
        for (NodeWALUncommittedOffsetsRecord.NodeWALUncommittedOffset entry : record.entries()) {
            if (entry.startOffset() < entry.endOffset()) {
                nodeMetadata.uncommittedOffsets().put(entry.streamId(), new NodeWALUncommittedOffset(
                    entry.streamId(), entry.startOffset(), entry.endOffset()));
            } else if (entry.startOffset() == entry.endOffset()) {
                nodeMetadata.uncommittedOffsets().remove(entry.streamId());
            }
        }
    }

    /**
     * Replaces the complete durable Archive state for a Stream.
     */
    public void replay(S3StreamArchiveRecord record) {
        streamArchiveControlManager.replay(record);
    }

    /**
     * Removes the durable Archive state for a Stream.
     */
    public void replay(RemoveS3StreamArchiveRecord record) {
        streamArchiveControlManager.replay(record);
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

    private void tryReassignPartitionBack(StreamRuntimeMetadata stream) {
        ControllerRequestContext context = new ControllerRequestContext(null, null, OptionalLong.empty());
        AlterPartitionReassignmentsRequestData request = new AlterPartitionReassignmentsRequestData();
        String rawTopicId = stream.tags().get(StreamTags.Topic.KEY);
        String rawPartitionIndex = stream.tags().get(StreamTags.Partition.KEY);
        if (Strings.isNullOrEmpty(rawTopicId) || Strings.isNullOrEmpty(rawPartitionIndex)) {
            return;
        }
        Uuid topicId = Uuid.fromString(rawTopicId);
        int partitionIndex = StreamTags.Partition.decode(rawPartitionIndex);
        int nodeId = stream.currentRangeOwner();
        quorumController.findTopicNames(context, List.of(topicId)).thenAccept(uuid2name -> {
            String topicName = Optional.ofNullable(uuid2name.get(topicId)).filter(r -> !r.isError()).map(r -> r.result()).orElse(null);
            if (topicName == null) {
                return;
            }
            request.setTopics(List.of(new AlterPartitionReassignmentsRequestData.ReassignableTopic()
                .setName(topicName)
                .setPartitions(List.of(
                    new AlterPartitionReassignmentsRequestData.ReassignablePartition()
                        .setPartitionIndex(partitionIndex)
                        .setReplicas(List.of(nodeId))
                ))));
            quorumController.alterPartitionReassignments(context, request)
                .thenAccept(rst -> {
                    LOGGER.info("[REASSIGN_PARTITION_BACK_TO_LOCKED_NODE],req={},resp={}", request, rst);
                });
        });
    }

    @VisibleForTesting
    ControllerResult<Void> cleanupStreamArchiveMetadata(long streamId) {
        return streamArchiveControlManager.cleanupMetadata(streamId);
    }

    @VisibleForTesting
    void reconcileStreamArchiveMetadataCleanup() {
        streamArchiveControlManager.reconcile();
    }

    @VisibleForTesting
    public S3StreamArchiveMetadata getStreamArchiveMetadata(long streamId) {
        return streamArchiveControlManager.get(streamId);
    }
}
