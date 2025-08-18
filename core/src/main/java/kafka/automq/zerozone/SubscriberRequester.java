/*
 * Copyright 2025, AutoMQ HK Limited.
 *
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

package kafka.automq.zerozone;

import kafka.automq.partition.snapshot.SnapshotOperation;
import kafka.cluster.PartitionSnapshot;
import kafka.log.streamaspect.ElasticLogMeta;
import kafka.log.streamaspect.ElasticStreamSegmentMeta;
import kafka.log.streamaspect.SliceRange;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.AutomqGetPartitionSnapshotRequestData;
import org.apache.kafka.common.message.AutomqGetPartitionSnapshotResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.s3.AutomqGetPartitionSnapshotRequest;
import org.apache.kafka.common.requests.s3.AutomqGetPartitionSnapshotResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.common.automq.AutoMQVersion;
import org.apache.kafka.storage.internals.log.LogOffsetMetadata;
import org.apache.kafka.storage.internals.log.TimestampOffset;

import com.automq.stream.s3.wal.impl.DefaultRecordOffset;
import com.automq.stream.utils.Threads;
import com.automq.stream.utils.threads.EventLoop;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.netty.buffer.Unpooled;

@SuppressWarnings("NPathComplexity") class SubscriberRequester {
    private static final Logger LOGGER = LoggerFactory.getLogger(SubscriberRequester.class);
    private boolean closed = false;
    private long lastRequestTime;
    private int sessionId;
    private int sessionEpoch;
    boolean requestCommit = false;

    private final SnapshotReadPartitionsManager.Subscriber subscriber;
    private final Node node;
    private final AutoMQVersion version;
    private final AsyncSender asyncSender;
    private final Function<Uuid, String> topicNameGetter;
    private final EventLoop eventLoop;
    private final Time time;

    public SubscriberRequester(SnapshotReadPartitionsManager.Subscriber subscriber, Node node, AutoMQVersion version, AsyncSender asyncSender,
        Function<Uuid, String> topicNameGetter, EventLoop eventLoop, Time time) {
        this.subscriber = subscriber;
        this.node = node;
        this.version = version;
        this.asyncSender = asyncSender;
        this.topicNameGetter = topicNameGetter;
        this.eventLoop = eventLoop;
        this.time = time;
        request();
    }

    public void reset() {
        sessionId = 0;
        sessionEpoch = 0;
    }

    public void close() {
        closed = true;
    }

    private void request() {
        eventLoop.execute(this::request0);
    }

    private void request0() {
        if (closed) {
            return;
        }
        lastRequestTime = time.milliseconds();
        AutomqGetPartitionSnapshotRequestData data = new AutomqGetPartitionSnapshotRequestData().setSessionId(sessionId).setSessionEpoch(sessionEpoch).setVersion((short) 1);
        if (version.isZeroZoneV2Supported()) {
            data.setVersion((short) 1);
        }
        if (version.isZeroZoneV2Supported() && sessionEpoch == 0) {
            // request ConfirmWAL commit data to main storage, then the data that doesn't replay could be read from main storage.
            data.setRequestCommit(true);
        } else if (requestCommit) {
            requestCommit = false;
            data.setRequestCommit(true);
        }
        AutomqGetPartitionSnapshotRequest.Builder builder = new AutomqGetPartitionSnapshotRequest.Builder(data);
        asyncSender.sendRequest(node, builder)
            .thenAcceptAsync(rst -> {
                handleResponse(rst);
                subscriber.unsafeRun();
            }, eventLoop)
            .exceptionally(ex -> {
                LOGGER.error("[SNAPSHOT_SUBSCRIBE_ERROR],node={}", node, ex);
                return null;
            }).whenComplete((nil, ex) -> {
                long elapsed = time.milliseconds() - lastRequestTime;
                if (SnapshotReadPartitionsManager.REQUEST_INTERVAL_MS > elapsed) {
                    Threads.COMMON_SCHEDULER.schedule(() -> eventLoop.execute(this::request), SnapshotReadPartitionsManager.REQUEST_INTERVAL_MS - elapsed, TimeUnit.MILLISECONDS);
                } else {
                    request();
                }
            });
    }

    private void handleResponse(ClientResponse clientResponse) {
        if (closed) {
            return;
        }
        if (!clientResponse.hasResponse()) {
            if (clientResponse.wasDisconnected() || clientResponse.wasTimedOut()) {
                LOGGER.warn("[GET_SNAPSHOTS],[REQUEST_FAIL],response={}", clientResponse);
            } else {
                LOGGER.error("[GET_SNAPSHOTS],[NO_RESPONSE],response={}", clientResponse);
            }
            return;
        }
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("[GET_SNAPSHOTS],[RESPONSE],response={}", clientResponse);
        }
        AutomqGetPartitionSnapshotResponse zoneRouterResponse = (AutomqGetPartitionSnapshotResponse) clientResponse.responseBody();
        AutomqGetPartitionSnapshotResponseData resp = zoneRouterResponse.data();
        if (resp.errorCode() != Errors.NONE.code()) {
            LOGGER.error("[GET_SNAPSHOTS],[ERROR],response={}", resp);
            return;
        }
        if (resp.sessionId() != sessionId) {
            // switch to a new session
            subscriber.reset();
        }
        SnapshotReadPartitionsManager.OperationBatch batch = new SnapshotReadPartitionsManager.OperationBatch();
        resp.topics().forEach(topic -> topic.partitions().forEach(partition -> {
            String topicName = topicNameGetter.apply(topic.topicId());
            if (topicName == null) {
                subscriber.reset();
                throw new RuntimeException(String.format("Cannot find topic uuid=%s, the kraft metadata replay delay or the topic is deleted.", topic.topicId()));
            }
            batch.operations.add(convert(new TopicIdPartition(topic.topicId(), partition.partitionIndex(), topicName), partition));
        }));
        // Make sure the REMOVE operations will be applied first.
        batch.operations.sort((o1, o2) -> {
            int c1 = o1.operation.code() == SnapshotOperation.REMOVE.code() ? 0 : 1;
            int c2 = o2.operation.code() == SnapshotOperation.REMOVE.code() ? 0 : 1;
            return c1 - c2;
        });
        subscriber.onNewWalEndOffset(resp.confirmWalConfig(), DefaultRecordOffset.of(Unpooled.wrappedBuffer(resp.confirmWalEndOffset())));
        subscriber.onNewOperationBatch(batch);
        sessionId = resp.sessionId();
        sessionEpoch = resp.sessionEpoch();
    }

    static SnapshotReadPartitionsManager.SnapshotWithOperation convert(TopicIdPartition topicIdPartition,
        AutomqGetPartitionSnapshotResponseData.PartitionSnapshot src) {
        PartitionSnapshot.Builder snapshot = PartitionSnapshot.builder();
        snapshot.leaderEpoch(src.leaderEpoch());
        snapshot.logMeta(convert(src.logMetadata()));
        snapshot.firstUnstableOffset(convert(src.firstUnstableOffset()));
        snapshot.logEndOffset(convert(src.logEndOffset()));
        src.streamMetadata().forEach(m -> snapshot.streamEndOffset(m.streamId(), m.endOffset()));
        snapshot.lastTimestampOffset(convertTimestampOffset(src.lastTimestampOffset()));

        SnapshotOperation operation = SnapshotOperation.parse(src.operation());
        return new SnapshotReadPartitionsManager.SnapshotWithOperation(topicIdPartition, snapshot.build(), operation);
    }

    static ElasticLogMeta convert(AutomqGetPartitionSnapshotResponseData.LogMetadata src) {
        if (src == null || src.segments().isEmpty()) {
            // the AutomqGetPartitionSnapshotResponseData's default LogMetadata is an empty LogMetadata.
            return null;
        }
        ElasticLogMeta logMeta = new ElasticLogMeta();
        logMeta.setStreamMap(src.streamMap().stream().collect(Collectors.toMap(AutomqGetPartitionSnapshotResponseData.StreamMapping::name, AutomqGetPartitionSnapshotResponseData.StreamMapping::streamId)));
        src.segments().forEach(m -> logMeta.getSegmentMetas().add(convert(m)));
        return logMeta;
    }

    static ElasticStreamSegmentMeta convert(AutomqGetPartitionSnapshotResponseData.SegmentMetadata src) {
        ElasticStreamSegmentMeta meta = new ElasticStreamSegmentMeta();
        meta.baseOffset(src.baseOffset());
        meta.createTimestamp(src.createTimestamp());
        meta.lastModifiedTimestamp(src.lastModifiedTimestamp());
        meta.streamSuffix(src.streamSuffix());
        meta.logSize(src.logSize());
        meta.log(convert(src.log()));
        meta.time(convert(src.time()));
        meta.txn(convert(src.transaction()));
        meta.firstBatchTimestamp(src.firstBatchTimestamp());
        meta.timeIndexLastEntry(convert(src.timeIndexLastEntry()));
        return meta;
    }

    static SliceRange convert(AutomqGetPartitionSnapshotResponseData.SliceRange src) {
        return SliceRange.of(src.start(), src.end());
    }

    static ElasticStreamSegmentMeta.TimestampOffsetData convert(
        AutomqGetPartitionSnapshotResponseData.TimestampOffsetData src) {
        return ElasticStreamSegmentMeta.TimestampOffsetData.of(src.timestamp(), src.offset());
    }

    static TimestampOffset convertTimestampOffset(AutomqGetPartitionSnapshotResponseData.TimestampOffsetData src) {
        if (src == null) {
            return null;
        }
        return new TimestampOffset(src.timestamp(), src.offset());
    }

    static LogOffsetMetadata convert(AutomqGetPartitionSnapshotResponseData.LogOffsetMetadata src) {
        if (src == null) {
            return null;
        }
        // The segment offset should be fill in Partition#snapshot
        return new LogOffsetMetadata(src.messageOffset(), -1, src.relativePositionInSegment());
    }
}
