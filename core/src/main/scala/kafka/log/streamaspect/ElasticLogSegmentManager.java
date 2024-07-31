/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.log.streamaspect;

import com.automq.stream.utils.Threads;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import org.apache.kafka.storage.internals.log.LogOffsetMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticLogSegmentManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticLogSegmentManager.class);
    private final ConcurrentMap<Long, ElasticLogSegment> segments = new ConcurrentHashMap<>();
    private final ConcurrentMap<Long, ElasticLogSegment> inflightCleanedSegments = new ConcurrentHashMap<>();
    private final EventListener segmentEventListener = new EventListener();
    final AtomicReference<LogOffsetMetadata> offsetUpperBound = new AtomicReference<>();

    private final MetaStream metaStream;
    private final ElasticLogStreamManager streamManager;
    private final String logIdent;

    public ElasticLogSegmentManager(MetaStream metaStream, ElasticLogStreamManager streamManager, String logIdent) {
        this.metaStream = metaStream;
        this.streamManager = streamManager;
        this.logIdent = logIdent;
    }

    public void put(long baseOffset, ElasticLogSegment segment) {
        segments.put(baseOffset, segment);
        inflightCleanedSegments.remove(baseOffset, segment);
    }

    public void putInflightCleaned(long baseOffset, ElasticLogSegment segment) {
        inflightCleanedSegments.put(baseOffset, segment);
    }

    public CompletableFuture<Void> create(long baseOffset, ElasticLogSegment segment) {
        LogOffsetMetadata offset = new LogOffsetMetadata(baseOffset, baseOffset, 0);
        while (!offsetUpperBound.compareAndSet(null, offset)) {
            LOGGER.info("{} try create new segment with offset $baseOffset, wait last segment meta persisted.", logIdent);
            Threads.sleep(1L);
        }
        segments.put(baseOffset, segment);
        return asyncPersistLogMeta().thenAccept(nil -> {
            offsetUpperBound.set(null);
        });
    }

    public ElasticLogSegment remove(long baseOffset) {
        return segments.remove(baseOffset);
    }

    public ElasticLogMeta persistLogMeta() {
        try {
            return asyncPersistLogMeta().get();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    public CompletableFuture<ElasticLogMeta> asyncPersistLogMeta() {
        ElasticLogMeta meta = logMeta();
        MetaKeyValue kv = MetaKeyValue.of(MetaStream.LOG_META_KEY, ElasticLogMeta.encode(meta));
        return metaStream.append(kv).thenApply(nil -> {
            LOGGER.info("{} save log meta {}", logIdent, meta);
            trimStream(meta);
            return meta;
        }).whenComplete((nil, ex) -> {
            if (ex != null) {
                LOGGER.error("{} persist log meta {} fail", logIdent, meta, ex);
            }
        });
    }

    private void trimStream(ElasticLogMeta meta) {
        try {
            trimStream0(meta);
        } catch (Throwable e) {
            LOGGER.error("{} trim stream failed", logIdent, e);
        }
    }

    private void trimStream0(ElasticLogMeta meta) {
        Map<String, Long> streamMinOffsets = new HashMap<>();
        inflightCleanedSegments.forEach((offset, segment) -> {
            ElasticStreamSegmentMeta segMeta = segment.meta();
            calStreamsMinOffset(streamMinOffsets, segMeta);
        });
        for (ElasticStreamSegmentMeta segMeta : meta.getSegmentMetas()) {
            calStreamsMinOffset(streamMinOffsets, segMeta);
        }

        streamManager.streams().forEach((streamName, stream) -> {
            var minOffset = streamMinOffsets.get(streamName);
            // if minOffset == null, then stream is not used by any segment, should trim it to end.
            minOffset = Optional.ofNullable(minOffset).orElse(stream.nextOffset());
            if (minOffset > stream.startOffset()) {
                stream.trim(minOffset);
            }
        });
    }

    private void calStreamsMinOffset(Map<String, Long> streamMinOffsets, ElasticStreamSegmentMeta segMeta) {
        streamMinOffsets.compute("log" + segMeta.streamSuffix(), (k, v) -> Math.min(segMeta.log().start(), Optional.ofNullable(v).orElse(Long.MAX_VALUE)));
        streamMinOffsets.compute("tim" + segMeta.streamSuffix(), (k, v) -> Math.min(segMeta.time().start(), Optional.ofNullable(v).orElse(Long.MAX_VALUE)));
        streamMinOffsets.compute("txn" + segMeta.streamSuffix(), (k, v) -> Math.min(segMeta.txn().start(), Optional.ofNullable(v).orElse(Long.MAX_VALUE)));
    }

    public ElasticLogSegmentEventListener logSegmentEventListener() {
        return segmentEventListener;
    }

    public ElasticLogMeta logMeta() {
        ElasticLogMeta elasticLogMeta = new ElasticLogMeta();
        Map<String, Long> streamMap = new HashMap<>();
        streamManager.streams().forEach((streamName, stream) -> streamMap.put(streamName, stream.streamId()));
        elasticLogMeta.setStreamMap(streamMap);
        List<ElasticStreamSegmentMeta> segmentList = segments.values().stream().sorted().map(ElasticLogSegment::meta).collect(Collectors.toList());
        elasticLogMeta.setSegmentMetas(segmentList);
        return elasticLogMeta;
    }

    class EventListener implements ElasticLogSegmentEventListener {
        public static final long NO_OP_OFFSET = -1L;
        private final Queue<Long> pendingDeleteSegmentBaseOffset = new ConcurrentLinkedQueue<>();
        private volatile CompletableFuture<ElasticLogMeta> pendingPersistentMetaCf = null;

        @Override
        public void onEvent(long segmentBaseOffset, ElasticLogSegmentEvent event) {
            switch (event) {
                case SEGMENT_DELETE: {
                    boolean deleted = remove(segmentBaseOffset) != null;
                    if (deleted) {
                        // This may happen since kafka.log.LocalLog.deleteSegmentFiles schedules the delayed deletion task.
                        if (metaStream.isFenced()) {
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug("{} meta stream is closed, skip persisting log meta", logIdent);
                            }
                        } else {
                            submitOrDrainPendingPersistentMetaQueue(segmentBaseOffset);
                        }
                    }
                    break;
                }
                case SEGMENT_UPDATE: {
                    persistLogMeta();
                    break;
                }
                default: {
                    throw new IllegalStateException("Unsupported event " + event);
                }
            }
        }

        @VisibleForTesting
        Queue<Long> getPendingDeleteSegmentQueue() {
            return pendingDeleteSegmentBaseOffset;
        }

        @VisibleForTesting
        synchronized CompletableFuture<ElasticLogMeta> getPendingPersistentMetaCf() {
            return pendingPersistentMetaCf;
        }

        private void submitOrDrainPendingPersistentMetaQueue(long segmentBaseOffset) {
            if (segmentBaseOffset != NO_OP_OFFSET) {
                pendingDeleteSegmentBaseOffset.add(segmentBaseOffset);
            }

            synchronized (this) {
                if (pendingPersistentMetaCf != null && !pendingPersistentMetaCf.isDone()) {
                    return;
                }

                long maxOffset = NO_OP_OFFSET;

                while (!pendingDeleteSegmentBaseOffset.isEmpty()) {
                    long baseOffset = pendingDeleteSegmentBaseOffset.poll();
                    maxOffset = Math.max(maxOffset, baseOffset);
                }

                if (maxOffset != NO_OP_OFFSET) {
                    if (metaStream.isFenced()) {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("{} meta stream is closed, skip persisting log meta", logIdent);
                        }

                        return;
                    }

                    long finalMaxOffset = maxOffset;
                    pendingPersistentMetaCf = asyncPersistLogMeta();
                    pendingPersistentMetaCf.whenCompleteAsync((res, e) -> {
                        if (e != null) {
                            LOGGER.error("error when persisLogMeta maxOffset {}", finalMaxOffset, e);
                        }

                        submitOrDrainPendingPersistentMetaQueue(-1);
                    });
                }
            }
        }
    }

}
