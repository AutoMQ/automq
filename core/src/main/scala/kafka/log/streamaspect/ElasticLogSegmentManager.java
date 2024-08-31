/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.log.streamaspect;

import com.automq.stream.api.Stream;
import com.automq.stream.utils.Threads;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import org.apache.kafka.storage.internals.log.LogOffsetMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticLogSegmentManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticLogSegmentManager.class);
    /**
     * The lock of {@link #segments} and {@link #inflightCleanedSegments}
     */
    private final ReentrantLock segmentLock = new ReentrantLock();
    private final Map<Long, ElasticLogSegment> segments = new HashMap<>();
    private final Map<Long, ElasticLogSegment> inflightCleanedSegments = new HashMap<>();
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
        segmentLock.lock();
        try {
            segments.put(baseOffset, segment);
            inflightCleanedSegments.remove(baseOffset, segment);
        } finally {
            segmentLock.unlock();
        }
    }

    public void putInflightCleaned(long baseOffset, ElasticLogSegment segment) {
        segmentLock.lock();
        try {
            inflightCleanedSegments.put(baseOffset, segment);
        } finally {
            segmentLock.unlock();
        }
    }

    public CompletableFuture<Void> create(long baseOffset, ElasticLogSegment segment) {
        LogOffsetMetadata offset = new LogOffsetMetadata(baseOffset, baseOffset, 0);
        while (!offsetUpperBound.compareAndSet(null, offset)) {
            LOGGER.info("{} try create new segment with offset $baseOffset, wait last segment meta persisted.", logIdent);
            Threads.sleep(1L);
        }
        segmentLock.lock();
        try {
            segments.put(baseOffset, segment);
        } finally {
            segmentLock.unlock();
        }
        return asyncPersistLogMeta().thenAccept(nil -> {
            offsetUpperBound.set(null);
        });
    }

    public ElasticLogSegment remove(long baseOffset) {
        segmentLock.lock();
        try {
            return segments.remove(baseOffset);
        } finally {
            segmentLock.unlock();
        }
    }

    public ElasticLogMeta persistLogMeta() {
        try {
            return asyncPersistLogMeta().get();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    public CompletableFuture<ElasticLogMeta> asyncPersistLogMeta() {
        ElasticLogMeta meta;
        Map<String, Long> trimOffsets;

        segmentLock.lock();
        try {
            Map<String, Stream> streams = streamManager.streams();
            List<ElasticStreamSegmentMeta> segmentList = segments.values().stream()
                .sorted()
                .map(ElasticLogSegment::meta)
                .collect(Collectors.toList());

            meta = logMeta(streams, segmentList);
            // We calculate trimOffsets in the lock to ensure that no more new stream with data is created during the calculation.
            trimOffsets = calTrimOffset(
                streams,
                segmentList.iterator(),
                inflightCleanedSegments.values().stream().map(ElasticLogSegment::meta).iterator()
            );
        } finally {
            segmentLock.unlock();
        }

        MetaKeyValue kv = MetaKeyValue.of(MetaStream.LOG_META_KEY, ElasticLogMeta.encode(meta));
        return metaStream.append(kv).thenApply(nil -> {
            LOGGER.info("{} save log meta {}", logIdent, meta);
            trimStream(trimOffsets);
            return meta;
        }).whenComplete((nil, ex) -> {
            if (ex != null) {
                LOGGER.error("{} persist log meta {} fail", logIdent, meta, ex);
            }
        });
    }

    private void trimStream(Map<String, Long> trimOffsets) {
        try {
            trimStream0(trimOffsets);
        } catch (Throwable e) {
            LOGGER.error("{} trim stream failed", logIdent, e);
        }
    }

    private void trimStream0(Map<String, Long> trimOffsets) {
        streamManager.streams().forEach((streamName, stream) -> {
            Long trimOffset = trimOffsets.get(streamName);
            if (trimOffset != null && trimOffset > stream.startOffset()) {
                stream.trim(trimOffset);
            }
        });
    }

    /**
     * Calculate trim offset of each stream.
     *
     * @return stream trim offset map, key is stream name, value is trim offset.
     */
    private static Map<String, Long> calTrimOffset(Map<String, Stream> streams,
        Iterator<ElasticStreamSegmentMeta> segments, Iterator<ElasticStreamSegmentMeta> inflightSegments) {
        Map<String, Long> trimOffsets = new HashMap<>();

        inflightSegments.forEachRemaining(segMeta -> calTrimOffset(trimOffsets, segMeta));
        segments.forEachRemaining(segMeta -> calTrimOffset(trimOffsets, segMeta));

        streams.forEach((streamName, stream) -> {
            // if we haven't seen a stream before, then it is not used by any segment, and should be trimmed to the end.
            if (!trimOffsets.containsKey(streamName)) {
                trimOffsets.put(streamName, stream.nextOffset());
            }
        });
        return trimOffsets;
    }

    private static void calTrimOffset(Map<String, Long> streamMinOffsets, ElasticStreamSegmentMeta segMeta) {
        streamMinOffsets.compute("log" + segMeta.streamSuffix(), (k, v) -> Math.min(segMeta.log().start(), Optional.ofNullable(v).orElse(Long.MAX_VALUE)));
        streamMinOffsets.compute("tim" + segMeta.streamSuffix(), (k, v) -> Math.min(segMeta.time().start(), Optional.ofNullable(v).orElse(Long.MAX_VALUE)));
        streamMinOffsets.compute("txn" + segMeta.streamSuffix(), (k, v) -> Math.min(segMeta.txn().start(), Optional.ofNullable(v).orElse(Long.MAX_VALUE)));
    }

    public ElasticLogSegmentEventListener logSegmentEventListener() {
        return segmentEventListener;
    }

    public static ElasticLogMeta logMeta(Map<String, Stream> streams, List<ElasticStreamSegmentMeta> segmentList) {
        ElasticLogMeta elasticLogMeta = new ElasticLogMeta();
        Map<String, Long> streamMap = new HashMap<>();
        streams.forEach((streamName, stream) -> streamMap.put(streamName, stream.streamId()));
        elasticLogMeta.setStreamMap(streamMap);
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
