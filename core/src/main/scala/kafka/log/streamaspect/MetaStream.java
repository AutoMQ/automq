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

package kafka.log.streamaspect;

import kafka.log.streamaspect.reassignment.FastPartitionReassignmentManager;
import kafka.log.streamaspect.reassignment.MetaStreamHandoff;
import kafka.log.streamaspect.reassignment.MetaStreamHandoffRecord;
import kafka.log.streamaspect.reassignment.PartitionHandoff;
import kafka.log.streamaspect.reassignment.PartitionHandoffSendException;

import org.apache.kafka.common.Uuid;

import com.automq.stream.DefaultRecordBatch;
import com.automq.stream.api.AppendResult;
import com.automq.stream.api.FetchResult;
import com.automq.stream.api.ReadOptions;
import com.automq.stream.api.RecordBatch;
import com.automq.stream.api.RecordBatchWithContext;
import com.automq.stream.api.Stream;
import com.automq.stream.s3.context.AppendContext;
import com.automq.stream.s3.context.FetchContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.Unpooled;

/**
 * Meta stream is a wrapper of stream, it is used to record basic info of a topicPartition.
 * It serves as a kv stream.
 */
public class MetaStream implements Stream {
    public static final String LOG_META_KEY = "LOG";
    public static final String PRODUCER_SNAPSHOTS_META_KEY = "PRODUCER_SNAPSHOTS";
    public static final String PARTITION_META_KEY = "PARTITION";
    public static final String LEADER_EPOCH_CHECKPOINT_KEY = "LEADER_EPOCH_CHECKPOINT";
    public static final Logger LOGGER = LoggerFactory.getLogger(MetaStream.class);

    private static final double COMPACTION_HOLLOW_RATE = 0.6;
    private static final long COMPACTION_THRESHOLD_MS = TimeUnit.MINUTES.toMillis(1);

    private final Stream innerStream;
    private final ScheduledExecutorService scheduler;
    private final String logIdent;
    private final Uuid topicId;
    private final int partitionId;
    private final FastPartitionReassignmentManager fastReassignmentManager;
    /**
     * metaCache is used to cache meta key values.
     * key: meta key
     * value: pair of base offset and meta value
     */
    private volatile Map<String, MetadataValue> metaCache;

    /**
     * trimFuture is used to record a trim task. It may be cancelled and rescheduled.
     */
    private ScheduledFuture<?> compactionFuture;

    private final Set<CompletableFuture<?>> inflightMutations = new HashSet<>();
    private volatile State state = State.OPEN;
    private CompletableFuture<Void> closeFuture;

    /**
     * closed is used to record if the stream is fenced.
     */
    private volatile boolean fenced;

    /**
     * replayDone is used to record if the meta stream has been fully replayed.
     */
    private volatile boolean replayDone;

    /**
     * Creates a MetaStream with the broker-lifecycle fast reassignment seam used by close and replay.
     *
     * @param innerStream underlying stream
     * @param scheduler MetaStream compaction scheduler
     * @param logIdent log correlation prefix
     * @param topicId topic identity used to correlate a handoff
     * @param partitionId partition identity used to correlate a handoff
     * @param fastReassignmentManager lifecycle-owned handoff manager
     */
    public MetaStream(
        Stream innerStream,
        ScheduledExecutorService scheduler,
        String logIdent,
        Uuid topicId,
        int partitionId,
        FastPartitionReassignmentManager fastReassignmentManager
    ) {
        this.innerStream = innerStream;
        this.scheduler = scheduler;
        this.metaCache = new ConcurrentHashMap<>();
        this.logIdent = logIdent;
        this.topicId = topicId;
        this.partitionId = partitionId;
        this.fastReassignmentManager = fastReassignmentManager;
        this.replayDone = false;
    }

    @Override
    public long streamId() {
        return innerStream.streamId();
    }

    @Override
    public long streamEpoch() {
        return innerStream.streamEpoch();
    }

    @Override
    public long startOffset() {
        return innerStream.startOffset();
    }

    @Override
    public long confirmOffset() {
        return innerStream.confirmOffset();
    }

    @Override
    public void confirmOffset(long offset) {
        innerStream.confirmOffset(offset);
    }

    @Override
    public long nextOffset() {
        return innerStream.nextOffset();
    }

    @Override
    public CompletableFuture<AppendResult> append(AppendContext context, RecordBatch batch) {
        throw new UnsupportedOperationException("append record batch is not supported in meta stream");
    }

    public synchronized CompletableFuture<AppendResult> append(MetaKeyValue kv) {
        if (state != State.OPEN) {
            return frozenMutation("append");
        }
        return appendAndTrack(kv);
    }

    private CompletableFuture<AppendResult> appendAndTrack(MetaKeyValue kv) {
        CompletableFuture<AppendResult> appendFuture = appendEncoded(kv.getKey(), kv.getValue())
            .thenApply(result -> {
                tryCompaction();
                return result;
            });
        return trackMutation(appendFuture);
    }

    public AppendResult appendSync(MetaKeyValue kv) throws IOException {
        try {
            return append(kv).get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException) (e.getCause());
            } else {
                throw new RuntimeException(e.getCause());
            }
        }
    }

    /**
     * Append a batch of meta key values without trims.
     *
     * @return a future of append result
     */
    private CompletableFuture<AppendResult> appendEncoded(String key, ByteBuffer value) {
        ByteBuffer encoded = MetaKeyValue.encode(MetaKeyValue.of(key, value));
        CompletableFuture<AppendResult> appendFuture = innerStream.append(new DefaultRecordBatch(
            1, System.currentTimeMillis(), Collections.emptyMap(), encoded.duplicate()));
        return appendFuture.thenApply(result -> {
            synchronized (this) {
                MetadataValue newValue = new MetadataValue(result.baseOffset(), value);
                metaCache.compute(key, (ignored, current) -> current == null || current.offset < result.baseOffset()
                    ? newValue : current);
            }
            return result;
        });
    }

    @Override
    public CompletableFuture<FetchResult> fetch(FetchContext context, long startOffset, long endOffset, int maxBytesHint) {
        return innerStream.fetch(context, startOffset, endOffset, maxBytesHint);
    }

    @Override
    public synchronized CompletableFuture<Void> trim(long newStartOffset) {
        if (state != State.OPEN) {
            return frozenMutation("trim");
        }
        return trackMutation(innerStream.trim(newStartOffset));
    }

    @Override
    public CompletableFuture<Void> close() {
        return close(false);
    }

    /**
     * Freezes metadata and closes the inner stream, optionally preparing a reassignment handoff.
     *
     * @param fastClose true only when final partition metadata was persisted successfully and handoff is allowed
     * @return close completion
     */
    public synchronized CompletableFuture<Void> close(boolean fastClose) {
        if (closeFuture == null) {
            closeFuture = fastClose ? closeWithHandoff() : closeWithoutHandoff();
        }
        return closeFuture;
    }

    public boolean isFenced() {
        return fenced || state != State.OPEN;
    }

    @Override
    public synchronized CompletableFuture<Void> destroy() {
        if (compactionFuture != null) {
            compactionFuture.cancel(false);
        }
        return innerStream.destroy();
    }

    @Override
    public CompletableFuture<AppendResult> lastAppendFuture() {
        return innerStream.lastAppendFuture();
    }

    /**
     * Recovers the latest metadata, first using an exact fast-reassignment handoff when available and otherwise
     * replaying the metadata stream.
     *
     * @return decoded metadata cached by this MetaStream
     * @throws IOException when authoritative metadata-stream replay fails
     */
    public Map<String, Object> replay() throws IOException {
        long handoffEndOffset = nextOffset();
        PartitionHandoff.Key correlation = new PartitionHandoff.Key(topicId, partitionId, handoffEndOffset);
        Optional<PartitionHandoff> handoff = fastReassignmentManager.take(correlation);
        if (handoff.isPresent()) {
            try {
                Map<String, Object> metadata = replay(handoff.get().metaStreamHandoff());
                LOGGER.info("FAST_REASSIGNMENT_OPEN topicId={} partitionId={} handoffEndOffset={} result=handoff",
                    correlation.topicId(), correlation.partitionId(), correlation.metaStreamHandoffEndOffset());
                return metadata;
            } catch (RuntimeException exception) {
                LOGGER.warn("{} failed to replay prepared MetaStream handoff; falling back to the metadata stream",
                    logIdent, exception);
            }
        }
        return replayFromStream();
    }

    private synchronized Map<String, Object> replayFromStream() throws IOException {
        ensureOpen("replay");
        replayDone = false;
        metaCache.clear();
        boolean summaryEnabled = LOGGER.isDebugEnabled();
        StringBuilder sb = new StringBuilder();
        if (summaryEnabled) {
            sb.append(logIdent)
                    .append("metaStream replay summary:")
                    .append(" id: ")
                    .append(streamId())
                    .append(", ");
        }
        long totalValueSize = 0L;

        long startOffset = startOffset();
        long endOffset = nextOffset();
        long pos = startOffset;
        FetchContext fetchContext = new FetchContext();
        ReadOptions readOptions = ReadOptions.builder().prioritizedRead(true).build();
        fetchContext.setReadOptions(readOptions);

        try {
            while (pos < endOffset) {
                FetchResult fetchRst = fetch(fetchContext, pos, endOffset, 64 * 1024).get();
                for (RecordBatchWithContext context : fetchRst.recordBatchList()) {
                    try {
                        MetaKeyValue kv = MetaKeyValue.decode(Unpooled.copiedBuffer(context.rawPayload()).nioBuffer());
                        metaCache.put(kv.getKey(), new MetadataValue(context.baseOffset(), kv.getValue()));
                        totalValueSize += kv.getValue().remaining();
                        if (summaryEnabled) {
                            sb.append("(key: ").append(kv.getKey()).append(", offset: ").append(context.baseOffset()).append(", value size: ").append(kv.getValue().remaining()).append("); ");
                        }
                    } catch (Exception e) {
                        LOGGER.error("{} streamId {}: decode meta failed, offset: {}, error: {}", logIdent, streamId(), context.baseOffset(), e.getMessage());
                    }
                    pos = context.lastOffset();
                }
                fetchRst.free();
            }
            replayDone = true;
        } catch (ExecutionException e) {
            if (e.getCause() instanceof IOException) {
                fenced = true;
                throw (IOException) (e.getCause());
            } else {
                throw new RuntimeException(e.getCause());
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        if (totalValueSize > 0 && summaryEnabled) {
            LOGGER.debug(sb.append("total value size: ").append(totalValueSize).toString());
        }
        return getValidMetaMap();
    }

    /**
     * Replays a prepared handoff into isolated temporary state and publishes it only after every record can be decoded
     * and applied. Unknown metadata keys remain available as opaque values.
     *
     * @param handoff prepared latest-value MetaStream records
     * @return decoded metadata from the newly published state
     */
    synchronized Map<String, Object> replay(MetaStreamHandoff handoff) {
        ensureOpen("replay handoff");
        Map<String, MetadataValue> temporaryCache = new HashMap<>();
        for (MetaStreamHandoffRecord record : handoff.records()) {
            ByteBuffer encoded = record.encodedMetaKeyValue();
            MetaKeyValue keyValue = MetaKeyValue.decode(encoded.duplicate());
            MetadataValue metadataValue = new MetadataValue(record.baseOffset(), keyValue.getValue());
            temporaryCache.compute(keyValue.getKey(), (ignored, current) ->
                current == null || current.offset < record.baseOffset() ? metadataValue : current);
        }
        Map<String, Object> metadata = getValidMetaMap(temporaryCache);
        metaCache = new ConcurrentHashMap<>(temporaryCache);
        replayDone = true;
        return metadata;
    }

    public Optional<ByteBuffer> get(String key) {
        return Optional.ofNullable(metaCache.get(key)).map(o -> o.value.slice());
    }

    private Map<String, Object> getValidMetaMap() {
        return getValidMetaMap(metaCache);
    }

    private Map<String, Object> getValidMetaMap(Map<String, MetadataValue> metadataCache) {
        Map<String, Object> metaMap = new HashMap<>();
        metadataCache.forEach((key, value) -> {
            switch (key) {
                case LOG_META_KEY:
                    metaMap.put(key, ElasticLogMeta.decode(value.value()));
                    break;
                case PARTITION_META_KEY:
                    metaMap.put(key, ElasticPartitionMeta.decode(value.value()));
                    break;
                case PRODUCER_SNAPSHOTS_META_KEY:
                    metaMap.put(key, ElasticPartitionProducerSnapshotsMeta.decode(value.value()));
                    break;
                case LEADER_EPOCH_CHECKPOINT_KEY:
                    metaMap.put(key, ElasticLeaderEpochCheckpointMeta.decode(value.value()));
                    break;
                default:
                    metaMap.put(key, value.value().duplicate());
            }
        });
        return metaMap;
    }

    private synchronized void tryCompaction() {
        if (state != State.OPEN || compactionFuture != null) {
            return;
        }
        // trigger after 10s to avoid compacting too quick
        compactionFuture = scheduler.schedule(() -> {
            CompletableFuture<Void> compaction;
            synchronized (this) {
                if (state != State.OPEN) {
                    compactionFuture = null;
                    return;
                }
                compaction = trackMutation(doCompaction(false));
            }
            compaction.whenComplete((nil, exception) -> {
                synchronized (this) {
                    compactionFuture = null;
                }
                if (exception != null) {
                    LOGGER.error("{} MetaStream compaction failed", logIdent, exception);
                }
            });
        }, 10, TimeUnit.SECONDS);
    }

    @SuppressWarnings("checkstyle:NPathComplexity")
    private synchronized CompletableFuture<Void> doCompaction(boolean force) {
        if (!replayDone) {
            return CompletableFuture.completedFuture(null);
        }
        long startOffset = startOffset();
        long endOffset = nextOffset();
        int size = (int) (endOffset - startOffset);
        if (size == 0) {
            return CompletableFuture.completedFuture(null);
        }
        double hollowRate = 1 - (double) metaCache.size() / size;
        if (!force && hollowRate < COMPACTION_HOLLOW_RATE) {
            return CompletableFuture.completedFuture(null);
        }
        MetadataValue last = null;
        for (MetadataValue value : metaCache.values()) {
            if (last == null || value.offset > last.offset) {
                last = value;
            }
        }
        List<Map.Entry<String, MetadataValue>> overwrite = new LinkedList<>();
        for (Map.Entry<String, MetadataValue> entry : metaCache.entrySet()) {
            MetadataValue value = entry.getValue();
            if (value == last || (!force && last.timestamp - value.timestamp < COMPACTION_THRESHOLD_MS)) {
                continue;
            }
            overwrite.add(entry);
        }
        if (overwrite.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        CompletableFuture<Void> overwriteCf = CompletableFuture.allOf(overwrite.stream()
            .map(entry -> appendEncoded(entry.getKey(), entry.getValue().value()))
            .toArray(CompletableFuture[]::new));
        // await overwrite complete then trim to the minimum offset in metaCache
        return overwriteCf.thenCompose(nil -> {
            OptionalLong minOffset = metaCache.values().stream().mapToLong(v -> v.offset).min();
            return minOffset.isPresent()
                ? innerStream.trim(minOffset.getAsLong())
                : CompletableFuture.completedFuture(null);
        });
    }

    private synchronized <T> CompletableFuture<T> trackMutation(CompletableFuture<T> mutation) {
        inflightMutations.add(mutation);
        mutation.whenComplete((nil, ex) -> {
            synchronized (this) {
                inflightMutations.remove(mutation);
            }
        });
        return mutation;
    }

    private void ensureOpen(String mutation) {
        if (state != State.OPEN) {
            throw new IllegalStateException("MetaStream is frozen; cannot " + mutation);
        }
    }

    private <T> CompletableFuture<T> frozenMutation(String mutation) {
        return CompletableFuture.failedFuture(
            new IllegalStateException("MetaStream is frozen; cannot " + mutation));
    }

    private CompletableFuture<Void> closeWithoutHandoff() {
        return freezeMutations().handle((nil, exception) -> {
            if (exception != null) {
                LOGGER.warn("{} failed to drain MetaStream mutations during close; continuing without handoff",
                    logIdent, exception);
            }
            return null;
        }).thenCompose(nil -> closeAfterFallback());
    }

    private CompletableFuture<Void> closeWithHandoff() {
        return freezeAndSend().handle((correlation, exception) -> exception == null
            ? closeAfterHandoff(correlation)
            : closeAfterFallback()).thenCompose(future -> future);
    }

    private CompletableFuture<PartitionHandoff.Key> freezeAndSend() {
        return freeze().whenComplete((handoff, exception) -> {
            if (exception != null) {
                LOGGER.warn("{} failed to freeze MetaStream handoff; continuing with fallback close",
                    logIdent, exception);
            }
        }).thenCompose(handoff -> {
            PartitionHandoff partitionHandoff = new PartitionHandoff(
                topicId, partitionId, handoff);
            return fastReassignmentManager.send(partitionHandoff)
                .whenComplete((nil, exception) -> {
                    if (exception == null) {
                        LOGGER.info(
                            "FAST_REASSIGNMENT_PREPARE topicId={} partitionId={} handoffEndOffset={} "
                                + "result=success reason=none",
                            partitionHandoff.key().topicId(), partitionHandoff.key().partitionId(),
                            partitionHandoff.key().metaStreamHandoffEndOffset());
                    } else {
                        PartitionHandoffSendException failure = PartitionHandoffSendException.from(exception);
                        if (failure.reason() != PartitionHandoffSendException.Reason.NOT_ATTEMPTED) {
                            LOGGER.info(
                                "FAST_REASSIGNMENT_PREPARE topicId={} partitionId={} handoffEndOffset={} "
                                    + "result=fallback reason={}",
                                partitionHandoff.key().topicId(), partitionHandoff.key().partitionId(),
                                partitionHandoff.key().metaStreamHandoffEndOffset(), failure.reason().logValue());
                        }
                    }
                }).thenApply(nil -> partitionHandoff.key());
        });
    }

    /**
     * Permanently rejects new mutations, drains mutations already admitted, and captures one immutable handoff.
     * A failed freeze remains terminal and the MetaStream never becomes writable again.
     *
     * @return the handoff containing the latest record for each key and the matching exclusive end offset
     */
    synchronized CompletableFuture<MetaStreamHandoff> freeze() {
        return freezeMutations().thenApply(nil -> captureFrozenHandoff());
    }

    private synchronized CompletableFuture<Void> freezeMutations() {
        state = State.FREEZING;
        if (compactionFuture != null) {
            compactionFuture.cancel(false);
            compactionFuture = null;
        }
        CompletableFuture<Void> drainFuture = CompletableFuture.allOf(
            inflightMutations.toArray(new CompletableFuture<?>[0]));
        return drainFuture.whenComplete((nil, exception) -> {
            synchronized (this) {
                state = State.FROZEN;
            }
        });
    }

    private synchronized MetaStreamHandoff captureFrozenHandoff() {
        List<MetaStreamHandoffRecord> records = metaCache.entrySet().stream()
            .sorted(Comparator.comparingLong(entry -> entry.getValue().offset))
            .map(entry -> new MetaStreamHandoffRecord(entry.getValue().offset,
                MetaKeyValue.encode(MetaKeyValue.of(entry.getKey(), entry.getValue().value()))))
            .toList();
        return new MetaStreamHandoff(nextOffset(), records);
    }

    private CompletableFuture<Void> closeAfterHandoff(PartitionHandoff.Key correlation) {
        return innerStream.close().thenRun(() -> {
            fenced = true;
            LOGGER.info(
                "FAST_REASSIGNMENT_CLOSE topicId={} partitionId={} handoffEndOffset={} result=success reason=none",
                correlation.topicId(), correlation.partitionId(), correlation.metaStreamHandoffEndOffset());
        });
    }

    private CompletableFuture<Void> closeAfterFallback() {
        return forceCompactionForFallback().thenCompose(nil -> innerStream.close()).thenRun(() -> fenced = true);
    }

    private CompletableFuture<Void> forceCompactionForFallback() {
        return doCompaction(true).exceptionally(exception -> {
            LOGGER.warn("{} failed to force compact MetaStream during fallback close; continuing inner close",
                logIdent, exception);
            return null;
        });
    }

    static ByteBuffer copy(ByteBuffer source) {
        ByteBuffer copy = ByteBuffer.allocate(source.remaining());
        copy.put(source.duplicate());
        copy.flip();
        return copy;
    }

    static class MetadataValue {
        private final ByteBuffer value;
        final long offset;
        final long timestamp = System.currentTimeMillis();

        public MetadataValue(long offset, ByteBuffer value) {
            this.offset = offset;
            this.value = copy(value).asReadOnlyBuffer();
        }

        public ByteBuffer value() {
            return value.duplicate();
        }
    }

    private enum State {
        OPEN,
        FREEZING,
        FROZEN
    }
}
