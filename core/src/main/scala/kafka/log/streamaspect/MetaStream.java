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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
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
    /**
     * metaCache is used to cache meta key values.
     * key: meta key
     * value: pair of base offset and meta value
     */
    private final Map<String, MetadataValue> metaCache;

    /**
     * trimFuture is used to record a trim task. It may be cancelled and rescheduled.
     */
    private ScheduledFuture<?> compactionFuture;

    /**
     * closed is used to record if the stream is fenced.
     */
    private volatile boolean fenced;

    /**
     * replayDone is used to record if the meta stream has been fully replayed.
     */
    private volatile boolean replayDone;

    public MetaStream(Stream innerStream, ScheduledExecutorService scheduler, String logIdent) {
        this.innerStream = innerStream;
        this.scheduler = scheduler;
        this.metaCache = new ConcurrentHashMap<>();
        this.logIdent = logIdent;
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
        metaCache.put(kv.getKey(), new MetadataValue(nextOffset(), kv.getValue()));
        return append0(kv).thenApply(result -> {
            tryCompaction();
            return result;
        });
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
    private synchronized CompletableFuture<AppendResult> append0(MetaKeyValue kv) {
        return innerStream.append(new DefaultRecordBatch(1, System.currentTimeMillis(), Collections.emptyMap(), MetaKeyValue.encode(kv)));
    }

    @Override
    public CompletableFuture<FetchResult> fetch(FetchContext context, long startOffset, long endOffset, int maxBytesHint) {
        return innerStream.fetch(context, startOffset, endOffset, maxBytesHint);
    }

    @Override
    public CompletableFuture<Void> trim(long newStartOffset) {
        return innerStream.trim(newStartOffset);
    }

    @Override
    public CompletableFuture<Void> close() {
        if (compactionFuture != null) {
            compactionFuture.cancel(true);
        }
        return doCompaction(true)
                .thenRun(innerStream::close)
                .thenRun(() -> fenced = true);
    }

    public boolean isFenced() {
        return fenced;
    }

    @Override
    public CompletableFuture<Void> destroy() {
        if (compactionFuture != null) {
            compactionFuture.cancel(true);
        }
        return innerStream.destroy();
    }

    @Override
    public CompletableFuture<AppendResult> lastAppendFuture() {
        return innerStream.lastAppendFuture();
    }

    /**
     * Replay meta stream and return a map of meta keyValues. KeyValues will be cached in metaCache.
     *
     * @return meta keyValues map
     */
    public Map<String, Object> replay() throws IOException {
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

    public Optional<ByteBuffer> get(String key) {
        return Optional.ofNullable(metaCache.get(key)).map(o -> o.value.slice());
    }

    private Map<String, Object> getValidMetaMap() {
        Map<String, Object> metaMap = new HashMap<>();
        metaCache.forEach((key, value) -> {
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

    private void tryCompaction() {
        if (compactionFuture != null) {
            return;
        }
        // trigger after 10s to avoid compacting too quick
        compactionFuture = scheduler.schedule(() -> {
            doCompaction(false);
            this.compactionFuture = null;
        }, 10, TimeUnit.SECONDS);
    }

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
        List<MetaKeyValue> overwrite = new LinkedList<>();
        for (Map.Entry<String, MetadataValue> entry : metaCache.entrySet()) {
            String key = entry.getKey();
            MetadataValue value = entry.getValue();
            if (value == last || (!force && last.timestamp - value.timestamp < COMPACTION_THRESHOLD_MS)) {
                continue;
            }
            overwrite.add(MetaKeyValue.of(key, value.value()));
        }
        if (overwrite.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        CompletableFuture<Void> overwriteCf = CompletableFuture.allOf(overwrite.stream().map(this::append).toArray(CompletableFuture[]::new));
        OptionalLong minOffset = metaCache.values().stream().mapToLong(v -> v.offset).min();
        // await overwrite complete then trim to the minimum offset in metaCache
        return overwriteCf.thenAccept(nil -> minOffset.ifPresent(this::trim));
    }

    static class MetadataValue {
        private final ByteBuffer value;
        final long offset;
        final long timestamp = System.currentTimeMillis();

        public MetadataValue(long offset, ByteBuffer value) {
            this.offset = offset;
            this.value = value;
        }

        public ByteBuffer value() {
            return value.duplicate();
        }
    }
}
