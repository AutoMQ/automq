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

package kafka.log.streamaspect;

import com.automq.stream.api.ReadOptions;
import io.netty.buffer.Unpooled;
import com.automq.stream.api.AppendResult;
import com.automq.stream.api.FetchResult;
import com.automq.stream.api.RecordBatch;
import com.automq.stream.api.RecordBatchWithContext;
import com.automq.stream.api.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Meta stream is a wrapper of stream, it is used to record basic info of a topicPartition.
 * It serves as a kv stream.
 */
public class MetaStream implements Stream {
    public static final String LOG_META_KEY = "LOG";
    public static final String PRODUCER_SNAPSHOTS_META_KEY = "PRODUCER_SNAPSHOTS";
    public static final String PRODUCER_SNAPSHOT_KEY_PREFIX = "PRODUCER_SNAPSHOT_";
    public static final String PARTITION_META_KEY = "PARTITION";
    public static final String LEADER_EPOCH_CHECKPOINT_KEY = "LEADER_EPOCH_CHECKPOINT";
    public static final Logger LOGGER = LoggerFactory.getLogger(MetaStream.class);

    private final Stream innerStream;
    private final ScheduledExecutorService trimScheduler;
    private final String logIdent;
    /**
     * metaCache is used to cache meta key values.
     * key: meta key
     * value: pair of base offset and meta value
     */
    private final Map<String, Pair<Long, ByteBuffer>> metaCache;

    /**
     * trimFuture is used to record a trim task. It may be cancelled and rescheduled.
     */
    private ScheduledFuture<?> trimFuture;

    /**
     * closed is used to record if the stream is fenced.
     */
    private volatile boolean fenced;

    public MetaStream(Stream innerStream, ScheduledExecutorService trimScheduler, String logIdent) {
        this.innerStream = innerStream;
        this.trimScheduler = trimScheduler;
        this.metaCache = new ConcurrentHashMap<>();
        this.logIdent = logIdent;
    }

    @Override
    public long streamId() {
        return innerStream.streamId();
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
    public long nextOffset() {
        return innerStream.nextOffset();
    }

    @Override
    public CompletableFuture<AppendResult> append(RecordBatch batch) {
        throw new UnsupportedOperationException("append record batch is not supported in meta stream");
    }

    public CompletableFuture<AppendResult> append(MetaKeyValue kv) {
        return append0(kv).thenApply(result -> {
            metaCache.put(kv.getKey(), Pair.of(result.baseOffset(), kv.getValue()));
            trimAsync();
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
    private CompletableFuture<AppendResult> append0(MetaKeyValue kv) {
        return innerStream.append(RawPayloadRecordBatch.of(MetaKeyValue.encode(kv)));
    }

    @Override
    public CompletableFuture<FetchResult> fetch(long startOffset, long endOffset, int maxBytesHint, ReadOptions readOptions) {
        return innerStream.fetch(startOffset, endOffset, maxBytesHint, readOptions);
    }

    @Override
    public CompletableFuture<Void> trim(long newStartOffset) {
        return innerStream.trim(newStartOffset);
    }

    @Override
    public CompletableFuture<Void> close() {
        if (trimFuture != null) {
            trimFuture.cancel(true);
        }
        return doCompaction()
                .thenAccept(sb -> {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(sb.toString());
                    }
                })
                .thenRun(innerStream::close)
                .thenRun(() -> fenced = true);
    }

    public boolean isFenced() {
        return fenced;
    }

    @Override
    public CompletableFuture<Void> destroy() {
        if (trimFuture != null) {
            trimFuture.cancel(true);
        }
        return innerStream.destroy();
    }

    /**
     * Replay meta stream and return a map of meta keyValues. KeyValues will be cached in metaCache.
     *
     * @return meta keyValues map
     */
    public Map<String, Object> replay() throws IOException {
        metaCache.clear();
        StringBuilder sb = new StringBuilder(logIdent)
                .append("metaStream replay summary:")
                .append(" id: ")
                .append(streamId())
                .append(", ");
        long totalValueSize = 0L;

        long startOffset = startOffset();
        long endOffset = nextOffset();
        long pos = startOffset;

        try {
            while (pos < endOffset) {
                FetchResult fetchRst = fetch(pos, endOffset, 64 * 1024).get();
                for (RecordBatchWithContext context : fetchRst.recordBatchList()) {
                    try {
                        MetaKeyValue kv = MetaKeyValue.decode(Unpooled.copiedBuffer(context.rawPayload()).nioBuffer());
                        metaCache.put(kv.getKey(), Pair.of(context.baseOffset(), kv.getValue()));
                        totalValueSize += kv.getValue().remaining();
                        sb.append("(key: ").append(kv.getKey()).append(", offset: ").append(context.baseOffset()).append(", value size: ").append(kv.getValue().remaining()).append("); ");
                    } catch (Exception e) {
                        LOGGER.error("{} streamId {}: decode meta failed, offset: {}, error: {}", logIdent, streamId(), context.baseOffset(), e.getMessage());
                    }
                    pos = context.lastOffset();
                }
                fetchRst.free();
            }
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

        if (totalValueSize > 0 && LOGGER.isDebugEnabled()) {
            LOGGER.debug(sb.append("total value size: ").append(totalValueSize).toString());
        }
        return getValidMetaMap();
    }

    public Map<Long, ElasticPartitionProducerSnapshotMeta> getAllProducerSnapshots() {
        if (!metaCache.containsKey(PRODUCER_SNAPSHOTS_META_KEY)) {
            return Collections.emptyMap();
        }
        Map<Long, ElasticPartitionProducerSnapshotMeta> snapshots = new HashMap<>();
        ElasticPartitionProducerSnapshotsMeta snapshotsMeta = ElasticPartitionProducerSnapshotsMeta.decode(metaCache.get(PRODUCER_SNAPSHOTS_META_KEY).getRight().duplicate());
        snapshotsMeta.getSnapshots().forEach(offset -> {
            String key = PRODUCER_SNAPSHOT_KEY_PREFIX + offset;
            if (!metaCache.containsKey(key)) {
                throw new RuntimeException("Missing producer snapshot meta for offset " + offset);
            }
            snapshots.put(offset, ElasticPartitionProducerSnapshotMeta.decode(metaCache.get(key).getRight().duplicate()));
        });
        return snapshots;
    }

    private Map<String, Object> getValidMetaMap() {
        Map<String, Object> metaMap = new HashMap<>();
        metaCache.forEach((key, pair) -> {
            switch (key) {
                case LOG_META_KEY:
                    metaMap.put(key, ElasticLogMeta.decode(pair.getRight().duplicate()));
                    break;
                case PARTITION_META_KEY:
                    metaMap.put(key, ElasticPartitionMeta.decode(pair.getRight().duplicate()));
                    break;
                case PRODUCER_SNAPSHOTS_META_KEY:
                    metaMap.put(key, ElasticPartitionProducerSnapshotsMeta.decode(pair.getRight().duplicate()));
                    break;
                case LEADER_EPOCH_CHECKPOINT_KEY:
                    metaMap.put(key, ElasticLeaderEpochCheckpointMeta.decode(pair.getRight().duplicate()));
                    break;
                default:
                    if (key.startsWith(PRODUCER_SNAPSHOT_KEY_PREFIX)) {
                        metaMap.put(key, ElasticPartitionProducerSnapshotMeta.decode(pair.getRight().duplicate()));
                    } else {
                        LOGGER.error("{} streamId {}: unknown meta key: {}", logIdent, streamId(), key);
                    }
            }
        });
        return metaMap;
    }

    private void trimAsync() {
        if (trimFuture != null) {
            trimFuture.cancel(true);
        }
        // trigger after 10 SECONDS to avoid successive trims
        trimFuture = trimScheduler.schedule(this::doCompaction, 10, TimeUnit.SECONDS);
    }

    private CompletableFuture<StringBuilder> doCompaction() {
        if (metaCache.size() <= 1) {
            return CompletableFuture.completedFuture(null);
        }

        List<CompletableFuture<AppendResult>> futures = new ArrayList<>();
        Set<Long> validSnapshots = metaCache.get(PRODUCER_SNAPSHOTS_META_KEY) == null ? Collections.emptySet() :
                ElasticPartitionProducerSnapshotsMeta.decode(metaCache.get(PRODUCER_SNAPSHOTS_META_KEY).getRight().duplicate()).getSnapshots();

        long lastOffset = 0L;
        StringBuilder sb = new StringBuilder(logIdent)
                .append("metaStream compaction summary:")
                .append(" id: ")
                .append(streamId())
                .append(", ");
        long totalValueSize = 0L;

        Iterator<Map.Entry<String, Pair<Long, ByteBuffer>>> iterator = metaCache.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Pair<Long, ByteBuffer>> entry = iterator.next();
            // remove invalid producer snapshots
            if (entry.getKey().startsWith(PRODUCER_SNAPSHOT_KEY_PREFIX)) {
                long offset = parseProducerSnapshotOffset(entry.getKey());
                if (!validSnapshots.contains(offset)) {
                    iterator.remove();
                    continue;
                }
            }
            if (lastOffset < entry.getValue().getLeft()) {
                lastOffset = entry.getValue().getLeft();
            }
            totalValueSize += entry.getValue().getRight().remaining();
            sb.append("(key: ").append(entry.getKey()).append(", offset: ").append(entry.getValue().getLeft()).append(", value size: ").append(entry.getValue().getRight().remaining()).append("); ");
            futures.add(append0(MetaKeyValue.of(entry.getKey(), entry.getValue().getRight().duplicate())));
        }

        final long finalLastOffset = lastOffset;
        sb.append("compact before: ").append(finalLastOffset + 1).append(", total value size: ").append(totalValueSize);
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenCompose(result -> trim(finalLastOffset + 1))
                .thenApply(result -> sb);
    }

    private long parseProducerSnapshotOffset(String key) {
        if (!key.startsWith(PRODUCER_SNAPSHOT_KEY_PREFIX)) {
            throw new IllegalArgumentException("Invalid producer snapshot key: " + key);
        }
        String[] split = key.split("_");
        return Long.parseLong(split[split.length - 1]);
    }
}
