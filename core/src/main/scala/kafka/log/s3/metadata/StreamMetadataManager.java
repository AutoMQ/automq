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

package kafka.log.s3.metadata;

import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import kafka.log.es.FutureUtil;
import kafka.server.BrokerServer;
import kafka.server.KafkaConfig;
import kafka.server.MetadataCache;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.metadata.stream.InRangeObjects;
import org.apache.kafka.metadata.stream.StreamOffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamMetadataManager implements InRangeObjectsFetcher {

    // TODO: optimize by more suitable concurrent protection
    private final static Logger LOGGER = LoggerFactory.getLogger(StreamMetadataManager.class);
    private final KafkaConfig config;
    private final BrokerServer broker;
    private final MetadataCache metadataCache;
    private final CatchUpMetadataListener catchUpMetadataListener;
    private final Map<Long/*stream id*/, Map<Long/*end offset*/, List<GetObjectsTask>>> pendingGetObjectsTasks;
    private final ExecutorService pendingExecutorService;
    private final ReplayedWalObjects replayedWalObjects;

    public StreamMetadataManager(BrokerServer broker, KafkaConfig config) {
        this.config = config;
        this.broker = broker;
        this.metadataCache = broker.metadataCache();
        this.catchUpMetadataListener = new CatchUpMetadataListener();
        this.broker.metadataListener().registerStreamMetadataListener(this.catchUpMetadataListener);
        this.replayedWalObjects = new ReplayedWalObjects();
        // TODO: optimize by more suitable data structure for pending tasks
        this.pendingGetObjectsTasks = new ConcurrentHashMap<>();
        this.pendingExecutorService = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("pending-get-objects-task-executor"));
    }

    @Override
    public CompletableFuture<InRangeObjects> fetch(long streamId, long startOffset, long endOffset, int limit) {
        return this.replayedWalObjects.fetch(streamId, startOffset, endOffset, limit);
    }

    public void retryPendingTasks(List<GetObjectsTask> tasks) {
        LOGGER.warn("[RetryPendingTasks]: retry tasks count: {}", tasks.size());
        tasks.forEach(task -> {
            long streamId = task.streamId;
            long startOffset = task.startOffset;
            long endOffset = task.endOffset;
            int limit = task.limit;
            CompletableFuture<InRangeObjects> newCf = this.fetch(streamId, startOffset, endOffset, limit);
            FutureUtil.propagate(newCf, task.cf);
        });
    }


    static class GetObjectsTask {

        private final CompletableFuture<InRangeObjects> cf;
        private final long streamId;
        private final long startOffset;
        private final long endOffset;
        private final int limit;

        public static GetObjectsTask of(long streamId, long startOffset, long endOffset, int limit) {
            CompletableFuture<InRangeObjects> cf = new CompletableFuture<>();
            GetObjectsTask task = new GetObjectsTask(cf, streamId, startOffset, endOffset, limit);
            return task;
        }

        private GetObjectsTask(CompletableFuture<InRangeObjects> cf, long streamId, long startOffset, long endOffset, int limit) {
            this.cf = cf;
            this.streamId = streamId;
            this.startOffset = startOffset;
            this.endOffset = endOffset;
            this.limit = limit;
        }

        void complete(InRangeObjects objects) {
            cf.complete(objects);
        }

        void completeExceptionally(Throwable ex) {
            cf.completeExceptionally(ex);
        }
    }

    class ReplayedWalObjects implements InRangeObjectsFetcher {

        @Override
        public CompletableFuture<InRangeObjects> fetch(long streamId, long startOffset, long endOffset, int limit) {
            StreamOffsetRange offsetRange = StreamMetadataManager.this.metadataCache.getStreamOffsetRange(streamId);
            if (offsetRange == null || offsetRange == StreamOffsetRange.INVALID) {
                return CompletableFuture.completedFuture(InRangeObjects.INVALID);
            }
            long streamStartOffset = offsetRange.getStartOffset();
            long streamEndOffset = offsetRange.getEndOffset();
            if (startOffset < streamStartOffset) {
                LOGGER.warn("[ReplayedWalObjects]: stream: {}, startOffset: {}, endOffset: {}, limit: {}, and startOffset < streamStartOffset: {}",
                    streamId, startOffset, endOffset, limit, streamStartOffset);
                return CompletableFuture.completedFuture(InRangeObjects.INVALID);
            }
            if (endOffset > streamEndOffset) {
                // lag behind, need to wait for cache catch up
                return pendingFetch(streamId, startOffset, endOffset, limit);
            }
            return fetch0(streamId, startOffset, endOffset, limit);
        }

        private CompletableFuture<InRangeObjects> pendingFetch(long streamId, long startOffset, long endOffset, int limit) {
            GetObjectsTask task = GetObjectsTask.of(streamId, startOffset, endOffset, limit);
            Map<Long, List<GetObjectsTask>> tasks = StreamMetadataManager.this.pendingGetObjectsTasks.computeIfAbsent(task.streamId,
                k -> new ConcurrentSkipListMap<>());
            List<GetObjectsTask> getObjectsTasks = tasks.computeIfAbsent(task.endOffset, k -> new ArrayList<>());
            getObjectsTasks.add(task);
            LOGGER.info("[PendingFetch]: stream: {}, startOffset: {}, endOffset: {}, limit: {}, and pending fetch", streamId, startOffset, endOffset,
                limit);
            return task.cf;
        }

        private CompletableFuture<InRangeObjects> fetch0(long streamId, long startOffset, long endOffset, int limit) {
            // only search in cache
            InRangeObjects cachedInRangeObjects = StreamMetadataManager.this.metadataCache.getObjects(streamId, startOffset, endOffset, limit);
            if (cachedInRangeObjects == null || cachedInRangeObjects == InRangeObjects.INVALID) {
                LOGGER.warn(
                    "[GetObjects]: stream: {}, startOffset: {}, endOffset: {}, limit: {}, and search in metadataCache failed with empty result",
                    streamId, startOffset, endOffset, limit);
                return CompletableFuture.completedFuture(InRangeObjects.INVALID);
            }
            LOGGER.info(
                "[GetObjects]: stream: {}, startOffset: {}, endOffset: {}, limit: {}, and search in metadataCache success with result: {}",
                streamId, startOffset, endOffset, limit, cachedInRangeObjects);
            return CompletableFuture.completedFuture(cachedInRangeObjects);
        }


    }

    public interface StreamMetadataListener {

        void onChange(MetadataDelta delta, MetadataImage image);
    }

    class CatchUpMetadataListener implements StreamMetadataListener {

        @Override
        public void onChange(MetadataDelta delta, MetadataImage newImage) {
            // TODO: pre filter unnecessary situations
            Set<Long> pendingStreams = pendingGetObjectsTasks.keySet();
            List<StreamOffsetRange> pendingStreamsOffsetRange = pendingStreams
                .stream()
                .map(metadataCache::getStreamOffsetRange)
                .filter(offset -> offset != StreamOffsetRange.INVALID)
                .collect(Collectors.toList());
            if (pendingStreamsOffsetRange.isEmpty()) {
                return;
            }
            List<GetObjectsTask> retryTasks = new ArrayList<>();
            pendingStreamsOffsetRange.forEach(offsetRange -> {
                long streamId = offsetRange.getStreamId();
                long endOffset = offsetRange.getEndOffset();
                Map<Long, List<GetObjectsTask>> tasks = StreamMetadataManager.this.pendingGetObjectsTasks.get(streamId);
                if (tasks == null) {
                    return;
                }
                Iterator<Entry<Long, List<GetObjectsTask>>> iterator =
                    tasks.entrySet().iterator();
                while (iterator.hasNext()) {
                    Entry<Long, List<GetObjectsTask>> entry = iterator.next();
                    long pendingEndOffset = entry.getKey();
                    if (pendingEndOffset > endOffset) {
                        break;
                    }
                    iterator.remove();
                    List<GetObjectsTask> getObjectsTasks = entry.getValue();
                    retryTasks.addAll(getObjectsTasks);
                }
                if (tasks.isEmpty()) {
                    StreamMetadataManager.this.pendingGetObjectsTasks.remove(streamId);
                }
            });
            pendingExecutorService.submit(() -> {
                retryPendingTasks(retryTasks);
            });
        }
    }

}
