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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
        this.pendingGetObjectsTasks = new HashMap<>();
        this.pendingExecutorService = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("pending-get-objects-task-executor"));
    }

    @Override
    public CompletableFuture<InRangeObjects> fetch(long streamId, long startOffset, long endOffset, int limit) {
        return this.replayedWalObjects.fetch(streamId, startOffset, endOffset, limit);
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
            synchronized (StreamMetadataManager.this) {
                Map<Long, List<GetObjectsTask>> tasks = StreamMetadataManager.this.pendingGetObjectsTasks.computeIfAbsent(task.streamId,
                    k -> new TreeMap<>());
                List<GetObjectsTask> getObjectsTasks = tasks.computeIfAbsent(task.endOffset, k -> new ArrayList<>());
                getObjectsTasks.add(task);
            }
            LOGGER.info("[PendingFetch]: stream: {}, startOffset: {}, endOffset: {}, limit: {}, and pending fetch", streamId, startOffset, endOffset, limit);
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
        }
    }

}
