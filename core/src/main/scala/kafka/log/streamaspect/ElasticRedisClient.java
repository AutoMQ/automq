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

import com.automq.stream.RecordBatchWithContextWrapper;
import com.automq.stream.api.AppendResult;
import com.automq.stream.api.Client;
import com.automq.stream.api.CreateStreamOptions;
import com.automq.stream.api.FetchResult;
import com.automq.stream.api.KVClient;
import com.automq.stream.api.KeyValue;
import com.automq.stream.api.OpenStreamOptions;
import com.automq.stream.api.RecordBatch;
import com.automq.stream.api.RecordBatchWithContext;
import com.automq.stream.api.Stream;
import com.automq.stream.api.StreamClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPooled;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class ElasticRedisClient implements Client {
    private static final Logger log = LoggerFactory.getLogger(ElasticRedisClient.class);
    private final JedisPooled jedis;
    private final StreamClient streamClient;
    private final KVClient kvClient;
    private final ExecutorService executors = Executors.newSingleThreadExecutor();

    public ElasticRedisClient(String endpoint) {
        String[] parts = endpoint.split(":");
        this.jedis = new JedisPooled(parts[0], Integer.parseInt(parts[1]));
        this.streamClient = new StreamClientImpl(jedis);
        this.kvClient = new KVClientImpl(jedis);
    }

    @Override
    public void start() {
        // do nothing
    }

    @Override
    public void shutdown() {
        // do nothing
    }

    @Override
    public StreamClient streamClient() {
        return streamClient;
    }

    @Override
    public KVClient kvClient() {
        return kvClient;
    }


    class StreamImpl implements Stream {
        private static final String STREAM_CONTENT_PREFIX_IN_REDIS = "S_";
        private static final String STREAM_CONTENT_RECORD_MAP_PREFIX_IN_REDIS = STREAM_CONTENT_PREFIX_IN_REDIS + "RM_";
        private static final String STREAM_CONTENT_NEXT_OFFSET_PREFIX_IN_REDIS = STREAM_CONTENT_PREFIX_IN_REDIS + "NO_";
        private final AtomicLong nextOffsetAlloc;
        private final long streamId;
        private final String recordMapKey;
        private final String offsetKey;
        private final JedisPooled jedis;
        private NavigableMap<Long, RecordBatchWithContext> recordMap = new ConcurrentSkipListMap<>();

        public StreamImpl(JedisPooled jedis, long streamId) {
            this.jedis = jedis;
            this.streamId = streamId;
            this.offsetKey = STREAM_CONTENT_NEXT_OFFSET_PREFIX_IN_REDIS + streamId;
            this.recordMapKey = STREAM_CONTENT_RECORD_MAP_PREFIX_IN_REDIS + streamId;

            long nextOffset = Optional.ofNullable(this.jedis.get(offsetKey)).map(Long::parseLong).orElse(0L);
            this.nextOffsetAlloc = new AtomicLong(nextOffset);
            this.jedis.hgetAll(recordMapKey.getBytes(StandardCharsets.UTF_8)).forEach((k, v) -> {
                RecordBatchWithContextWrapper batchWithContextWrapper = RecordBatchWithContextWrapper.decode(ByteBuffer.wrap(v));
                recordMap.put(batchWithContextWrapper.baseOffset(), batchWithContextWrapper);
            });
        }

        @Override
        public long streamId() {
            return streamId;
        }

        @Override
        public long startOffset() {
            return 0;
        }

        @Override
        public long nextOffset() {
            return nextOffsetAlloc.get();
        }

        @Override
        public synchronized CompletableFuture<AppendResult> append(RecordBatch recordBatch) {
            long baseOffset = nextOffsetAlloc.getAndAdd(recordBatch.count());
            RecordBatchWithContextWrapper wrapper = new RecordBatchWithContextWrapper(recordBatch, baseOffset);
            recordMap.put(baseOffset, wrapper);
            CompletableFuture<AppendResult> cf = new CompletableFuture<>();
            executors.submit(() -> {
                try {
                    Thread.sleep(2);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                this.jedis.hset(recordMapKey.getBytes(StandardCharsets.UTF_8), String.valueOf(baseOffset).getBytes(StandardCharsets.UTF_8), wrapper.encode());
                this.jedis.set(offsetKey, String.valueOf(nextOffsetAlloc.get()));
                log.info("[stream {}] saved record batch with baseOffset: {}, next Offset is: {}", streamId, baseOffset, nextOffsetAlloc.get());
                cf.complete(() -> baseOffset);
            });
            return cf;
        }

        @Override
        public CompletableFuture<FetchResult> fetch(long startOffset, long endOffset, int maxSizeHint) {
            Long floorKey = recordMap.floorKey(startOffset);
            if (floorKey == null) {
                return CompletableFuture.completedFuture(ArrayList::new);
            }
            List<RecordBatchWithContext> records = new ArrayList<>(recordMap.subMap(floorKey, endOffset).values());
            log.info("[stream {}] fetching from startOffset {} with maxSizeHint {}, baseOffset {}, recordsCount {}", streamId, startOffset, maxSizeHint, startOffset, records.size());
            return CompletableFuture.completedFuture(() -> records);
        }

        @Override
        public CompletableFuture<Void> trim(long newStartOffset) {
            recordMap.headMap(newStartOffset).forEach((baseOffset, record) -> {
                jedis.hdel(recordMapKey.getBytes(StandardCharsets.UTF_8), String.valueOf(baseOffset).getBytes(StandardCharsets.UTF_8));
                log.info("[stream {}] trim record batch with baseOffset: {}", streamId, baseOffset);
            });
            recordMap = new ConcurrentSkipListMap<>(recordMap.tailMap(newStartOffset));
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> close() {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> destroy() {
            return CompletableFuture.completedFuture(null);
        }
    }

    class StreamClientImpl implements StreamClient {
        private static final String STREAM_PREFIX_IN_REDIS = "NID_S";
        private final AtomicLong streamIdAlloc;
        private final JedisPooled jedis;

        public StreamClientImpl(JedisPooled jedis) {
            this.jedis = jedis;
            long nextStreamId = Optional.ofNullable(this.jedis.get(STREAM_PREFIX_IN_REDIS)).map(Long::parseLong).orElse(0L);
            this.streamIdAlloc = new AtomicLong(nextStreamId);
        }

        @Override
        public CompletableFuture<Stream> createAndOpenStream(CreateStreamOptions createStreamOptions) {
            return CompletableFuture.completedFuture(streamIdAlloc.getAndIncrement())
                    .thenApply(streamId -> {
                        log.info("creating stream {}", streamId);
                        jedis.set(STREAM_PREFIX_IN_REDIS, String.valueOf(streamIdAlloc.get()));
                        return new StreamImpl(jedis, streamId);
                    });
        }

        @Override
        public CompletableFuture<Stream> openStream(long streamId, OpenStreamOptions openStreamOptions) {
            return CompletableFuture.completedFuture(new StreamImpl(jedis, streamId));
        }

        @Override
        public void shutdown() {

        }
    }

    static class KVClientImpl implements KVClient {
        private static final String KV_SERVICE_KEY_IN_REDIS = "K_";

        private final JedisPooled jedis;

        public KVClientImpl(JedisPooled jedis) {
            this.jedis = jedis;
        }

        @Override
        public CompletableFuture<Void> putKV(List<KeyValue> list) {
            list.forEach(kv -> {
                String redisKey = KV_SERVICE_KEY_IN_REDIS + kv.key();
                byte[] valueBytes = new byte[kv.value().remaining()];
                kv.value().duplicate().get(valueBytes);
                jedis.set(redisKey.getBytes(StandardCharsets.UTF_8), valueBytes);
                log.info("put kv: {} -> size: {}, content: {}", kv.key(), valueBytes.length, Arrays.toString(valueBytes));
            });
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<List<KeyValue>> getKV(List<String> list) {
            List<KeyValue> rst = new LinkedList<>();
            list.forEach(key -> {
                String redisKey = KV_SERVICE_KEY_IN_REDIS + key;
                byte[] valueBytes = jedis.get(redisKey.getBytes(StandardCharsets.UTF_8));
                if (valueBytes == null) {
                    rst.add(KeyValue.of(key, null));
                    log.info("get kv: {} -> null", key);
                    return;
                }
                rst.add(KeyValue.of(key, ByteBuffer.wrap(valueBytes)));
                log.info("get kv: {} -> size: {}, content: {}", key, valueBytes.length, Arrays.toString(valueBytes));
            });
            return CompletableFuture.completedFuture(rst);
        }

        @Override
        public CompletableFuture<Void> delKV(List<String> list) {
            list.forEach(key -> {
                String redisKey = KV_SERVICE_KEY_IN_REDIS + key;
                jedis.del(redisKey.getBytes(StandardCharsets.UTF_8));
            });
            return CompletableFuture.completedFuture(null);
        }
    }
}
