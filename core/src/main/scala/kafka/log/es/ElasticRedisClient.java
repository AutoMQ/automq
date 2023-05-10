package kafka.log.es;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPooled;
import sdk.elastic.stream.api.AppendResult;
import sdk.elastic.stream.api.Client;
import sdk.elastic.stream.api.CreateStreamOptions;
import sdk.elastic.stream.api.FetchResult;
import sdk.elastic.stream.api.KVClient;
import sdk.elastic.stream.api.KeyValue;
import sdk.elastic.stream.api.OpenStreamOptions;
import sdk.elastic.stream.api.RecordBatch;
import sdk.elastic.stream.api.RecordBatchWithContext;
import sdk.elastic.stream.api.Stream;
import sdk.elastic.stream.api.StreamClient;

public class ElasticRedisClient implements Client {
    private static final Logger log = LoggerFactory.getLogger(ElasticRedisClient.class);
    private final JedisPooled jedis = new JedisPooled("localhost", 6379);
    private final StreamClient streamClient = new StreamClientImpl(jedis);
    private final KVClient kvClient = new KVClientImpl(jedis);

    @Override
    public StreamClient streamClient() {
        return streamClient;
    }

    @Override
    public KVClient kvClient() {
        return kvClient;
    }


    static class StreamImpl implements Stream {
        private static final String STREAM_CONTENT_PREFIX_IN_REDIS = "S_";
        private static final String STREAM_CONTENT_RECORD_MAP_PREFIX_IN_REDIS = STREAM_CONTENT_PREFIX_IN_REDIS + "RM_";
        private static final String STREAM_CONTENT_NEXT_OFFSET_PREFIX_IN_REDIS = STREAM_CONTENT_PREFIX_IN_REDIS + "NO_";
        private final AtomicLong nextOffsetAlloc;
        private final NavigableMap<Long, RecordBatchWithContext> recordMap = new ConcurrentSkipListMap<>();
        private final long streamId;
        private final String recordMapKey;
        private final String offsetKey;
        private final JedisPooled jedis;

        public StreamImpl(JedisPooled jedis, long streamId) {
            this.jedis = jedis;
            this.streamId = streamId;
            this.offsetKey = STREAM_CONTENT_NEXT_OFFSET_PREFIX_IN_REDIS + streamId;
            this.recordMapKey = STREAM_CONTENT_RECORD_MAP_PREFIX_IN_REDIS + streamId;

            long nextOffset = Optional.ofNullable(this.jedis.get(offsetKey)).map(Long::parseLong).orElse(0L);
            this.nextOffsetAlloc = new AtomicLong(nextOffset);
            this.jedis.hgetAll(recordMapKey.getBytes(StandardCharsets.UTF_8)).forEach((k, v) -> {
                MemoryClient.RecordBatchWithContextWrapper batchWithContextWrapper = MemoryClient.RecordBatchWithContextWrapper.decode(ByteBuffer.wrap(v));
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
            MemoryClient.RecordBatchWithContextWrapper wrapper = new MemoryClient.RecordBatchWithContextWrapper(recordBatch, baseOffset);
            recordMap.put(baseOffset, wrapper);
            this.jedis.hset(recordMapKey.getBytes(StandardCharsets.UTF_8), String.valueOf(baseOffset).getBytes(StandardCharsets.UTF_8), wrapper.encode());
            this.jedis.set(offsetKey, String.valueOf(nextOffsetAlloc.get()));
            log.info("[stream {}] saved record batch with baseOffset: {}, next Offset is: {}", streamId, baseOffset, nextOffsetAlloc.get());
            return CompletableFuture.completedFuture(() -> baseOffset);
        }

        @Override
        public CompletableFuture<FetchResult> fetch(long startOffset, int maxSizeHint) {
            Optional<RecordBatchWithContext> opt = Optional.ofNullable(recordMap.floorEntry(startOffset)).map(Map.Entry::getValue).filter(rb -> rb.lastOffset() > startOffset);
            if (opt.isPresent()) {
                log.info("[stream {}] fetching from startOffset {} with maxSizeHint {}, baseOffset is {}",streamId, startOffset, maxSizeHint, opt.get().baseOffset());
                return CompletableFuture.completedFuture(() -> Collections.singletonList(opt.get()));
            } else {
                log.info("[stream {}] fetching from startOffset {} with maxSizeHint {}, no record batch found", streamId, startOffset, maxSizeHint);
                return CompletableFuture.completedFuture(Collections::emptyList);
            }
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

    static class StreamClientImpl implements StreamClient {
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
