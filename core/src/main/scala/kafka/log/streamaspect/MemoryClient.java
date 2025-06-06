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
import com.automq.stream.RecordBatchWithContextWrapper;
import com.automq.stream.api.AppendResult;
import com.automq.stream.api.Client;
import com.automq.stream.api.CreateStreamOptions;
import com.automq.stream.api.FetchResult;
import com.automq.stream.api.KVClient;
import com.automq.stream.api.KeyValue;
import com.automq.stream.api.KeyValue.Key;
import com.automq.stream.api.KeyValue.KeyAndNamespace;
import com.automq.stream.api.KeyValue.Value;
import com.automq.stream.api.KeyValue.ValueAndEpoch;
import com.automq.stream.api.OpenStreamOptions;
import com.automq.stream.api.RecordBatch;
import com.automq.stream.api.RecordBatchWithContext;
import com.automq.stream.api.Stream;
import com.automq.stream.api.StreamClient;
import com.automq.stream.s3.context.AppendContext;
import com.automq.stream.s3.context.FetchContext;
import com.automq.stream.s3.failover.FailoverRequest;
import com.automq.stream.s3.failover.FailoverResponse;
import com.automq.stream.utils.FutureUtil;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.automq.stream.utils.KVRecordUtils.buildCompositeKey;
import static org.apache.kafka.common.protocol.Errors.INVALID_KV_RECORD_EPOCH;

public class MemoryClient implements Client {
    private final StreamClient streamClient = new StreamClientImpl();
    private final KVClient kvClient = new KVClientImpl();

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

    @Override
    public CompletableFuture<FailoverResponse> failover(FailoverRequest request) {
        return FutureUtil.failedFuture(new UnsupportedOperationException());
    }

    static class StreamImpl implements Stream {
        private final AtomicLong nextOffsetAlloc = new AtomicLong();
        private NavigableMap<Long, RecordBatchWithContext> recordMap = new ConcurrentSkipListMap<>();
        private final long streamId;

        public StreamImpl(long streamId) {
            this.streamId = streamId;
        }

        @Override
        public long streamId() {
            return streamId;
        }

        @Override
        public long streamEpoch() {
            return 0L;
        }

        @Override
        public long startOffset() {
            return 0;
        }

        @Override
        public long confirmOffset() {
            return nextOffsetAlloc.get();
        }

        @Override
        public long nextOffset() {
            return nextOffsetAlloc.get();
        }

        @Override
        public synchronized CompletableFuture<AppendResult> append(AppendContext context, RecordBatch recordBatch) {
            long baseOffset = nextOffsetAlloc.getAndAdd(recordBatch.count());
            ByteBuffer copy = ByteBuffer.allocate(recordBatch.rawPayload().remaining());
            copy.put(recordBatch.rawPayload().duplicate());
            copy.flip();
            recordBatch = new DefaultRecordBatch(recordBatch.count(), recordBatch.baseTimestamp(), recordBatch.properties(), copy);
            recordMap.put(baseOffset, new RecordBatchWithContextWrapper(recordBatch, baseOffset));
            return CompletableFuture.completedFuture(() -> baseOffset);
        }

        @Override
        public CompletableFuture<FetchResult> fetch(FetchContext context, long startOffset, long endOffset,
            int maxSizeHint) {
            Long floorKey = recordMap.floorKey(startOffset);
            if (floorKey == null) {
                return CompletableFuture.completedFuture(ArrayList::new);
            }
            List<RecordBatchWithContext> records = new ArrayList<>(recordMap.subMap(floorKey, endOffset).values());
            return CompletableFuture.completedFuture(() -> records);
        }

        @Override
        public CompletableFuture<Void> trim(long newStartOffset) {
            recordMap = new ConcurrentSkipListMap<>(recordMap.tailMap(newStartOffset));
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> close() {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> destroy() {
            recordMap.clear();
            return CompletableFuture.completedFuture(null);
        }
    }

    static class StreamClientImpl implements StreamClient {
        private final AtomicLong streamIdAlloc = new AtomicLong();

        @Override
        public CompletableFuture<Stream> createAndOpenStream(CreateStreamOptions createStreamOptions) {
            return CompletableFuture.completedFuture(new StreamImpl(streamIdAlloc.incrementAndGet()));
        }

        @Override
        public CompletableFuture<Stream> openStream(long streamId, OpenStreamOptions openStreamOptions) {
            return CompletableFuture.completedFuture(new StreamImpl(streamId));
        }

        @Override
        public Optional<Stream> getStream(long streamId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void shutdown() {

        }
    }

    public static class KVClientImpl implements KVClient {
        private final Map<String, ByteBuffer> store = new ConcurrentHashMap<>();
        private final Map<String, KeyMetadata> keyMetadataMap = new ConcurrentHashMap<>();

        @Override
        public CompletableFuture<Value> putKV(KeyValue keyValue) {
            store.put(keyValue.key().get(), keyValue.value().get().duplicate());
            return CompletableFuture.completedFuture(keyValue.value());
        }

        @Override
        public CompletableFuture<Value> putKVIfAbsent(KeyValue keyValue) {
            ByteBuffer value = store.putIfAbsent(keyValue.key().get(), keyValue.value().get().duplicate());
            return CompletableFuture.completedFuture(Value.of(value));
        }

        @Override
        public CompletableFuture<Value> getKV(Key key) {
            return CompletableFuture.completedFuture(Value.of(store.get(key.get())));
        }

        @Override
        public CompletableFuture<Value> delKV(Key key) {
            return CompletableFuture.completedFuture(Value.of(store.remove(key.get())));
        }

        @Override
        public CompletableFuture<ValueAndEpoch> putNamespacedKVIfAbsent(KeyValue keyValue) {
            String key = buildCompositeKey(keyValue.namespace(), keyValue.key().get());
            KeyMetadata keyMetadata = keyMetadataMap.get(key);
            long currentEpoch = keyMetadata != null ? keyMetadata.getEpoch() : 0;
            if (keyValue.epoch() > 0 && keyValue.epoch() != currentEpoch) {
                return CompletableFuture.failedFuture(INVALID_KV_RECORD_EPOCH.exception());
            }
            long newEpoch = System.currentTimeMillis();

            ByteBuffer value = store.putIfAbsent(key, keyValue.value().get().duplicate());
            if (keyValue.namespace() != null && !keyValue.namespace().isEmpty()) {
                keyMetadataMap.putIfAbsent(keyValue.key().get(), new KeyMetadata(keyValue.namespace(), newEpoch));
            }
            return CompletableFuture.completedFuture(ValueAndEpoch.of(value, newEpoch));
        }

        @Override
        public CompletableFuture<ValueAndEpoch> putNamespacedKV(KeyValue keyValue) {
            String key = buildCompositeKey(keyValue.namespace(), keyValue.key().get());
            KeyMetadata keyMetadata = keyMetadataMap.get(key);
            long currentEpoch = keyMetadata != null ? keyMetadata.getEpoch() : 0;
            if (keyValue.epoch() > 0 && keyValue.epoch() != currentEpoch) {
                return CompletableFuture.failedFuture(INVALID_KV_RECORD_EPOCH.exception());
            }
            long newEpoch = System.currentTimeMillis();

            ByteBuffer value = store.put(key, keyValue.value().get().duplicate());
            if (keyValue.namespace() != null && !keyValue.namespace().isEmpty()) {
                keyMetadataMap.put(keyValue.key().get(), new KeyMetadata(keyValue.namespace(), newEpoch));
            }
            return CompletableFuture.completedFuture(ValueAndEpoch.of(value, newEpoch));
        }

        @Override
        public CompletableFuture<ValueAndEpoch> getNamespacedKV(KeyAndNamespace keyAndNamespace) {
            String key = buildCompositeKey(keyAndNamespace.namespace(), keyAndNamespace.key().get());
            KeyMetadata keyMetadata = null;
            if (keyAndNamespace.namespace() != null && !keyAndNamespace.namespace().isEmpty()) {
                keyMetadata = keyMetadataMap.get(key);
            }
            return CompletableFuture.completedFuture(ValueAndEpoch.of(store.get(key), keyMetadata != null ? keyMetadata.getEpoch() : 0L));
        }

        @Override
        public CompletableFuture<ValueAndEpoch> delNamespacedKV(KeyValue keyValue) {
            String key = buildCompositeKey(keyValue.namespace(), keyValue.key().get());
            KeyMetadata keyMetadata = keyMetadataMap.get(key);
            long currentEpoch = keyMetadata != null ? keyMetadata.getEpoch() : 0;
            if (keyValue.epoch() > 0 && keyValue.epoch() != currentEpoch) {
                return CompletableFuture.failedFuture(INVALID_KV_RECORD_EPOCH.exception());
            }
            return CompletableFuture.completedFuture(ValueAndEpoch.of(store.remove(key), currentEpoch));
        }

        private static class KeyMetadata {
            private final long epoch;
            private final String namespace;
            public KeyMetadata(String namespace, long epoch) {
                this.namespace = namespace;
                this.epoch = epoch;
            }

            public long getEpoch() {
                return epoch;
            }

            public String getNamespace() {
                return namespace;
            }
        }
    }
}
