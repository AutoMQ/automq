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

import com.automq.stream.DefaultRecordBatch;
import com.automq.stream.RecordBatchWithContextWrapper;
import com.automq.stream.api.AppendResult;
import com.automq.stream.api.Client;
import com.automq.stream.api.CreateStreamOptions;
import com.automq.stream.api.FetchResult;
import com.automq.stream.api.KVClient;
import com.automq.stream.api.KeyValue;
import com.automq.stream.api.KeyValue.Key;
import com.automq.stream.api.KeyValue.Value;
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
    }
}
