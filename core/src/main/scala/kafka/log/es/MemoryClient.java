package kafka.log.es;

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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedList;
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
    public StreamClient streamClient() {
        return streamClient;
    }

    @Override
    public KVClient kvClient() {
        return kvClient;
    }

    static class StreamImpl implements Stream {
        private final AtomicLong nextOffsetAlloc = new AtomicLong();
        private final NavigableMap<Long, RecordBatchWithContext> recordMap = new ConcurrentSkipListMap<>();
        private final long streamId;

        public StreamImpl(long streamId) {
            this.streamId = streamId;
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
            recordMap.put(baseOffset, new RecordBatchWithContextWrapper(recordBatch, baseOffset));
            return CompletableFuture.completedFuture(() -> baseOffset);
        }

        @Override
        public CompletableFuture<FetchResult> fetch(long startOffset, int maxSizeHint) {
            Optional<RecordBatchWithContext> opt = Optional.ofNullable(recordMap.floorEntry(startOffset)).map(Map.Entry::getValue).filter(rb -> rb.lastOffset() > startOffset);
            if (opt.isPresent()) {
                return CompletableFuture.completedFuture(() -> Collections.singletonList(opt.get()));
            } else {
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
        private AtomicLong streamIdAlloc = new AtomicLong();

        @Override
        public CompletableFuture<Stream> createAndOpenStream(CreateStreamOptions createStreamOptions) {
            return CompletableFuture.completedFuture(new StreamImpl(streamIdAlloc.incrementAndGet()));
        }

        @Override
        public CompletableFuture<Stream> openStream(long streamId, OpenStreamOptions openStreamOptions) {
            return CompletableFuture.completedFuture(new StreamImpl(streamId));
        }
    }

    static class KVClientImpl implements KVClient {
        private final Map<String, ByteBuffer> store = new ConcurrentHashMap<>();

        @Override
        public CompletableFuture<Void> putKV(List<KeyValue> list) {
            list.forEach(kv -> store.put(kv.key(), kv.value().slice()));
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<List<KeyValue>> getKV(List<String> list) {
            List<KeyValue> rst = new LinkedList<>();
            list.forEach(key -> rst.add(KeyValue.of(key, Optional.ofNullable(store.get(key)).map(b -> b.slice()).orElse(null))));
            return CompletableFuture.completedFuture(rst);
        }

        @Override
        public CompletableFuture<Void> delKV(List<String> list) {
            list.forEach(store::remove);
            return CompletableFuture.completedFuture(null);
        }
    }

    static class RecordBatchWithContextWrapper implements RecordBatchWithContext {
        private final RecordBatch recordBatch;
        private final long baseOffset;

        public RecordBatchWithContextWrapper(RecordBatch recordBatch, long baseOffset) {
            this.recordBatch = recordBatch;
            this.baseOffset = baseOffset;
        }

        @Override
        public long baseOffset() {
            return baseOffset;
        }

        @Override
        public long lastOffset() {
            return baseOffset + recordBatch.count();
        }

        @Override
        public int count() {
            return recordBatch.count();
        }

        @Override
        public long baseTimestamp() {
            return recordBatch.baseTimestamp();
        }

        @Override
        public List<KeyValue> properties() {
            return recordBatch.properties();
        }

        @Override
        public ByteBuffer rawPayload() {
            return recordBatch.rawPayload().slice();
        }
    }
}
