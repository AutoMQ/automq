package kafka.log.es;

import sdk.elastic.stream.api.AppendResult;
import sdk.elastic.stream.api.FetchResult;
import sdk.elastic.stream.api.KeyValue;
import sdk.elastic.stream.api.RecordBatch;
import sdk.elastic.stream.api.RecordBatchWithContext;
import sdk.elastic.stream.api.Stream;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class DefaultElasticStreamSegment implements ElasticStreamSegment {
    /**
     * logical base offset of this segment.
     */
    private final long segmentBaseOffset;
    /**
     * the real start offset of this segment in the stream.
     */
    private final long startOffsetInStream;
    private final Stream stream;

    public DefaultElasticStreamSegment(long segmentBaseOffset, Stream stream, long startOffsetInStream) {
        this.segmentBaseOffset = segmentBaseOffset;
        this.stream = stream;
        this.startOffsetInStream = startOffsetInStream;
        System.out.println("new segment: " + segmentBaseOffset + ", " + startOffsetInStream + " in stream " + stream.streamId());
    }

    @Override
    public CompletableFuture<AppendResult> append(RecordBatch recordBatch) {
        return stream.append(recordBatch).thenApply(AppendResultWrapper::new);
    }

    @Override
    public CompletableFuture<FetchResult> fetch(long startOffset, int maxBytesHint) {
        return stream.fetch(segmentOffset2streamOffset(startOffset), maxBytesHint);
    }

    @Override
    public void destroy() {
        // TODO: update ElasticLogMeta and persist meta
    }

    private long segmentOffset2streamOffset(long offset) {
        return offset - segmentBaseOffset + startOffsetInStream;
    }

    private long streamOffset2segmentOffset(long offset) {
        return offset - startOffsetInStream + segmentBaseOffset;
    }

    class AppendResultWrapper implements AppendResult {
        private final AppendResult inner;

        public AppendResultWrapper(AppendResult inner) {
            this.inner = inner;
        }

        @Override
        public long baseOffset() {
            return streamOffset2segmentOffset(inner.baseOffset());
        }
    }

    class FetchResultWrapper implements FetchResult {
        private final List<RecordBatchWithContext> recordBatchList;

        public FetchResultWrapper(FetchResult fetchResult) {
            this.recordBatchList = fetchResult.recordBatchList().stream().map(RecordBatchWithContextWrapper::new).collect(Collectors.toList());
        }

        @Override
        public List<RecordBatchWithContext> recordBatchList() {
            return recordBatchList;
        }
    }

    class RecordBatchWithContextWrapper implements RecordBatchWithContext {
        private final RecordBatchWithContext inner;

        public RecordBatchWithContextWrapper(RecordBatchWithContext inner) {
            this.inner = inner;
        }

        @Override
        public long baseOffset() {
            return streamOffset2segmentOffset(inner.baseOffset());
        }

        @Override
        public long lastOffset() {
            return streamOffset2segmentOffset(inner.lastOffset());
        }

        @Override
        public int count() {
            return inner.count();
        }

        @Override
        public long baseTimestamp() {
            return inner.baseTimestamp();
        }

        @Override
        public List<KeyValue> properties() {
            return inner.properties();
        }

        @Override
        public ByteBuffer rawPayload() {
            return inner.rawPayload();
        }
    }
}
