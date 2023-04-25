package kafka.log.es;

import sdk.elastic.stream.api.AppendResult;
import sdk.elastic.stream.api.FetchResult;
import sdk.elastic.stream.api.RecordBatch;

import java.util.concurrent.CompletableFuture;

/**
 * Elastic stream segment, represent stream sub part.
 */
public interface ElasticStreamSegment {

    /**
     * Append record batch to segment.
     *
     * @param recordBatch {@link RecordBatch}
     * @return {@link AppendResult}
     */
    CompletableFuture<AppendResult> append(RecordBatch recordBatch);

    /**
     * Fetch record batch from segment.
     *
     * @param startOffset  start offset.
     * @param maxBytesHint max fetch data size hint, the real return data size may be larger than maxBytesHint.
     * @return {@link FetchResult}
     */
    CompletableFuture<FetchResult> fetch(long startOffset, int maxBytesHint);

    /**
     * Destroy segment.
     */
    void destroy();

}
