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

package kafka.log.es;

import com.automq.elasticstream.client.api.AppendResult;
import com.automq.elasticstream.client.api.FetchResult;
import com.automq.elasticstream.client.api.RecordBatch;
import com.automq.elasticstream.client.api.Stream;

import java.util.concurrent.CompletableFuture;
import org.apache.kafka.common.errors.es.SlowFetchHintException;

/**
 * Elastic stream slice is a slice from elastic stream, <strong> the startOffset of a slice is 0. </strong>
 * In the same time, there is only one writable slice in a stream, and the writable slice is always the last slice.
 */
public interface ElasticStreamSlice {

    /**
     * Append record batch to stream slice.
     *
     * @param recordBatch {@link RecordBatch}
     * @return {@link AppendResult}
     */
    CompletableFuture<AppendResult> append(RecordBatch recordBatch);

    /**
     * Fetch record batch from stream slice.
     *
     * @param startOffset  start offset.
     * @param endOffset    end offset.
     * @param maxBytesHint max fetch data size hint, the real return data size may be larger than maxBytesHint.
     * @return {@link FetchResult}
     */
    FetchResult fetch(long startOffset, long endOffset, int maxBytesHint) throws SlowFetchHintException;

    default FetchResult fetch(long startOffset, long endOffset) throws SlowFetchHintException {
        return fetch(startOffset, endOffset, (int) (endOffset - startOffset));
    }

    /**
     * Get stream slice next offset.
     */
    long nextOffset();

    /**
     * Get slice start offset in under stream.
     *
     * @return segment start offset in stream.
     */
    long startOffsetInStream();

    /**
     * Get slice range which is the relative offset range in stream.
     * @return {@link SliceRange}
     */
    SliceRange sliceRange();

    /**
     * Seal slice, forbid future append.
     */
    void seal();

    Stream stream();
}
