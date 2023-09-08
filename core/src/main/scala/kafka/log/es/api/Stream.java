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

package kafka.log.es.api;

import java.util.concurrent.CompletableFuture;

/**
 * Record stream.
 */
public interface Stream {

    /**
     * Get stream id
     */
    long streamId();

    /**
     * Get stream start offset.
     */
    long startOffset();

    /**
     * Get stream next append record offset.
     */
    long nextOffset();


    /**
     * Append recordBatch to stream.
     *
     * @param recordBatch {@link RecordBatch}.
     * @return - complete success with async {@link AppendResult}, when append success.
     * - complete exception with {@link ElasticStreamClientException}, when append fail. TODO: specify the exception.
     */
    CompletableFuture<AppendResult> append(RecordBatch recordBatch);

    /**
     * Fetch recordBatch list from stream. Note the startOffset may be in the middle in the first recordBatch.
     *
     * @param startOffset  start offset, if the startOffset in middle of a recordBatch, the recordBatch will be returned.
     * @param endOffset exclusive end offset, if the endOffset in middle of a recordBatch, the recordBatch will be returned.
     * @param maxBytesHint max fetch data size hint, the real return data size may be larger than maxBytesHint.
     * @return - complete success with {@link FetchResult}, when fetch success.
     * - complete exception with {@link ElasticStreamClientException}, when startOffset is bigger than stream end offset.
     */
    CompletableFuture<FetchResult> fetch(long startOffset, long endOffset, int maxBytesHint);

    /**
     * Trim stream.
     *
     * @param newStartOffset new start offset.
     * @return - complete success with async {@link Void}, when trim success.
     * - complete exception with {@link ElasticStreamClientException}, when trim fail.
     */
    CompletableFuture<Void> trim(long newStartOffset);

    /**
     * Close the stream.
     */
    CompletableFuture<Void> close();

    /**
     * Destroy stream.
     */
    CompletableFuture<Void> destroy();
}
