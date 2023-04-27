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

import sdk.elastic.stream.api.AppendResult;
import sdk.elastic.stream.api.FetchResult;
import sdk.elastic.stream.api.RecordBatch;

import java.util.concurrent.CompletableFuture;

/**
 * Elastic stream segment, represent stream sub part.
 * support read from in-flight write buffer
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
     * Get segment next offset.
     */
    long nextOffset();

    /**
     * Destroy segment.
     */
    void destroy();

}
