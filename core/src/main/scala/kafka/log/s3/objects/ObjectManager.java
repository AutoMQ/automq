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

package kafka.log.s3.objects;

import org.apache.kafka.metadata.stream.S3ObjectMetadata;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.metadata.stream.S3StreamObjectMetadata;

/**
 * Object metadata registry.
 */
public interface ObjectManager {

    /**
     * Prepare object id for write, if the objects is not committed in ttl, then delete it.
     *
     * @param count object id count.
     * @param ttl   ttl in milliseconds.
     * @return object id range start.
     */
    CompletableFuture<Long> prepareObject(int count, long ttl);

    /**
     * Commit WAL object.
     *
     * @param request {@link CommitWALObjectRequest}
     * @return {@link CommitWALObjectResponse}
     */
    CompletableFuture<CommitWALObjectResponse> commitWALObject(CommitWALObjectRequest request);

    /**
     * Commit stream object. When the source object has no reference, then delete it.
     *
     * @param request {@link CommitStreamObjectRequest}
     */
    CompletableFuture<Void> commitStreamObject(CommitStreamObjectRequest request);

    /**
     * Get objects by stream range.
     * When obj1 contains stream0 <code>[0, 100) [200, 300)</code> and obj2 contains stream1 <code>[100, 200)</code>,
     * expect getObjects(streamId, 0, 300) return <code>[obj1, obj2, obj1]</code>
     *
     * @param streamId    stream id.
     * @param startOffset get range start offset.
     * @param endOffset   get range end offset.
     * @param limit       max object count. Why use limit instead of maxBytes? Because we cannot get stream size from object metadata.
     * @return {@link S3ObjectMetadata}
     */
    List<S3ObjectMetadata> getObjects(long streamId, long startOffset, long endOffset, int limit);

    /**
     * Get stream objects by stream range and limit.
     * @param streamId stream id.
     * @param startOffset searching start offset in the stream.
     * @param endOffset searching end offset in the stream.
     * @param limit max object count.
     * @return {@link S3StreamObjectMetadata}
     */
    List<S3StreamObjectMetadata> getStreamObjects(long streamId, long startOffset, long endOffset, int limit);

    /**
     * Get current server wal objects.
     * When server is starting, wal need server wal objects to recover.
     */
    List<S3ObjectMetadata> getServerObjects();
}

