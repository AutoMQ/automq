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

package com.automq.stream.s3.objects;

import com.automq.stream.s3.metadata.S3ObjectMetadata;
import java.util.List;
import java.util.concurrent.CompletableFuture;

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
     * Commit stream set object.
     *
     * @param request {@link CommitStreamSetObjectRequest}
     * @return {@link CommitStreamSetObjectResponse}
     */
    CompletableFuture<CommitStreamSetObjectResponse> commitStreamSetObject(CommitStreamSetObjectRequest request);

    /**
     * Compact stream object. When the source object has no reference, then delete it.
     *
     * @param request {@link CompactStreamObjectRequest}
     */
    CompletableFuture<Void> compactStreamObject(CompactStreamObjectRequest request);

    /**
     * Get objects by stream range.
     * When obj1 contains stream0 <code>[0, 100) [200, 300)</code> and obj2 contains stream1 <code>[100, 200)</code>,
     * expect getObjects(streamId, 0, 300) return <code>[obj1, obj2, obj1]</code>
     * <ul>
     *     <li> Concern two types of objects: stream object and stream set object.
     *     <li> Returned objects must be continuous of stream range.
     *     <li> Returned objects aren't physical object concept, they are logical object concept.
     *     (regard each returned object-metadata as a slice of object)
     * </ul>
     *
     * @param streamId    stream id.
     * @param startOffset get range start offset.
     * @param endOffset   get range end offset. NOOP_OFFSET represent endOffset is unlimited.
     * @param limit       max object range count.
     * @return {@link S3ObjectMetadata}
     */
    CompletableFuture<List<S3ObjectMetadata>> getObjects(long streamId, long startOffset, long endOffset, int limit);

    /**
     * Get current server stream set objects.
     * When server is starting, server need server stream set objects to recover.
     */
    CompletableFuture<List<S3ObjectMetadata>> getServerObjects();

    /**
     * Get stream objects by stream range.
     * <ul>
     *      <li> Only concern about stream objects, ignore stream set objects.
     *      <li> Returned stream objects can be discontinuous of stream range.
     *      <li> Ranges of the returned stream objects are <strong>in ascending order</strong>.
     * </ul>
     *
     * @param streamId    stream id.
     * @param startOffset get range start offset.
     * @param endOffset   get range end offset.
     * @param limit       max object count.
     * @return {@link S3ObjectMetadata}
     */
    CompletableFuture<List<S3ObjectMetadata>> getStreamObjects(long streamId, long startOffset, long endOffset,
        int limit);
}

