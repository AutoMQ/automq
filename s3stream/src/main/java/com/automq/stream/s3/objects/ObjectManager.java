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
     * Verify object exist.
     * @param objectId object id.
     * @return true if object exist, otherwise false.
     */
    boolean isObjectExist(long objectId);

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

    /**
     * Get the cluster objects count.
     */
    CompletableFuture<Integer> getObjectsCount();

    void setCommitStreamSetObjectHook(CommitStreamSetObjectHook hook);
}
