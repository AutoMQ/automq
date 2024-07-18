/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.log.stream.s3.metadata;

import org.apache.kafka.metadata.stream.InRangeObjects;

import java.util.concurrent.CompletableFuture;

public interface InRangeObjectsFetcher {

    /**
     * fetch stream interval related objects
     *
     * @param streamId    stream id
     * @param startOffset start offset, inclusive, if not exist, return INVALID
     * @param endOffset   end offset, exclusive, if not exist, wait for it
     * @param limit       max object count
     * @return {@link InRangeObjects}
     */
    CompletableFuture<InRangeObjects> fetch(long streamId, long startOffset, long endOffset, int limit);

}
