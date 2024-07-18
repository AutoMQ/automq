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

package com.automq.stream.api;

import com.automq.stream.s3.cache.CacheAccessType;
import java.util.List;

public interface FetchResult {

    /**
     * Get fetched RecordBatch list.
     *
     * @return {@link RecordBatchWithContext} list.
     */
    List<RecordBatchWithContext> recordBatchList();

    default CacheAccessType getCacheAccessType() {
        return CacheAccessType.DELTA_WAL_CACHE_HIT;
    }

    /**
     * Free fetch result backend memory.
     */
    default void free() {
    }
}
