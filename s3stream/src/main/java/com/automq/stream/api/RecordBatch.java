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

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Record batch.
 */
public interface RecordBatch {

    /**
     * Get payload record count.
     *
     * @return record count.
     */
    int count();

    /**
     * Get min timestamp of records.
     *
     * @return min timestamp of records.
     */
    long baseTimestamp();

    /**
     * Get record batch extension properties.
     *
     * @return batch extension properties.
     */
    Map<String, String> properties();

    /**
     * Get raw payload.
     *
     * @return raw payload.
     */
    ByteBuffer rawPayload();
}
