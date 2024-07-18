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

package com.automq.stream;

import com.automq.stream.api.RecordBatch;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;

public class DefaultRecordBatch implements RecordBatch {
    private final int count;
    private final long baseTimestamp;
    private final Map<String, String> properties;
    private final ByteBuffer rawPayload;

    public DefaultRecordBatch(int count, long baseTimestamp, Map<String, String> properties, ByteBuffer rawPayload) {
        this.count = count;
        this.baseTimestamp = baseTimestamp;
        this.properties = properties;
        this.rawPayload = rawPayload;
    }

    @Override
    public int count() {
        return count;
    }

    @Override
    public long baseTimestamp() {
        return baseTimestamp;
    }

    @Override
    public Map<String, String> properties() {
        if (properties == null) {
            return Collections.emptyMap();
        }
        return properties;
    }

    @Override
    public ByteBuffer rawPayload() {
        return rawPayload.duplicate();
    }
}
