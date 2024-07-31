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

package com.automq.stream.s3;

import com.automq.stream.api.RecordBatch;
import com.automq.stream.api.RecordBatchWithContext;
import java.nio.ByteBuffer;
import java.util.Map;

public class DefaultRecordBatchWithContext implements RecordBatchWithContext {
    private final RecordBatch recordBatch;
    private final long baseOffset;

    public DefaultRecordBatchWithContext(RecordBatch recordBatch, long baseOffset) {
        this.recordBatch = recordBatch;
        this.baseOffset = baseOffset;
    }

    @Override
    public long baseOffset() {
        return baseOffset;
    }

    @Override
    public long lastOffset() {
        return baseOffset + recordBatch.count();
    }

    @Override
    public int count() {
        return recordBatch.count();
    }

    @Override
    public long baseTimestamp() {
        return recordBatch.baseTimestamp();
    }

    @Override
    public Map<String, String> properties() {
        return recordBatch.properties();
    }

    @Override
    public ByteBuffer rawPayload() {
        return recordBatch.rawPayload();
    }
}
