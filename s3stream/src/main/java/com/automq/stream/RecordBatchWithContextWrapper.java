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
import com.automq.stream.api.RecordBatchWithContext;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;

public class RecordBatchWithContextWrapper implements RecordBatchWithContext {
    private final RecordBatch recordBatch;
    private final long baseOffset;

    public RecordBatchWithContextWrapper(RecordBatch recordBatch, long baseOffset) {
        this.recordBatch = recordBatch;
        this.baseOffset = baseOffset;
    }

    public static RecordBatchWithContextWrapper decode(ByteBuffer buffer) {
        long baseOffset = buffer.getLong();
        int count = buffer.getInt();
        return new RecordBatchWithContextWrapper(new DefaultRecordBatch(count, 0, Collections.emptyMap(), buffer), baseOffset);
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
        return recordBatch.rawPayload().duplicate();
    }

    public byte[] encode() {
        ByteBuffer buffer = ByteBuffer.allocate(8 + 4 + recordBatch.rawPayload().remaining())
            .putLong(baseOffset)
            .putInt(recordBatch.count())
            .put(recordBatch.rawPayload().duplicate())
            .flip();
        return buffer.array();
    }
}
