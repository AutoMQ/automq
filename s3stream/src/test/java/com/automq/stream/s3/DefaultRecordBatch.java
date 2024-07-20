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

package com.automq.stream.s3;

import com.automq.stream.api.RecordBatch;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Random;

public class DefaultRecordBatch implements RecordBatch {
    int count;
    ByteBuffer payload;

    public static RecordBatch of(int count, int size) {
        DefaultRecordBatch record = new DefaultRecordBatch();
        record.count = count;
        byte[] bytes = new byte[size];
        new Random().nextBytes(bytes);
        record.payload = ByteBuffer.wrap(bytes);
        return record;
    }

    @Override
    public int count() {
        return count;
    }

    @Override
    public long baseTimestamp() {
        return 0;
    }

    @Override
    public Map<String, String> properties() {
        return Collections.emptyMap();
    }

    @Override
    public ByteBuffer rawPayload() {
        return payload.duplicate();
    }
}
