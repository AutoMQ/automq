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

package kafka.log.streamaspect;

import com.automq.stream.api.RecordBatch;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;

public class RawPayloadRecordBatch implements RecordBatch {
    private final ByteBuffer rawPayload;

    private RawPayloadRecordBatch(ByteBuffer rawPayload) {
        this.rawPayload = rawPayload.duplicate();
    }

    public static RecordBatch of(ByteBuffer rawPayload) {
        return new RawPayloadRecordBatch(rawPayload);
    }

    @Override
    public int count() {
        return rawPayload.remaining();
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
        return rawPayload.duplicate();
    }
}
