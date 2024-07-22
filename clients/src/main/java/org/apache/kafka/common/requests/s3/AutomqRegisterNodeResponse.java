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

package org.apache.kafka.common.requests.s3;

import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.kafka.common.message.AutomqRegisterNodeResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;

public class AutomqRegisterNodeResponse extends AbstractResponse {

    private final AutomqRegisterNodeResponseData data;

    public AutomqRegisterNodeResponse(AutomqRegisterNodeResponseData data) {
        super(ApiKeys.COMMIT_STREAM_OBJECT);
        this.data = data;
    }

    @Override
    public AutomqRegisterNodeResponseData data() {
        return data;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(Errors.forCode(data.errorCode()));
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    @Override
    public void maybeSetThrottleTimeMs(int throttleTimeMs) {
        data.setThrottleTimeMs(throttleTimeMs);
    }

    public static AutomqRegisterNodeResponse parse(ByteBuffer buffer, short version) {
        return new AutomqRegisterNodeResponse(new AutomqRegisterNodeResponseData(
            new ByteBufferAccessor(buffer), version));
    }
}
