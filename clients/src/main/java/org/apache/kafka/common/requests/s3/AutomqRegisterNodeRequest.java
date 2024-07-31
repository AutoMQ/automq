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

package org.apache.kafka.common.requests.s3;

import java.nio.ByteBuffer;
import org.apache.kafka.common.message.AutomqRegisterNodeRequestData;
import org.apache.kafka.common.message.AutomqRegisterNodeResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.ApiError;

public class AutomqRegisterNodeRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<AutomqRegisterNodeRequest> {

        private final AutomqRegisterNodeRequestData data;

        public Builder(AutomqRegisterNodeRequestData data) {
            super(ApiKeys.AUTOMQ_REGISTER_NODE);
            this.data = data;
        }

        @Override
        public AutomqRegisterNodeRequest build(short version) {
            return new AutomqRegisterNodeRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final AutomqRegisterNodeRequestData data;

    public AutomqRegisterNodeRequest(AutomqRegisterNodeRequestData data, short version) {
        super(ApiKeys.AUTOMQ_REGISTER_NODE, version);
        this.data = data;
    }

    @Override
    public AutomqRegisterNodeResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        ApiError apiError = ApiError.fromThrowable(e);
        AutomqRegisterNodeResponseData response = new AutomqRegisterNodeResponseData()
            .setErrorCode(apiError.error().code())
            .setThrottleTimeMs(throttleTimeMs);
        return new AutomqRegisterNodeResponse(response);
    }

    @Override
    public AutomqRegisterNodeRequestData data() {
        return data;
    }

    public static AutomqRegisterNodeRequest parse(ByteBuffer buffer, short version) {
        return new AutomqRegisterNodeRequest(new AutomqRegisterNodeRequestData(
            new ByteBufferAccessor(buffer), version), version);
    }
}
