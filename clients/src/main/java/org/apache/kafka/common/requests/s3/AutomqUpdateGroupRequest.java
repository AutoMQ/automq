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

import org.apache.kafka.common.message.AutomqUpdateGroupRequestData;
import org.apache.kafka.common.message.AutomqUpdateGroupResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiError;

import java.nio.ByteBuffer;

public class AutomqUpdateGroupRequest extends AbstractRequest {
    private final AutomqUpdateGroupRequestData data;

    public AutomqUpdateGroupRequest(AutomqUpdateGroupRequestData data, short version) {
        super(ApiKeys.AUTOMQ_UPDATE_GROUP, version);
        this.data = data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        ApiError apiError = ApiError.fromThrowable(e);
        AutomqUpdateGroupResponseData response = new AutomqUpdateGroupResponseData()
            .setErrorCode(apiError.error().code())
            .setThrottleTimeMs(throttleTimeMs);
        return new AutomqUpdateGroupResponse(response);
    }

    @Override
    public AutomqUpdateGroupRequestData data() {
        return data;
    }

    public static AutomqUpdateGroupRequest parse(ByteBuffer buffer, short version) {
        return new AutomqUpdateGroupRequest(new AutomqUpdateGroupRequestData(new ByteBufferAccessor(buffer), version), version);
    }

    public static class Builder extends AbstractRequest.Builder<AutomqUpdateGroupRequest> {

        private final AutomqUpdateGroupRequestData data;

        public Builder(AutomqUpdateGroupRequestData data) {
            super(ApiKeys.AUTOMQ_UPDATE_GROUP);
            this.data = data;
        }

        @Override
        public AutomqUpdateGroupRequest build(short version) {
            return new AutomqUpdateGroupRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }
}
