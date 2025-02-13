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

import org.apache.kafka.common.message.AutomqUpdateGroupsRequestData;
import org.apache.kafka.common.message.AutomqUpdateGroupsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiError;

import java.nio.ByteBuffer;

public class AutomqUpdateGroupsRequest extends AbstractRequest {
    private final AutomqUpdateGroupsRequestData data;

    public AutomqUpdateGroupsRequest(AutomqUpdateGroupsRequestData data, short version) {
        super(ApiKeys.AUTOMQ_UPDATE_GROUPS, version);
        this.data = data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        ApiError apiError = ApiError.fromThrowable(e);
        AutomqUpdateGroupsResponseData response = new AutomqUpdateGroupsResponseData()
            .setErrorCode(apiError.error().code())
            .setThrottleTimeMs(throttleTimeMs);
        return new AutomqUpdateGroupsResponse(response);
    }

    @Override
    public AutomqUpdateGroupsRequestData data() {
        return data;
    }

    public static AutomqUpdateGroupsRequest parse(ByteBuffer buffer, short version) {
        return new AutomqUpdateGroupsRequest(new AutomqUpdateGroupsRequestData(new ByteBufferAccessor(buffer), version), version);
    }

    public static class Builder extends AbstractRequest.Builder<AutomqUpdateGroupsRequest> {

        private final AutomqUpdateGroupsRequestData data;

        public Builder(AutomqUpdateGroupsRequestData data) {
            super(ApiKeys.AUTOMQ_UPDATE_GROUPS);
            this.data = data;
        }

        @Override
        public AutomqUpdateGroupsRequest build(short version) {
            return new AutomqUpdateGroupsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }
}
