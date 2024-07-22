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
import org.apache.kafka.common.message.AutomqGetNodesRequestData;
import org.apache.kafka.common.message.AutomqGetNodesResponseData;
import org.apache.kafka.common.message.CommitStreamObjectRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiError;

public class AutomqGetNodesRequest extends AbstractRequest {
    private final AutomqGetNodesRequestData data;

    public AutomqGetNodesRequest(AutomqGetNodesRequestData data, short version) {
        super(ApiKeys.AUTOMQ_GET_NODES, version);
        this.data = data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        ApiError apiError = ApiError.fromThrowable(e);
        AutomqGetNodesResponseData response = new AutomqGetNodesResponseData()
            .setErrorCode(apiError.error().code())
            .setThrottleTimeMs(throttleTimeMs);
        return new AutomqGetNodesResponse(response);
    }

    @Override
    public AutomqGetNodesRequestData data() {
        return data;
    }

    public static AutomqGetNodesRequest parse(ByteBuffer buffer, short version) {
        return new AutomqGetNodesRequest(new AutomqGetNodesRequestData(new ByteBufferAccessor(buffer), version), version);
    }

    public static class Builder extends AbstractRequest.Builder<CommitStreamObjectRequest> {

        private final CommitStreamObjectRequestData data;

        public Builder(CommitStreamObjectRequestData data) {
            super(ApiKeys.COMMIT_STREAM_OBJECT);
            this.data = data;
        }

        @Override
        public CommitStreamObjectRequest build(short version) {
            return new CommitStreamObjectRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }
}
