/*
 * Copyright 2025, AutoMQ HK Limited.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.common.requests.s3;

import org.apache.kafka.common.message.AutomqGetNodesRequestData;
import org.apache.kafka.common.message.AutomqGetNodesResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiError;

import java.nio.ByteBuffer;

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

    public static class Builder extends AbstractRequest.Builder<AutomqGetNodesRequest> {

        private final AutomqGetNodesRequestData data;

        public Builder(AutomqGetNodesRequestData data) {
            super(ApiKeys.AUTOMQ_GET_NODES);
            this.data = data;
        }

        @Override
        public AutomqGetNodesRequest build(short version) {
            return new AutomqGetNodesRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }
}
