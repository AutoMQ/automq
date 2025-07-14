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

import org.apache.kafka.common.message.AutomqRegisterNodeRequestData;
import org.apache.kafka.common.message.AutomqRegisterNodeResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.ApiError;

import java.nio.ByteBuffer;

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
