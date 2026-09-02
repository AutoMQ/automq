/*
 * Copyright 2026, AutoMQ HK Limited.
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

import org.apache.kafka.common.message.AutomqPreparePartitionHandoffRequestData;
import org.apache.kafka.common.message.AutomqPreparePartitionHandoffResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiError;

import java.nio.ByteBuffer;

/**
 * Broker request that stages a whole batch of optional frozen MetaStream handoffs on a target broker.
 */
public class AutomqPreparePartitionHandoffRequest extends AbstractRequest {
    private final AutomqPreparePartitionHandoffRequestData data;

    /**
     * Creates a versioned prepare request.
     *
     * @param data whole-batch request data
     * @param version protocol version
     */
    public AutomqPreparePartitionHandoffRequest(AutomqPreparePartitionHandoffRequestData data, short version) {
        super(ApiKeys.AUTOMQ_PREPARE_PARTITION_HANDOFF, version);
        this.data = data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        ApiError error = ApiError.fromThrowable(e);
        return new AutomqPreparePartitionHandoffResponse(new AutomqPreparePartitionHandoffResponseData()
            .setErrorCode(error.error().code())
            .setThrottleTimeMs(throttleTimeMs));
    }

    @Override
    public AutomqPreparePartitionHandoffRequestData data() {
        return data;
    }

    /**
     * Parses a prepare request body.
     *
     * @param buffer serialized request body
     * @param version protocol version
     * @return parsed prepare request
     */
    public static AutomqPreparePartitionHandoffRequest parse(ByteBuffer buffer, short version) {
        return new AutomqPreparePartitionHandoffRequest(
            new AutomqPreparePartitionHandoffRequestData(new ByteBufferAccessor(buffer), version), version);
    }

    /**
     * Builds prepare requests from generated protocol data.
     */
    public static class Builder extends AbstractRequest.Builder<AutomqPreparePartitionHandoffRequest> {
        private final AutomqPreparePartitionHandoffRequestData data;

        /**
         * Creates a whole-batch request builder.
         *
         * @param data request data
         */
        public Builder(AutomqPreparePartitionHandoffRequestData data) {
            super(ApiKeys.AUTOMQ_PREPARE_PARTITION_HANDOFF);
            this.data = data;
        }

        @Override
        public AutomqPreparePartitionHandoffRequest build(short version) {
            return new AutomqPreparePartitionHandoffRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }
}
