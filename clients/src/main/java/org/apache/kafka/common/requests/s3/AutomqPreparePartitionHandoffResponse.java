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

import org.apache.kafka.common.message.AutomqPreparePartitionHandoffResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Whole-request outcome for staging a prepare partition handoff batch.
 */
public class AutomqPreparePartitionHandoffResponse extends AbstractResponse {
    private final AutomqPreparePartitionHandoffResponseData data;

    /**
     * Creates a prepare response.
     *
     * @param data top-level response data
     */
    public AutomqPreparePartitionHandoffResponse(AutomqPreparePartitionHandoffResponseData data) {
        super(ApiKeys.AUTOMQ_PREPARE_PARTITION_HANDOFF);
        this.data = data;
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

    @Override
    public AutomqPreparePartitionHandoffResponseData data() {
        return data;
    }

    /**
     * Parses a prepare response body.
     *
     * @param buffer serialized response body
     * @param version protocol version
     * @return parsed prepare response
     */
    public static AutomqPreparePartitionHandoffResponse parse(ByteBuffer buffer, short version) {
        return new AutomqPreparePartitionHandoffResponse(
            new AutomqPreparePartitionHandoffResponseData(new ByteBufferAccessor(buffer), version));
    }
}
