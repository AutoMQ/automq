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

import org.apache.kafka.common.message.AutomqGetOffsetTimestampsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;

import java.nio.ByteBuffer;
import java.util.Map;

public class AutomqGetOffsetTimestampsResponse extends AbstractResponse {
    private final AutomqGetOffsetTimestampsResponseData data;

    public AutomqGetOffsetTimestampsResponse(AutomqGetOffsetTimestampsResponseData data) {
        super(ApiKeys.AUTOMQ_GET_OFFSET_TIMESTAMPS);
        this.data = data;
    }

    @Override
    public AutomqGetOffsetTimestampsResponseData data() {
        return data;
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
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> counts = new java.util.HashMap<>();
        data.topics().forEach(topic -> topic.partitions().forEach(partition ->
            updateErrorCounts(counts, Errors.forCode(partition.errorCode()))));
        return counts;
    }

    public static AutomqGetOffsetTimestampsResponse parse(ByteBuffer buffer, short version) {
        return new AutomqGetOffsetTimestampsResponse(
            new AutomqGetOffsetTimestampsResponseData(new ByteBufferAccessor(buffer), version)
        );
    }
}
