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

import org.apache.kafka.common.message.AutomqGetPartitionSnapshotRequestData;
import org.apache.kafka.common.message.AutomqGetPartitionSnapshotResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiError;

import java.nio.ByteBuffer;

public class AutomqGetPartitionSnapshotRequest extends AbstractRequest {
    private final AutomqGetPartitionSnapshotRequestData data;

    public AutomqGetPartitionSnapshotRequest(AutomqGetPartitionSnapshotRequestData data, short version) {
        super(ApiKeys.AUTOMQ_GET_PARTITION_SNAPSHOT, version);
        this.data = data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        ApiError apiError = ApiError.fromThrowable(e);
        AutomqGetPartitionSnapshotResponseData response = new AutomqGetPartitionSnapshotResponseData()
            .setErrorCode(apiError.error().code())
            .setThrottleTimeMs(throttleTimeMs);
        return new AutomqGetPartitionSnapshotResponse(response);
    }

    @Override
    public AutomqGetPartitionSnapshotRequestData data() {
        return data;
    }

    public static AutomqGetPartitionSnapshotRequest parse(ByteBuffer buffer, short version) {
        return new AutomqGetPartitionSnapshotRequest(new AutomqGetPartitionSnapshotRequestData(new ByteBufferAccessor(buffer), version), version);
    }

    public static class Builder extends AbstractRequest.Builder<AutomqGetPartitionSnapshotRequest> {
        private final AutomqGetPartitionSnapshotRequestData data;

        public Builder(AutomqGetPartitionSnapshotRequestData data) {
            super(ApiKeys.AUTOMQ_GET_PARTITION_SNAPSHOT);
            this.data = data;
        }

        @Override
        public AutomqGetPartitionSnapshotRequest build(short version) {
            return new AutomqGetPartitionSnapshotRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }
}
