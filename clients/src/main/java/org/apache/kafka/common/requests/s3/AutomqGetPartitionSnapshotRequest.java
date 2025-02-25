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
