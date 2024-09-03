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

import java.nio.ByteBuffer;
import org.apache.kafka.common.message.AutomqZoneRouterRequestData;
import org.apache.kafka.common.message.AutomqZoneRouterResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiError;

public class AutomqZoneRouterRequest extends AbstractRequest {
    private final AutomqZoneRouterRequestData data;

    public AutomqZoneRouterRequest(AutomqZoneRouterRequestData data, short version) {
        super(ApiKeys.AUTOMQ_ZONE_ROUTER, version);
        this.data = data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        ApiError apiError = ApiError.fromThrowable(e);
        AutomqZoneRouterResponseData response = new AutomqZoneRouterResponseData()
            .setErrorCode(apiError.error().code())
            .setThrottleTimeMs(throttleTimeMs);
        return new AutomqZoneRouterResponse(response);
    }

    @Override
    public AutomqZoneRouterRequestData data() {
        return data;
    }

    public static AutomqZoneRouterRequest parse(ByteBuffer buffer, short version) {
        return new AutomqZoneRouterRequest(new AutomqZoneRouterRequestData(new ByteBufferAccessor(buffer), version), version);
    }

    public static class Builder extends AbstractRequest.Builder<AutomqZoneRouterRequest> {

        private final AutomqZoneRouterRequestData data;

        public Builder(AutomqZoneRouterRequestData data) {
            super(ApiKeys.AUTOMQ_ZONE_ROUTER);
            this.data = data;
        }

        @Override
        public AutomqZoneRouterRequest build(short version) {
            return new AutomqZoneRouterRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }
}
