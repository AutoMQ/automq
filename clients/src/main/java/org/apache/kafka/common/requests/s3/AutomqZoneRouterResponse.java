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
import java.util.Map;
import org.apache.kafka.common.message.AutomqZoneRouterResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;

public class AutomqZoneRouterResponse extends AbstractResponse {
    private final AutomqZoneRouterResponseData data;

    public AutomqZoneRouterResponse(AutomqZoneRouterResponseData data) {
        super(ApiKeys.AUTOMQ_ZONE_ROUTER);
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
    public AutomqZoneRouterResponseData data() {
        return data;
    }

    public static AutomqZoneRouterResponse parse(ByteBuffer buffer, short version) {
        return new AutomqZoneRouterResponse(new AutomqZoneRouterResponseData(new ByteBufferAccessor(buffer), version));
    }
}
