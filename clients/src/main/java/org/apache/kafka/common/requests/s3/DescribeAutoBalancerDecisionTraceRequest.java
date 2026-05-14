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

import org.apache.kafka.common.message.DescribeAutoBalancerDecisionTraceRequestData;
import org.apache.kafka.common.message.DescribeAutoBalancerDecisionTraceResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.ApiError;

import java.nio.ByteBuffer;

public class DescribeAutoBalancerDecisionTraceRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<DescribeAutoBalancerDecisionTraceRequest> {

        private final DescribeAutoBalancerDecisionTraceRequestData data;

        public Builder(DescribeAutoBalancerDecisionTraceRequestData data) {
            super(ApiKeys.DESCRIBE_AUTO_BALANCER_DECISION_TRACE);
            this.data = data;
        }

        @Override
        public DescribeAutoBalancerDecisionTraceRequest build(short version) {
            return new DescribeAutoBalancerDecisionTraceRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final DescribeAutoBalancerDecisionTraceRequestData data;

    public DescribeAutoBalancerDecisionTraceRequest(DescribeAutoBalancerDecisionTraceRequestData data, short version) {
        super(ApiKeys.DESCRIBE_AUTO_BALANCER_DECISION_TRACE, version);
        this.data = data;
    }

    @Override
    public DescribeAutoBalancerDecisionTraceResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        ApiError apiError = ApiError.fromThrowable(e);
        DescribeAutoBalancerDecisionTraceResponseData response = new DescribeAutoBalancerDecisionTraceResponseData()
            .setErrorCode(apiError.error().code())
            .setErrorMessage(apiError.message())
            .setThrottleTimeMs(throttleTimeMs);
        return new DescribeAutoBalancerDecisionTraceResponse(response);
    }

    @Override
    public DescribeAutoBalancerDecisionTraceRequestData data() {
        return data;
    }

    public static DescribeAutoBalancerDecisionTraceRequest parse(ByteBuffer buffer, short version) {
        return new DescribeAutoBalancerDecisionTraceRequest(new DescribeAutoBalancerDecisionTraceRequestData(
            new ByteBufferAccessor(buffer), version), version);
    }
}
