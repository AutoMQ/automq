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

import org.apache.kafka.common.message.DescribeStreamsRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;

import java.nio.ByteBuffer;

public class DescribeStreamsRequest extends AbstractRequest {

    private final DescribeStreamsRequestData data;

    public static class Builder extends AbstractRequest.Builder<DescribeStreamsRequest> {

        private final DescribeStreamsRequestData data;

        public Builder(DescribeStreamsRequestData data) {
            super(ApiKeys.DESCRIBE_STREAMS);
            this.data = data;
        }

        @Override
        public DescribeStreamsRequest build(short version) {
            return new DescribeStreamsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    public DescribeStreamsRequest(DescribeStreamsRequestData data, short version) {
        super(ApiKeys.DESCRIBE_STREAMS, version);
        this.data = data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return null;
    }

    @Override
    public DescribeStreamsRequestData data() {
        return data;
    }

    public static DescribeStreamsRequest parse(ByteBuffer buffer, short version) {
        return new DescribeStreamsRequest(new DescribeStreamsRequestData(
            new ByteBufferAccessor(buffer), version), version);
    }
}
