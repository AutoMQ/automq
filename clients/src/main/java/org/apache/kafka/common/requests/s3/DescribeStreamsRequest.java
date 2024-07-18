/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package org.apache.kafka.common.requests.s3;

import java.nio.ByteBuffer;
import org.apache.kafka.common.message.DescribeStreamsRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;

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
