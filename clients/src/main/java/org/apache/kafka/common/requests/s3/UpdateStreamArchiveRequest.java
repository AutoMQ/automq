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

import org.apache.kafka.common.message.UpdateStreamArchiveRequestData;
import org.apache.kafka.common.message.UpdateStreamArchiveRequestData.StreamArchiveOperation;
import org.apache.kafka.common.message.UpdateStreamArchiveResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.ApiError;

import java.nio.ByteBuffer;

/**
 * Version-zero Broker-to-Controller request for typed per-Stream Archive operations.
 */
public class UpdateStreamArchiveRequest extends AbstractRequest {

    /**
     * Builds a version-zero operation request and supports Broker-side positional batching.
     */
    public static class Builder extends AbstractRequest.Builder<UpdateStreamArchiveRequest> {
        private final UpdateStreamArchiveRequestData data;

        /**
         * Creates a builder around the complete request data.
         */
        public Builder(UpdateStreamArchiveRequestData data) {
            super(ApiKeys.UPDATE_STREAM_ARCHIVE);
            this.data = data;
        }

        /**
         * Appends one update while preserving submission order.
         */
        public Builder addSubRequest(StreamArchiveOperation request) {
            data.operations().add(request);
            return this;
        }

        @Override
        public UpdateStreamArchiveRequest build(short version) {
            return new UpdateStreamArchiveRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final UpdateStreamArchiveRequestData data;

    /**
     * Creates a request for the supplied wire version.
     */
    public UpdateStreamArchiveRequest(UpdateStreamArchiveRequestData data, short version) {
        super(ApiKeys.UPDATE_STREAM_ARCHIVE, version);
        this.data = data;
    }

    @Override
    public UpdateStreamArchiveResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        ApiError error = ApiError.fromThrowable(e);
        return new UpdateStreamArchiveResponse(new UpdateStreamArchiveResponseData()
            .setErrorCode(error.error().code())
            .setThrottleTimeMs(throttleTimeMs));
    }

    @Override
    public UpdateStreamArchiveRequestData data() {
        return data;
    }

    /**
     * Parses one request from its protocol payload.
     */
    public static UpdateStreamArchiveRequest parse(ByteBuffer buffer, short version) {
        return new UpdateStreamArchiveRequest(
            new UpdateStreamArchiveRequestData(new ByteBufferAccessor(buffer), version), version);
    }
}
