/*
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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.common.message.OpenStreamsRequestData;
import org.apache.kafka.common.message.OpenStreamsRequestData.OpenStreamRequest;
import org.apache.kafka.common.message.OpenStreamsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.ApiError;

public class OpenStreamsRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<OpenStreamsRequest> {

        private final OpenStreamsRequestData data;
        public Builder(OpenStreamsRequestData data) {
            super(ApiKeys.OPEN_STREAMS);
            this.data = data;
        }

        public Builder addSubRequest(OpenStreamRequest request) {
            List<OpenStreamRequest> requests = data.openStreamRequests();
            if (requests == null) {
                requests = new ArrayList<>();
                data.setOpenStreamRequests(requests);
            }
            requests.add(request);
            return this;
        }

        @Override
        public OpenStreamsRequest build(short version) {
            return new OpenStreamsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final OpenStreamsRequestData data;
    public OpenStreamsRequest(OpenStreamsRequestData data, short version) {
        super(ApiKeys.OPEN_STREAMS, version);
        this.data = data;
    }

    @Override
    public OpenStreamsResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        ApiError apiError = ApiError.fromThrowable(e);
        OpenStreamsResponseData response = new OpenStreamsResponseData()
            .setErrorCode(apiError.error().code())
            .setThrottleTimeMs(throttleTimeMs);
        return new OpenStreamsResponse(response);
    }

    @Override
    public OpenStreamsRequestData data() {
        return data;
    }

    public static OpenStreamsRequest parse(ByteBuffer buffer, short version) {
        return new OpenStreamsRequest(new OpenStreamsRequestData(
            new ByteBufferAccessor(buffer), version), version);
    }
}
