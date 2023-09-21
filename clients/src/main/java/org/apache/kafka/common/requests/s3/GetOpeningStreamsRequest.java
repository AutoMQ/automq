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
import org.apache.kafka.common.message.GetOpeningStreamsRequestData;
import org.apache.kafka.common.message.GetOpeningStreamsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.ApiError;

public class GetOpeningStreamsRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<GetOpeningStreamsRequest> {

        private final GetOpeningStreamsRequestData data;
        public Builder(GetOpeningStreamsRequestData data) {
            super(ApiKeys.GET_OPENING_STREAMS);
            this.data = data;
        }

        @Override
        public GetOpeningStreamsRequest build(short version) {
            return new GetOpeningStreamsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final GetOpeningStreamsRequestData data;

    public GetOpeningStreamsRequest(GetOpeningStreamsRequestData data, short version) {
        super(ApiKeys.GET_OPENING_STREAMS, version);
        this.data = data;
    }

    @Override
    public GetOpeningStreamsResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        ApiError apiError = ApiError.fromThrowable(e);
        GetOpeningStreamsResponseData response = new GetOpeningStreamsResponseData()
            .setErrorCode(apiError.error().code())
            .setThrottleTimeMs(throttleTimeMs);
        return new GetOpeningStreamsResponse(response);
    }

    @Override
    public GetOpeningStreamsRequestData data() {
        return data;
    }

    public static GetOpeningStreamsRequest parse(ByteBuffer buffer, short version) {
        return new GetOpeningStreamsRequest(new GetOpeningStreamsRequestData(
            new ByteBufferAccessor(buffer), version), version);
    }
    
}
