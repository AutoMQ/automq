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

import org.apache.kafka.common.message.GetKVsRequestData;
import org.apache.kafka.common.message.GetKVsRequestData.GetKVRequest;
import org.apache.kafka.common.message.GetKVsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.ApiError;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class GetKVsRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<GetKVsRequest> {

        private final GetKVsRequestData data;
        public Builder(GetKVsRequestData data) {
            super(ApiKeys.GET_KVS);
            this.data = data;
        }

        public Builder addSubRequest(GetKVsRequestData.GetKVRequest request) {
            List<GetKVRequest> keyRequests = data.getKeyRequests();
            if (keyRequests == null) {
                keyRequests = new ArrayList<>();
                data.setGetKeyRequests(keyRequests);
            }
            keyRequests.add(request);
            return this;
        }

        @Override
        public GetKVsRequest build(short version) {
            return new GetKVsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final GetKVsRequestData data;

    public GetKVsRequest(GetKVsRequestData data, short version) {
        super(ApiKeys.GET_KVS, version);
        this.data = data;
    }

    @Override
    public GetKVsResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        ApiError apiError = ApiError.fromThrowable(e);
        GetKVsResponseData response = new GetKVsResponseData()
            .setErrorCode(apiError.error().code())
            .setThrottleTimeMs(throttleTimeMs);
        return new GetKVsResponse(response);
    }

    @Override
    public GetKVsRequestData data() {
        return data;
    }

    public static GetKVsRequest parse(ByteBuffer buffer, short version) {
        return new GetKVsRequest(new GetKVsRequestData(
            new ByteBufferAccessor(buffer), version), version);
    }
    
}
