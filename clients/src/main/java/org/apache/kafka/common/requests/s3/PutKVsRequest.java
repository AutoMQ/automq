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
import org.apache.kafka.common.message.PutKVsRequestData;
import org.apache.kafka.common.message.PutKVsRequestData.PutKVRequest;
import org.apache.kafka.common.message.PutKVsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.ApiError;

public class PutKVsRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<PutKVsRequest> {

        private final PutKVsRequestData data;
        public Builder(PutKVsRequestData data) {
            super(ApiKeys.PUT_KVS);
            this.data = data;
        }

        public Builder addSubRequest(PutKVRequest request) {
            List<PutKVRequest> requests = data.putKVRequests();
            if (requests == null) {
                requests = new ArrayList<>();
                data.setPutKVRequests(requests);
            }
            requests.add(request);
            return this;
        }

        @Override
        public PutKVsRequest build(short version) {
            return new PutKVsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final PutKVsRequestData data;

    public PutKVsRequest(PutKVsRequestData data, short version) {
        super(ApiKeys.PUT_KVS, version);
        this.data = data;
    }

    @Override
    public PutKVsResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        ApiError apiError = ApiError.fromThrowable(e);
        PutKVsResponseData response = new PutKVsResponseData()
            .setErrorCode(apiError.error().code())
            .setThrottleTimeMs(throttleTimeMs);
        return new PutKVsResponse(response);
    }

    @Override
    public PutKVsRequestData data() {
        return data;
    }

    public static PutKVsRequest parse(ByteBuffer buffer, short version) {
        return new PutKVsRequest(new PutKVsRequestData(
            new ByteBufferAccessor(buffer), version), version);
    }
}
