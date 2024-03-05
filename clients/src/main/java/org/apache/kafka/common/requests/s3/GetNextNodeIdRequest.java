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

import org.apache.kafka.common.message.GetNextNodeIdRequestData;
import org.apache.kafka.common.message.GetNextNodeIdResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;

public class GetNextNodeIdRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<GetNextNodeIdRequest> {
        private final GetNextNodeIdRequestData data;

        public Builder(GetNextNodeIdRequestData data) {
            super(ApiKeys.GET_NEXT_NODE_ID);
            this.data = data;
        }

        @Override
        public GetNextNodeIdRequest build(short version) {
            return new GetNextNodeIdRequest(data, version);
        }
    }


    private final GetNextNodeIdRequestData data;

    public GetNextNodeIdRequest(GetNextNodeIdRequestData data, short version) {
        super(ApiKeys.GET_NEXT_NODE_ID, version);
        this.data = data;
    }

    @Override
    public GetNextNodeIdRequestData data() {
        return data;
    }

    @Override
    public GetNextNodeIdResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        return new GetNextNodeIdResponse(new GetNextNodeIdResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setErrorCode(error.code()));
    }

    public static GetNextNodeIdRequest parse(ByteBuffer buffer, short version) {
        return new GetNextNodeIdRequest(new GetNextNodeIdRequestData(
                new ByteBufferAccessor(buffer), version), version);
    }
}
