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
import org.apache.kafka.common.message.DeleteKVsRequestData;
import org.apache.kafka.common.message.DeleteKVsRequestData.DeleteKVRequest;
import org.apache.kafka.common.message.DeleteKVsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.ApiError;

public class DeleteKVsRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<DeleteKVsRequest> {

        private final DeleteKVsRequestData data;
        public Builder(DeleteKVsRequestData data) {
            super(ApiKeys.DELETE_KVS);
            this.data = data;
        }

        public Builder addSubRequest(DeleteKVsRequestData.DeleteKVRequest request) {
            List<DeleteKVRequest> requests = data.deleteKVRequests();
            if (requests == null) {
                requests = new ArrayList<>();
                data.setDeleteKVRequests(requests);
            }
            requests.add(request);
            return this;
        }

        @Override
        public DeleteKVsRequest build(short version) {
            return new DeleteKVsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final DeleteKVsRequestData data;

    public DeleteKVsRequest(DeleteKVsRequestData data, short version) {
        super(ApiKeys.DELETE_KVS, version);
        this.data = data;
    }

    @Override
    public DeleteKVsResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        ApiError apiError = ApiError.fromThrowable(e);
        DeleteKVsResponseData response = new DeleteKVsResponseData()
            .setErrorCode(apiError.error().code())
            .setThrottleTimeMs(throttleTimeMs);
        return new DeleteKVsResponse(response);
    }

    @Override
    public DeleteKVsRequestData data() {
        return data;
    }

    public static DeleteKVsRequest parse(ByteBuffer buffer, short version) {
        return new DeleteKVsRequest(new DeleteKVsRequestData(
            new ByteBufferAccessor(buffer), version), version);
    }
}
