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
import org.apache.kafka.common.message.DeleteStreamsRequestData;
import org.apache.kafka.common.message.DeleteStreamsRequestData.DeleteStreamRequest;
import org.apache.kafka.common.message.DeleteStreamsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.ApiError;

public class DeleteStreamsRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<DeleteStreamsRequest> {

        private final DeleteStreamsRequestData data;
        public Builder(DeleteStreamsRequestData data) {
            super(ApiKeys.DELETE_STREAMS);
            this.data = data;
        }

        public Builder addSubRequest(DeleteStreamsRequestData.DeleteStreamRequest request) {
            List<DeleteStreamRequest> requests = data.deleteStreamRequests();
            if (requests == null) {
                requests = new ArrayList<>();
                data.setDeleteStreamRequests(requests);
            }
            requests.add(request);
            return this;
        }

        @Override
        public DeleteStreamsRequest build(short version) {
            return new DeleteStreamsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }
    private final DeleteStreamsRequestData data;

    public DeleteStreamsRequest(DeleteStreamsRequestData data, short version) {
        super(ApiKeys.DELETE_STREAMS, version);
        this.data = data;
    }

    @Override
    public DeleteStreamsResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        ApiError apiError = ApiError.fromThrowable(e);
        DeleteStreamsResponseData response = new DeleteStreamsResponseData()
            .setErrorCode(apiError.error().code())
            .setThrottleTimeMs(throttleTimeMs);
        return new DeleteStreamsResponse(response);
    }

    @Override
    public DeleteStreamsRequestData data() {
        return data;
    }

    public static DeleteStreamsRequest parse(ByteBuffer buffer, short version) {
        return new DeleteStreamsRequest(new DeleteStreamsRequestData(
            new ByteBufferAccessor(buffer), version), version);
    }
}
