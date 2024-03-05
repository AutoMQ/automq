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
import org.apache.kafka.common.message.CreateStreamsRequestData;
import org.apache.kafka.common.message.CreateStreamsRequestData.CreateStreamRequest;
import org.apache.kafka.common.message.CreateStreamsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.ApiError;

public class CreateStreamsRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<CreateStreamsRequest> {

        private final CreateStreamsRequestData data;
        public Builder(CreateStreamsRequestData data) {
            super(ApiKeys.CREATE_STREAMS);
            this.data = data;
        }

        public Builder addSubRequest(CreateStreamRequest request) {
            List<CreateStreamRequest> requests = data.createStreamRequests();
            if (requests == null) {
                requests = new ArrayList<>();
                data.setCreateStreamRequests(requests);
            }
            requests.add(request);
            return this;
        }

        @Override
        public CreateStreamsRequest build(short version) {
            return new CreateStreamsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final CreateStreamsRequestData data;

    public CreateStreamsRequest(CreateStreamsRequestData data, short version) {
        super(ApiKeys.CREATE_STREAMS, version);
        this.data = data;
    }

    @Override
    public CreateStreamsResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        ApiError apiError = ApiError.fromThrowable(e);
        CreateStreamsResponseData response = new CreateStreamsResponseData()
                .setErrorCode(apiError.error().code())
                .setThrottleTimeMs(throttleTimeMs);
        return new CreateStreamsResponse(response);
    }

    @Override
    public CreateStreamsRequestData data() {
        return data;
    }

    public static CreateStreamsRequest parse(ByteBuffer buffer, short version) {
        return new CreateStreamsRequest(new CreateStreamsRequestData(
            new ByteBufferAccessor(buffer), version), version);
    }
}
