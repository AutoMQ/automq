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
import org.apache.kafka.common.message.CloseStreamsRequestData;
import org.apache.kafka.common.message.CloseStreamsRequestData.CloseStreamRequest;
import org.apache.kafka.common.message.CloseStreamsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.ApiError;

public class CloseStreamsRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<CloseStreamsRequest> {

        private final CloseStreamsRequestData data;
        public Builder(CloseStreamsRequestData data) {
            super(ApiKeys.CLOSE_STREAMS);
            this.data = data;
        }

        public Builder addSubRequest(CloseStreamsRequestData.CloseStreamRequest request) {
            List<CloseStreamRequest> requests = data.closeStreamRequests();
            if (requests == null) {
                requests = new ArrayList<>();
                data.setCloseStreamRequests(requests);
            }
            requests.add(request);
            return this;
        }

        @Override
        public CloseStreamsRequest build(short version) {
            return new CloseStreamsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }
    private final CloseStreamsRequestData data;

    public CloseStreamsRequest(CloseStreamsRequestData data, short version) {
        super(ApiKeys.CLOSE_STREAMS, version);
        this.data = data;
    }

    @Override
    public CloseStreamsResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        ApiError apiError = ApiError.fromThrowable(e);
        CloseStreamsResponseData response = new CloseStreamsResponseData()
            .setErrorCode(apiError.error().code())
            .setThrottleTimeMs(throttleTimeMs);
        return new CloseStreamsResponse(response);
    }

    @Override
    public CloseStreamsRequestData data() {
        return data;
    }

    public static CloseStreamsRequest parse(ByteBuffer buffer, short version) {
        return new CloseStreamsRequest(new CloseStreamsRequestData(
            new ByteBufferAccessor(buffer), version), version);
    }
}
