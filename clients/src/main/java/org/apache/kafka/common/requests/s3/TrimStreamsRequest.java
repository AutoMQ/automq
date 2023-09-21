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
import org.apache.kafka.common.message.TrimStreamsRequestData;
import org.apache.kafka.common.message.TrimStreamsRequestData.TrimStreamRequest;
import org.apache.kafka.common.message.TrimStreamsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.ApiError;

public class TrimStreamsRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<TrimStreamsRequest> {

        private final TrimStreamsRequestData data;
        public Builder(TrimStreamsRequestData data) {
            super(ApiKeys.TRIM_STREAMS);
            this.data = data;
        }

        public Builder addSubRequest(TrimStreamsRequestData.TrimStreamRequest request) {
            List<TrimStreamRequest> requests = data.trimStreamRequests();
            if (requests == null) {
                requests = new ArrayList<>();
                data.setTrimStreamRequests(requests);
            }
            requests.add(request);
            return this;
        }

        @Override
        public TrimStreamsRequest build(short version) {
            return new TrimStreamsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final TrimStreamsRequestData data;
    public TrimStreamsRequest(TrimStreamsRequestData data, short version) {
        super(ApiKeys.TRIM_STREAMS, version);
        this.data = data;
    }

    @Override
    public TrimStreamsResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        ApiError apiError = ApiError.fromThrowable(e);
        TrimStreamsResponseData response = new TrimStreamsResponseData()
            .setErrorCode(apiError.error().code())
            .setThrottleTimeMs(throttleTimeMs);
        return new TrimStreamsResponse(response);
    }

    @Override
    public TrimStreamsRequestData data() {
        return data;
    }

    public static TrimStreamsRequest parse(ByteBuffer buffer, short version) {
        return new TrimStreamsRequest(new TrimStreamsRequestData(
            new ByteBufferAccessor(buffer), version), version);
    }
}
