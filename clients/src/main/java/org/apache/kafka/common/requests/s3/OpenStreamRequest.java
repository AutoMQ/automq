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

import org.apache.kafka.common.message.OpenStreamRequestData;
import org.apache.kafka.common.message.OpenStreamResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.ApiError;

public class OpenStreamRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<OpenStreamRequest> {

        private final OpenStreamRequestData data;
        public Builder(OpenStreamRequestData data) {
            super(ApiKeys.CREATE_STREAM);
            this.data = data;
        }

        @Override
        public OpenStreamRequest build(short version) {
            return new OpenStreamRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final OpenStreamRequestData data;
    public OpenStreamRequest(OpenStreamRequestData data, short version) {
        super(ApiKeys.OPEN_STREAM, version);
        this.data = data;
    }

    @Override
    public OpenStreamResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        ApiError apiError = ApiError.fromThrowable(e);
        OpenStreamResponseData response = new OpenStreamResponseData()
            .setErrorCode(apiError.error().code())
            .setThrottleTimeMs(throttleTimeMs);
        return new OpenStreamResponse(response);
    }

    @Override
    public OpenStreamRequestData data() {
        return data;
    }
}
