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

import org.apache.kafka.common.message.PrepareS3ObjectRequestData;
import org.apache.kafka.common.message.PrepareS3ObjectResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.ApiError;

public class PrepareS3ObjectRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<PrepareS3ObjectRequest> {

        private final PrepareS3ObjectRequestData data;
        public Builder(PrepareS3ObjectRequestData data) {
            super(ApiKeys.DELETE_STREAM);
            this.data = data;
        }

        @Override
        public PrepareS3ObjectRequest build(short version) {
            return new PrepareS3ObjectRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }
    private final PrepareS3ObjectRequestData data;

    public PrepareS3ObjectRequest(PrepareS3ObjectRequestData data, short version) {
        super(ApiKeys.PREPARE_S3_OBJECT, version);
        this.data = data;
    }

    @Override
    public PrepareS3ObjectResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        ApiError apiError = ApiError.fromThrowable(e);
        PrepareS3ObjectResponseData response = new PrepareS3ObjectResponseData()
            .setErrorCode(apiError.error().code())
            .setThrottleTimeMs(throttleTimeMs);
        return new PrepareS3ObjectResponse(response);
    }

    @Override
    public PrepareS3ObjectRequestData data() {
        return data;
    }
    
}
