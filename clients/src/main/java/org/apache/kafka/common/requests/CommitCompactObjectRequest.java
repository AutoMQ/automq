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

package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.CommitCompactObjectRequestData;
import org.apache.kafka.common.message.CommitCompactObjectResponseData;
import org.apache.kafka.common.protocol.ApiKeys;

public class CommitCompactObjectRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<CommitCompactObjectRequest> {

        private final CommitCompactObjectRequestData data;
        public Builder(CommitCompactObjectRequestData data) {
            super(ApiKeys.COMMIT_COMPACT_OBJECT);
            this.data = data;
        }

        @Override
        public CommitCompactObjectRequest build(short version) {
            return new CommitCompactObjectRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }
    private final CommitCompactObjectRequestData data;

    public CommitCompactObjectRequest(CommitCompactObjectRequestData data, short version) {
        super(ApiKeys.DELETE_STREAM, version);
        this.data = data;
    }

    @Override
    public CommitCompactObjectResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        ApiError apiError = ApiError.fromThrowable(e);
        CommitCompactObjectResponseData response = new CommitCompactObjectResponseData()
            .setErrorCode(apiError.error().code())
            .setThrottleTimeMs(throttleTimeMs);
        return new CommitCompactObjectResponse(response);
    }

    @Override
    public CommitCompactObjectRequestData data() {
        return data;
    }
}
