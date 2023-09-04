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
import org.apache.kafka.common.message.CommitWALObjectRequestData;
import org.apache.kafka.common.message.CommitWALObjectResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.ApiError;

public class CommitWALObjectRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<CommitWALObjectRequest> {

        private final CommitWALObjectRequestData data;
        public Builder(CommitWALObjectRequestData data) {
            super(ApiKeys.COMMIT_WALOBJECT);
            this.data = data;
        }

        @Override
        public CommitWALObjectRequest build(short version) {
            return new CommitWALObjectRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }
    private final CommitWALObjectRequestData data;

    public CommitWALObjectRequest(CommitWALObjectRequestData data, short version) {
        super(ApiKeys.DELETE_STREAM, version);
        this.data = data;
    }

    @Override
    public CommitWALObjectResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        ApiError apiError = ApiError.fromThrowable(e);
        CommitWALObjectResponseData response = new CommitWALObjectResponseData()
            .setErrorCode(apiError.error().code())
            .setThrottleTimeMs(throttleTimeMs);
        return new CommitWALObjectResponse(response);
    }

    @Override
    public CommitWALObjectRequestData data() {
        return data;
    }

    public static CommitWALObjectRequest parse(ByteBuffer buffer, short version) {
        return new CommitWALObjectRequest(new CommitWALObjectRequestData(
            new ByteBufferAccessor(buffer), version), version);
    }

}
