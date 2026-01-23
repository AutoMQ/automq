/*
 * Copyright 2025, AutoMQ HK Limited.
 *
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

import org.apache.kafka.common.message.UpdateLicenseRequestData;
import org.apache.kafka.common.message.UpdateLicenseResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.ApiError;

import java.nio.ByteBuffer;

public class UpdateLicenseRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<UpdateLicenseRequest> {

        private final UpdateLicenseRequestData data;

        public Builder(UpdateLicenseRequestData data) {
            super(ApiKeys.UPDATE_LICENSE);
            this.data = data;
        }

        @Override
        public UpdateLicenseRequest build(short version) {
            return new UpdateLicenseRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final UpdateLicenseRequestData data;

    public UpdateLicenseRequest(UpdateLicenseRequestData data, short version) {
        super(ApiKeys.UPDATE_LICENSE, version);
        this.data = data;
    }

    @Override
    public UpdateLicenseResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        ApiError apiError = ApiError.fromThrowable(e);
        UpdateLicenseResponseData response = new UpdateLicenseResponseData()
            .setErrorCode(apiError.error().code())
            .setErrorMessage(apiError.message())
            .setThrottleTimeMs(throttleTimeMs);
        return new UpdateLicenseResponse(response);
    }

    @Override
    public UpdateLicenseRequestData data() {
        return data;
    }

    public static UpdateLicenseRequest parse(ByteBuffer buffer, short version) {
        return new UpdateLicenseRequest(new UpdateLicenseRequestData(
            new ByteBufferAccessor(buffer), version), version);
    }
}
