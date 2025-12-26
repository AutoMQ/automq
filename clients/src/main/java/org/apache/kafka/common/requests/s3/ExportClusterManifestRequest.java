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

import org.apache.kafka.common.message.ExportClusterManifestRequestData;
import org.apache.kafka.common.message.ExportClusterManifestResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.ApiError;

import java.nio.ByteBuffer;

public class ExportClusterManifestRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<ExportClusterManifestRequest> {

        private final ExportClusterManifestRequestData data;

        public Builder(ExportClusterManifestRequestData data) {
            super(ApiKeys.EXPORT_CLUSTER_MANIFEST);
            this.data = data;
        }

        @Override
        public ExportClusterManifestRequest build(short version) {
            return new ExportClusterManifestRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final ExportClusterManifestRequestData data;

    public ExportClusterManifestRequest(ExportClusterManifestRequestData data, short version) {
        super(ApiKeys.EXPORT_CLUSTER_MANIFEST, version);
        this.data = data;
    }

    @Override
    public ExportClusterManifestResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        ApiError apiError = ApiError.fromThrowable(e);
        ExportClusterManifestResponseData response = new ExportClusterManifestResponseData()
            .setErrorCode(apiError.error().code())
            .setThrottleTimeMs(throttleTimeMs);
        return new ExportClusterManifestResponse(response);
    }

    @Override
    public ExportClusterManifestRequestData data() {
        return data;
    }

    public static ExportClusterManifestRequest parse(ByteBuffer buffer, short version) {
        return new ExportClusterManifestRequest(new ExportClusterManifestRequestData(
            new ByteBufferAccessor(buffer), version), version);
    }
}
