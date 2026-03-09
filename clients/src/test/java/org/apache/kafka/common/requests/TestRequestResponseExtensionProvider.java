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

import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.Map;

public class TestRequestResponseExtensionProvider implements RequestResponseExtensionProvider {
    @Override
    public AbstractRequest parseRequest(ApiKeys apiKey, short apiVersion, ByteBuffer buffer) {
        return new TestEnterpriseRequest(apiVersion);
    }

    @Override
    public AbstractResponse parseResponse(ApiKeys apiKey, short version, ByteBuffer buffer) {
        return new TestEnterpriseResponse();
    }

    static final class TestEnterpriseRequest extends AbstractRequest {
        private final ApiVersionsRequestData data = new ApiVersionsRequestData();

        TestEnterpriseRequest(short version) {
            super(ApiKeys.FETCH, version);
        }

        @Override
        public ApiVersionsRequestData data() {
            return data;
        }

        @Override
        public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
            return new TestEnterpriseResponse();
        }
    }

    static final class TestEnterpriseResponse extends AbstractResponse {
        private final ApiVersionsResponseData data = new ApiVersionsResponseData();

        TestEnterpriseResponse() {
            super(ApiKeys.FETCH);
        }

        @Override
        public ApiVersionsResponseData data() {
            return data;
        }

        @Override
        public int throttleTimeMs() {
            return 0;
        }

        @Override
        public void maybeSetThrottleTimeMs(int throttleTimeMs) {
            // no-op for tests
        }

        @Override
        public Map<Errors, Integer> errorCounts() {
            return errorCounts(Errors.NONE);
        }
    }
}
