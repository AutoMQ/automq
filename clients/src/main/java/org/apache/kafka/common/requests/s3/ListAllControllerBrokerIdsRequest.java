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

import org.apache.kafka.common.message.ListAllControllerBrokerIdsRequestData;
import org.apache.kafka.common.message.ListAllControllerBrokerIdsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;

public class ListAllControllerBrokerIdsRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<ListAllControllerBrokerIdsRequest> {
        private final ListAllControllerBrokerIdsRequestData data;

        public Builder(ListAllControllerBrokerIdsRequestData data) {
            super(ApiKeys.LIST_ALL_CONTROLLER_BROKER_IDS);
            this.data = data;
        }

        @Override
        public ListAllControllerBrokerIdsRequest build(short version) {
            return new ListAllControllerBrokerIdsRequest(data, version);
        }
    }


    private final ListAllControllerBrokerIdsRequestData data;

    public ListAllControllerBrokerIdsRequest(ListAllControllerBrokerIdsRequestData data, short version) {
        super(ApiKeys.LIST_ALL_CONTROLLER_BROKER_IDS, version);
        this.data = data;
    }

    @Override
    public ListAllControllerBrokerIdsRequestData data() {
        return data;
    }

    @Override
    public ListAllControllerBrokerIdsResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        return new ListAllControllerBrokerIdsResponse(new ListAllControllerBrokerIdsResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setErrorCode(error.code()));
    }

    public static ListAllControllerBrokerIdsRequest parse(ByteBuffer buffer, short version) {
        return new ListAllControllerBrokerIdsRequest(new ListAllControllerBrokerIdsRequestData(
                new ByteBufferAccessor(buffer), version), version);
    }
}
