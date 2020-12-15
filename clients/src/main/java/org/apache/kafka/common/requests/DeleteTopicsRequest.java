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

import org.apache.kafka.common.message.DeleteTopicsRequestData;
import org.apache.kafka.common.message.DeleteTopicsResponseData;
import org.apache.kafka.common.message.DeleteTopicsResponseData.DeletableTopicResult;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;

import java.nio.ByteBuffer;

public class DeleteTopicsRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<DeleteTopicsRequest> {
        private DeleteTopicsRequestData data;

        public Builder(DeleteTopicsRequestData data) {
            super(ApiKeys.DELETE_TOPICS);
            this.data = data;
        }

        @Override
        public DeleteTopicsRequest build(short version) {
            return new DeleteTopicsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private DeleteTopicsRequestData data;

    private DeleteTopicsRequest(DeleteTopicsRequestData data, short version) {
        super(ApiKeys.DELETE_TOPICS, version);
        this.data = data;
    }

    @Override
    public DeleteTopicsRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        DeleteTopicsResponseData response = new DeleteTopicsResponseData();
        if (version() >= 1) {
            response.setThrottleTimeMs(throttleTimeMs);
        }
        ApiError apiError = ApiError.fromThrowable(e);
        for (String topic : data.topicNames()) {
            response.responses().add(new DeletableTopicResult()
                    .setName(topic)
                    .setErrorCode(apiError.error().code()));
        }
        return new DeleteTopicsResponse(response);
    }

    public static DeleteTopicsRequest parse(ByteBuffer buffer, short version) {
        return new DeleteTopicsRequest(new DeleteTopicsRequestData(new ByteBufferAccessor(buffer), version), version);
    }

}
