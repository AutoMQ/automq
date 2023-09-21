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
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.message.CreateStreamsResponseData;
import org.apache.kafka.common.message.CreateStreamsResponseData.CreateStreamResponse;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

public class CreateStreamsResponse extends AbstractBatchResponse<CreateStreamResponse> {

    private final CreateStreamsResponseData data;

    public CreateStreamsResponse(CreateStreamsResponseData data) {
        super(ApiKeys.CREATE_STREAMS);
        this.data = data;
    }

    @Override
    public List<CreateStreamResponse> subResponses() {
        return data.createStreamResponses();
    }

    @Override
    public CreateStreamsResponseData data() {
        return data;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(Errors.forCode(data.errorCode()));
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    @Override
    public void maybeSetThrottleTimeMs(int throttleTimeMs) {
        data.setThrottleTimeMs(throttleTimeMs);
    }

    public static CreateStreamsResponse parse(ByteBuffer buffer, short version) {
        return new CreateStreamsResponse(new CreateStreamsResponseData(
            new ByteBufferAccessor(buffer), version));
    }
}
