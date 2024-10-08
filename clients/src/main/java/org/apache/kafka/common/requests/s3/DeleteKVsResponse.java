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

import org.apache.kafka.common.message.DeleteKVsResponseData;
import org.apache.kafka.common.message.DeleteKVsResponseData.DeleteKVResponse;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DeleteKVsResponse extends AbstractBatchResponse<DeleteKVResponse> {
    private final DeleteKVsResponseData data;

    public DeleteKVsResponse(DeleteKVsResponseData data) {
        super(ApiKeys.DELETE_KVS);
        this.data = data;
    }

    @Override
    public List<DeleteKVResponse> subResponses() {
        return data.deleteKVResponses();
    }

    @Override
    public DeleteKVsResponseData data() {
        return data;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> errorCounts = new HashMap<>();
        updateErrorCounts(errorCounts, Errors.forCode(data.errorCode()));
        data.deleteKVResponses().forEach(response -> updateErrorCounts(errorCounts, Errors.forCode(response.errorCode())));
        return errorCounts;
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    @Override
    public void maybeSetThrottleTimeMs(int throttleTimeMs) {
        data.setThrottleTimeMs(throttleTimeMs);
    }

    public static DeleteKVsResponse parse(ByteBuffer buffer, short version) {
        return new DeleteKVsResponse(new DeleteKVsResponseData(
            new ByteBufferAccessor(buffer), version));
    }

}
