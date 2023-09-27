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
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.message.GetNextNodeIdResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;

public class GetNextNodeIdResponse extends AbstractResponse {
    public final GetNextNodeIdResponseData data;

    public GetNextNodeIdResponse(GetNextNodeIdResponseData data) {
        super(ApiKeys.GET_NEXT_NODE_ID);
        this.data = data;
    }

    @Override
    public GetNextNodeIdResponseData data() {
        return data;
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    @Override
    public void maybeSetThrottleTimeMs(int throttleTimeMs) {
        data.setThrottleTimeMs(throttleTimeMs);
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> errorCounts = new HashMap<>();
        errorCounts.put(Errors.forCode(data.errorCode()), 1);
        return errorCounts;
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return true;
    }

    @Override
    public String toString() {
        return data.toString();
    }

    public static GetNextNodeIdResponse parse(ByteBuffer buffer, short version) {
        return new GetNextNodeIdResponse(new GetNextNodeIdResponseData(
                new ByteBufferAccessor(buffer), version));
    }
}
