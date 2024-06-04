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

import org.apache.kafka.common.message.RemoveRaftVoterResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;

public class RemoveRaftVoterResponse extends AbstractResponse {
    private final RemoveRaftVoterResponseData data;

    public RemoveRaftVoterResponse(RemoveRaftVoterResponseData data) {
        super(ApiKeys.REMOVE_RAFT_VOTER);
        this.data = data;
    }

    @Override
    public RemoveRaftVoterResponseData data() {
        return data;
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    @Override
    public void maybeSetThrottleTimeMs(int throttleTimeMs) {
        // not supported
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        if (data.errorCode() != Errors.NONE.code()) {
            return Collections.singletonMap(Errors.forCode(data.errorCode()), 1);
        } else {
            return Collections.emptyMap();
        }
    }

    public static RemoveRaftVoterResponse parse(ByteBuffer buffer, short version) {
        return new RemoveRaftVoterResponse(
            new RemoveRaftVoterResponseData(new ByteBufferAccessor(buffer), version));
    }
}
