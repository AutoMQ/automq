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

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.kafka.common.message.RenewDelegationTokenResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

public class RenewDelegationTokenResponse extends AbstractResponse {

    private final RenewDelegationTokenResponseData data;

    public RenewDelegationTokenResponse(RenewDelegationTokenResponseData data) {
        super(ApiKeys.RENEW_DELEGATION_TOKEN);
        this.data = data;
    }

    public static RenewDelegationTokenResponse parse(ByteBuffer buffer, short version) {
        return new RenewDelegationTokenResponse(new RenewDelegationTokenResponseData(
            new ByteBufferAccessor(buffer), version));
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(error());
    }

    @Override
    public RenewDelegationTokenResponseData data() {
        return data;
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    public Errors error() {
        return Errors.forCode(data.errorCode());
    }

    public long expiryTimestamp() {
        return data.expiryTimestampMs();
    }

    public boolean hasError() {
        return error() != Errors.NONE;
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 1;
    }
}
