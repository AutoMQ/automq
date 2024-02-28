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

import org.apache.kafka.common.message.ControllerRegistrationRequestData;
import org.apache.kafka.common.message.ControllerRegistrationResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;

public class ControllerRegistrationRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<ControllerRegistrationRequest> {
        private final ControllerRegistrationRequestData data;

        public Builder(ControllerRegistrationRequestData data) {
            super(ApiKeys.CONTROLLER_REGISTRATION);
            this.data = data;
        }

        @Override
        public ControllerRegistrationRequest build(short version) {
            return new ControllerRegistrationRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final ControllerRegistrationRequestData data;

    public ControllerRegistrationRequest(ControllerRegistrationRequestData data, short version) {
        super(ApiKeys.CONTROLLER_REGISTRATION, version);
        this.data = data;
    }

    @Override
    public ControllerRegistrationRequestData data() {
        return data;
    }

    @Override
    public ControllerRegistrationResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        return new ControllerRegistrationResponse(new ControllerRegistrationResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setErrorCode(error.code())
                .setErrorMessage(error.message()));
    }

    public static ControllerRegistrationRequest parse(ByteBuffer buffer, short version) {
        return new ControllerRegistrationRequest(
            new ControllerRegistrationRequestData(new ByteBufferAccessor(buffer), version),
            version);
    }
}
