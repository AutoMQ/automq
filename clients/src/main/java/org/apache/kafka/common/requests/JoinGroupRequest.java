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

import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.Collections;

public class JoinGroupRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<JoinGroupRequest> {

        private final JoinGroupRequestData data;

        public Builder(JoinGroupRequestData data) {
            super(ApiKeys.JOIN_GROUP);
            this.data = data;
        }

        @Override
        public JoinGroupRequest build(short version) {
            return new JoinGroupRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final JoinGroupRequestData data;
    private final short version;

    public static final String UNKNOWN_MEMBER_ID = "";

    public JoinGroupRequest(JoinGroupRequestData data, short version) {
        super(ApiKeys.JOIN_GROUP, version);
        this.data = data;
        this.version = version;
    }

    public JoinGroupRequest(Struct struct, short version) {
        super(ApiKeys.JOIN_GROUP, version);
        this.data = new JoinGroupRequestData(struct, version);
        this.version = version;
    }

    public JoinGroupRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        short versionId = version();
        switch (versionId) {
            case 0:
            case 1:
                return new JoinGroupResponse(
                        new JoinGroupResponseData()
                                .setErrorCode(Errors.forException(e).code())
                                .setGenerationId(JoinGroupResponse.UNKNOWN_GENERATION_ID)
                                .setProtocolName(JoinGroupResponse.UNKNOWN_PROTOCOL)
                                .setLeader(JoinGroupResponse.UNKNOWN_MEMBER_ID)
                                .setMemberId(JoinGroupResponse.UNKNOWN_MEMBER_ID)
                                .setMembers(Collections.emptyList())
                );
            case 2:
            case 3:
            case 4:
                return new JoinGroupResponse(
                        new JoinGroupResponseData()
                                .setThrottleTimeMs(throttleTimeMs)
                                .setErrorCode(Errors.forException(e).code())
                                .setGenerationId(JoinGroupResponse.UNKNOWN_GENERATION_ID)
                                .setProtocolName(JoinGroupResponse.UNKNOWN_PROTOCOL)
                                .setLeader(JoinGroupResponse.UNKNOWN_MEMBER_ID)
                                .setMemberId(JoinGroupResponse.UNKNOWN_MEMBER_ID)
                                .setMembers(Collections.emptyList())
                );
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ApiKeys.JOIN_GROUP.latestVersion()));
        }
    }

    public static JoinGroupRequest parse(ByteBuffer buffer, short version) {
        return new JoinGroupRequest(ApiKeys.JOIN_GROUP.parseRequest(version, buffer), version);
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version);
    }
}
