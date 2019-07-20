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

import org.apache.kafka.common.message.DeleteGroupsRequestData;
import org.apache.kafka.common.message.DeleteGroupsResponseData;
import org.apache.kafka.common.message.DeleteGroupsResponseData.DeletableGroupResult;
import org.apache.kafka.common.message.DeleteGroupsResponseData.DeletableGroupResultCollection;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;

public class DeleteGroupsRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<DeleteGroupsRequest> {
        private final DeleteGroupsRequestData data;

        public Builder(DeleteGroupsRequestData data) {
            super(ApiKeys.DELETE_GROUPS);
            this.data = data;
        }

        @Override
        public DeleteGroupsRequest build(short version) {
            return new DeleteGroupsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    public final DeleteGroupsRequestData data;

    public DeleteGroupsRequest(DeleteGroupsRequestData data, short version) {
        super(ApiKeys.DELETE_GROUPS, version);
        this.data = data;
    }

    public DeleteGroupsRequest(Struct struct, short version) {
        super(ApiKeys.DELETE_GROUPS, version);
        this.data = new DeleteGroupsRequestData(struct, version);
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);

        switch (version()) {
            case 0:
            case 1:
                DeletableGroupResultCollection groupResults = new DeletableGroupResultCollection();
                for (String groupId : data.groupsNames()) {
                    groupResults.add(new DeletableGroupResult()
                                         .setGroupId(groupId)
                                         .setErrorCode(error.code()));
                }

                return new DeleteGroupsResponse(
                    new DeleteGroupsResponseData()
                        .setResults(groupResults)
                        .setThrottleTimeMs(throttleTimeMs)
                );
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                    version(), ApiKeys.DELETE_GROUPS.name, ApiKeys.DELETE_GROUPS.latestVersion()));
        }
    }

    public static DeleteGroupsRequest parse(ByteBuffer buffer, short version) {
        return new DeleteGroupsRequest(ApiKeys.DELETE_GROUPS.parseRequest(version, buffer), version);
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version());
    }
}
