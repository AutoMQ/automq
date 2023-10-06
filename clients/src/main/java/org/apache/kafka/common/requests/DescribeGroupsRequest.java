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

import org.apache.kafka.common.message.DescribeGroupsRequestData;
import org.apache.kafka.common.message.DescribeGroupsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;

public class DescribeGroupsRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<DescribeGroupsRequest> {
        private final DescribeGroupsRequestData data;

        public Builder(DescribeGroupsRequestData data) {
            super(ApiKeys.DESCRIBE_GROUPS);
            this.data = data;
        }

        @Override
        public DescribeGroupsRequest build(short version) {
            return new DescribeGroupsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final DescribeGroupsRequestData data;

    private DescribeGroupsRequest(DescribeGroupsRequestData data, short version) {
        super(ApiKeys.DESCRIBE_GROUPS, version);
        this.data = data;
    }

    @Override
    public DescribeGroupsRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        DescribeGroupsResponseData describeGroupsResponseData = new DescribeGroupsResponseData();

        data.groups().forEach(groupId ->
            describeGroupsResponseData.groups().add(DescribeGroupsResponse.groupError(groupId, error))
        );

        if (version() >= 1) {
            describeGroupsResponseData.setThrottleTimeMs(throttleTimeMs);
        }

        return new DescribeGroupsResponse(describeGroupsResponseData);
    }

    public static DescribeGroupsRequest parse(ByteBuffer buffer, short version) {
        return new DescribeGroupsRequest(new DescribeGroupsRequestData(new ByteBufferAccessor(buffer), version), version);
    }

    public static List<DescribeGroupsResponseData.DescribedGroup> getErrorDescribedGroupList(
        List<String> groupIds,
        Errors error
    ) {
        return groupIds.stream()
            .map(groupId -> new DescribeGroupsResponseData.DescribedGroup()
                .setGroupId(groupId)
                .setErrorCode(error.code())
            )
            .collect(Collectors.toList());
    }
}
