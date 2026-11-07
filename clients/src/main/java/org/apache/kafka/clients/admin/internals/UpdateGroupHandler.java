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
package org.apache.kafka.clients.admin.internals;

import org.apache.kafka.clients.admin.UpdateGroupSpec;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.message.AutomqUpdateGroupRequestData;
import org.apache.kafka.common.message.AutomqUpdateGroupResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType;
import org.apache.kafka.common.requests.s3.AutomqUpdateGroupRequest;
import org.apache.kafka.common.requests.s3.AutomqUpdateGroupResponse;
import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.singleton;

public class UpdateGroupHandler extends AdminApiHandler.Batched<CoordinatorKey, Void> {
    private final CoordinatorKey groupId;
    private final UpdateGroupSpec groupSpec;
    private final Logger logger;
    private final AdminApiLookupStrategy<CoordinatorKey> lookupStrategy;

    public UpdateGroupHandler(
        String groupId,
        UpdateGroupSpec groupSpec,
        LogContext logContext
    ) {
        this.groupId = CoordinatorKey.byGroupId(groupId);
        this.groupSpec = groupSpec;
        this.logger = logContext.logger(UpdateGroupHandler.class);
        this.lookupStrategy = new CoordinatorStrategy(CoordinatorType.GROUP, logContext);
    }

    @Override
    public String apiName() {
        return "updateGroup";
    }

    @Override
    public AdminApiLookupStrategy<CoordinatorKey> lookupStrategy() {
        return lookupStrategy;
    }

    public static AdminApiFuture.SimpleAdminApiFuture<CoordinatorKey, Void> newFuture(
        String groupId
    ) {
        return AdminApiFuture.forKeys(Collections.singleton(CoordinatorKey.byGroupId(groupId)));
    }

    private void validateKeys(Set<CoordinatorKey> groupIds) {
        if (!groupIds.equals(singleton(groupId))) {
            throw new IllegalArgumentException("Received unexpected group ids " + groupIds +
                " (expected only " + singleton(groupId) + ")");
        }
    }

    @Override
    public AutomqUpdateGroupRequest.Builder buildBatchedRequest(
        int coordinatorId,
        Set<CoordinatorKey> groupIds
    ) {
        validateKeys(groupIds);
        return new AutomqUpdateGroupRequest.Builder(
            new AutomqUpdateGroupRequestData()
                .setLinkId(groupSpec.linkId())
                .setGroupId(this.groupId.idValue)
                .setPromoted(groupSpec.promoted())
        );
    }

    @Override
    public ApiResult<CoordinatorKey, Void> handleResponse(
        Node coordinator,
        Set<CoordinatorKey> groupIds,
        AbstractResponse abstractResponse
    ) {
        validateKeys(groupIds);

        final Map<CoordinatorKey, Void> completed = new HashMap<>();
        final Map<CoordinatorKey, Throwable> failed = new HashMap<>();
        final List<CoordinatorKey> groupsToUnmap = new ArrayList<>();
        AutomqUpdateGroupResponse response = (AutomqUpdateGroupResponse) abstractResponse;
        AutomqUpdateGroupResponseData data = response.data();
        Errors error = Errors.forCode(data.errorCode());
        if (error != Errors.NONE) {
            handleError(
                CoordinatorKey.byGroupId(data.groupId()),
                error,
                data.errorMessage(),
                failed,
                groupsToUnmap
            );
        } else {
            completed.put(groupId, null);
        }
        return new ApiResult<>(completed, failed, groupsToUnmap);
    }

    private void handleError(
        CoordinatorKey groupId,
        Errors error,
        String errorMsg,
        Map<CoordinatorKey, Throwable> failed,
        List<CoordinatorKey> groupsToUnmap
    ) {
        switch (error) {
            case COORDINATOR_NOT_AVAILABLE:
            case NOT_COORDINATOR:
                // If the coordinator is unavailable or there was a coordinator change, then we unmap
                // the key so that we retry the `FindCoordinator` request
                logger.debug("`{}` request for group id {} returned error {}. " +
                    "Will attempt to find the coordinator again and retry.", apiName(), groupId.idValue, error);
                groupsToUnmap.add(groupId);
                break;
            default:
                logger.error("`{}` request for group id {} failed due to unexpected error {}.", apiName(), groupId.idValue, error);
                failed.put(groupId, error.exception(errorMsg));
        }
    }
}
