/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.automq.kafkalinking;

import org.apache.kafka.common.message.AutomqUpdateGroupsRequestData;
import org.apache.kafka.common.message.AutomqUpdateGroupsResponseData;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.coordinator.group.GroupCoordinator;

import java.util.concurrent.CompletableFuture;

public abstract class KafkaLinkingGroupCoordinator implements GroupCoordinator {
    /**
     * Update consumer groups
     *
     * @param context           The coordinator request context.
     * @param request           The AutomqUpdateGroupsRequestData data.
     * @param bufferSupplier    The buffer supplier tight to the request thread.
     *
     * @return  A future yielding the response.
     *          The error code(s) of the response are set to indicate the error(s) occurred during the execution.
     */
    public abstract CompletableFuture<AutomqUpdateGroupsResponseData> updateGroups(
        RequestContext context,
        AutomqUpdateGroupsRequestData request,
        BufferSupplier bufferSupplier
    );

}
