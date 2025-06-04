package org.apache.kafka.clients.admin.internals;

import org.apache.kafka.clients.admin.NamespacedKVRecordsToDelete;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.DeleteKVsRequestData;
import org.apache.kafka.common.message.DeleteKVsRequestData.DeleteKVRequest;
import org.apache.kafka.common.message.DeleteKVsResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.s3.DeleteKVsRequest;
import org.apache.kafka.common.requests.s3.DeleteKVsResponse;
import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DeleteNamespacedKVHandler extends AdminApiHandler.Batched<TopicPartition, Void> {

    private final Logger logger;
    private final NamespacedKVRecordsToDelete recordsToDelete;
    private final AdminApiLookupStrategy<TopicPartition> lookupStrategy;

    public DeleteNamespacedKVHandler(LogContext logContext, NamespacedKVRecordsToDelete recordsToDelete) {
        this.logger = logContext.logger(PutNamespacedKVHandler.class);
        this.recordsToDelete = recordsToDelete;
        this.lookupStrategy = new PartitionLeaderStrategy(logContext);
    }

    @Override
    AbstractRequest.Builder<?> buildBatchedRequest(int brokerId, Set<TopicPartition> partitions) {
        Map<TopicPartition, List<DeleteKVRequest>> filteredRecords = new HashMap<>();
        for (TopicPartition partition : partitions) {
            if (recordsToDelete.recordsByPartition().containsKey(partition)) {
                filteredRecords.put(partition, recordsToDelete.recordsByPartition().get(partition));
            }
        }

        DeleteKVsRequestData requestData = new DeleteKVsRequestData();
        List<DeleteKVRequest> allRequests = new ArrayList<>();
        filteredRecords.values().forEach(allRequests::addAll);
        requestData.setDeleteKVRequests(allRequests);

        return new DeleteKVsRequest.Builder(requestData);
    }

    @Override
    public String apiName() {
        return "DeleteKVs";
    }

    @Override
    public ApiResult<TopicPartition, Void> handleResponse(Node broker, Set<TopicPartition> partitions, AbstractResponse response) {
        DeleteKVsResponse deleteResponse = (DeleteKVsResponse) response;
        DeleteKVsResponseData responseData = deleteResponse.data();
        final Map<TopicPartition, Void> completed = new HashMap<>();
        final Map<TopicPartition, Throwable> failed = new HashMap<>();

        partitions.forEach(partition -> {
            Errors error = Errors.forCode(responseData.errorCode());
            if (error != Errors.NONE) {
                failed.put(partition, error.exception());
            } else {
                completed.put(partition, null);
            }
        });

        return new ApiResult<>(completed, failed, Collections.emptyList());
    }

    @Override
    public AdminApiLookupStrategy<TopicPartition> lookupStrategy() {
        return this.lookupStrategy;
    }

    public static AdminApiFuture.SimpleAdminApiFuture<TopicPartition, Void> newFuture(
        Set<TopicPartition> partitions
    ) {
        return AdminApiFuture.forKeys(new HashSet<>(partitions));
    }
}
