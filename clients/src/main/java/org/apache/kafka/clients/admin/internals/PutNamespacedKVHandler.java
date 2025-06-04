package org.apache.kafka.clients.admin.internals;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.PutKVsRequestData;
import org.apache.kafka.common.message.PutKVsRequestData.PutKVRequest;
import org.apache.kafka.common.message.PutKVsResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.s3.PutKVsRequest;
import org.apache.kafka.common.requests.s3.PutKVsResponse;
import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PutNamespacedKVHandler extends AdminApiHandler.Batched<TopicPartition, Void> {
    private final Logger logger;
    private final NamespacedKVRecordsToPut recordsToPut;
    private final AdminApiLookupStrategy<TopicPartition> lookupStrategy;

    public PutNamespacedKVHandler(LogContext logContext, NamespacedKVRecordsToPut recordsToPut) {
        this.logger = logContext.logger(PutNamespacedKVHandler.class);
        this.recordsToPut = recordsToPut;
        this.lookupStrategy = new PartitionLeaderStrategy(logContext);
    }

    @Override
    protected AbstractRequest.Builder<?> buildBatchedRequest(int brokerId, Set<TopicPartition> partitions) {
        Map<TopicPartition, List<PutKVRequest>> filteredRecords = new HashMap<>();
        for (TopicPartition partition : partitions) {
            if (recordsToPut.recordsByPartition().containsKey(partition)) {
                filteredRecords.put(partition, recordsToPut.recordsByPartition().get(partition));
            }
        }

        PutKVsRequestData requestData = new PutKVsRequestData();
        List<PutKVRequest> allRequests = new ArrayList<>();
        filteredRecords.values().forEach(allRequests::addAll);
        requestData.setPutKVRequests(allRequests);

        return new PutKVsRequest.Builder(requestData);
    }

    @Override
    public String apiName() {
        return "PutKVs";
    }

    @Override
    public ApiResult<TopicPartition, Void> handleResponse(Node broker, Set<TopicPartition> partitions, AbstractResponse response) {
        PutKVsResponse putResponse = (PutKVsResponse) response;
        PutKVsResponseData responseData = putResponse.data();
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
