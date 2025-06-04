package org.apache.kafka.clients.admin.internals;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.GetKVsRequestData;
import org.apache.kafka.common.message.GetKVsResponseData;
import org.apache.kafka.common.message.GetKVsResponseData.GetKVResponse;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.s3.GetKVsRequest;
import org.apache.kafka.common.requests.s3.GetKVsResponse;
import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class GetNamespacedKVHandler extends AdminApiHandler.Batched<TopicPartition, GetKVResponse> {
    private final Logger logger;
    private final NamespacedKVRecordsToGet recordsToGet;
    private final AdminApiLookupStrategy<TopicPartition> lookupStrategy;
    private final List<TopicPartition> orderedPartitions;

    public GetNamespacedKVHandler(LogContext logContext, NamespacedKVRecordsToGet recordsToGet) {
        this.logger = logContext.logger(PutNamespacedKVHandler.class);
        this.recordsToGet = recordsToGet;
        this.lookupStrategy = new PartitionLeaderStrategy(logContext);
        this.orderedPartitions = new ArrayList<>(recordsToGet.recordsByPartition().keySet());
    }

    @Override
    AbstractRequest.Builder<?> buildBatchedRequest(int brokerId, Set<TopicPartition> partitions) {

        GetKVsRequestData requestData = new GetKVsRequestData();
        for (TopicPartition tp : orderedPartitions) {
            if (partitions.contains(tp)) {
                requestData.getKeyRequests().addAll(
                    recordsToGet.recordsByPartition().get(tp)
                );
            }
        }

        return new GetKVsRequest.Builder(requestData);
    }

    @Override
    public String apiName() {
        return "GetKVs";
    }

    @Override
    public ApiResult<TopicPartition, GetKVResponse> handleResponse(Node broker, Set<TopicPartition> partitions, AbstractResponse response) {

        GetKVsResponseData data = ((GetKVsResponse) response).data();
        Map<TopicPartition, GetKVResponse> completed = new LinkedHashMap<>();
        Map<TopicPartition, Throwable> failed = new HashMap<>();
        List<GetKVResponse> responses = data.getKVResponses();
        int responseIndex = 0;
        for (TopicPartition tp : orderedPartitions) {
            if (!partitions.contains(tp)) {
                continue;
            }
            if (responseIndex >= responses.size()) {
                failed.put(tp, new IllegalStateException("Missing response for partition"));
                continue;
            }
            GetKVResponse resp = responses.get(responseIndex++);
            if (resp.errorCode() == Errors.NONE.code()) {
                completed.put(tp, resp);
            } else {
                failed.put(tp, Errors.forCode(resp.errorCode()).exception());
            }
        }
        return new ApiResult<>(completed, failed, Collections.emptyList());
    }

    @Override
    public AdminApiLookupStrategy<TopicPartition> lookupStrategy() {
        return this.lookupStrategy;
    }

    public static AdminApiFuture.SimpleAdminApiFuture<TopicPartition, GetKVResponse> newFuture(
        Set<TopicPartition> partitions
    ) {
        return AdminApiFuture.forKeys(new HashSet<>(partitions));
    }
}
