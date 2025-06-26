package org.apache.kafka.clients.admin.internals;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.PutKVsRequestData;
import org.apache.kafka.common.message.PutKVsRequestData.PutKVRequest;
import org.apache.kafka.common.message.PutKVsResponseData;
import org.apache.kafka.common.message.PutKVsResponseData.PutKVResponse;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.s3.PutKVsRequest;
import org.apache.kafka.common.requests.s3.PutKVsResponse;
import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class PutNamespacedKVHandler extends AdminApiHandler.Batched<TopicPartition, PutKVResponse> {
    private final Logger logger;
    private final NamespacedKVRecordsToPut recordsToPut;
    private final AdminApiLookupStrategy<TopicPartition> lookupStrategy;
    private final List<TopicPartition> orderedPartitions;

    public PutNamespacedKVHandler(LogContext logContext, NamespacedKVRecordsToPut recordsToPut) {
        this.logger = logContext.logger(PutNamespacedKVHandler.class);
        this.recordsToPut = recordsToPut;
        this.lookupStrategy = new PartitionLeaderStrategy(logContext);
        this.orderedPartitions = new ArrayList<>(recordsToPut.recordsByPartition().keySet());
    }

    @Override
    protected AbstractRequest.Builder<?> buildBatchedRequest(int brokerId, Set<TopicPartition> partitions) {
        PutKVsRequestData requestData = new PutKVsRequestData();
        List<PutKVRequest> allPutRequests = orderedPartitions.stream()
            .filter(partitions::contains)
            .map(tp -> recordsToPut.recordsByPartition().get(tp))
            .filter(Objects::nonNull)
            .flatMap(Collection::stream).collect(Collectors.toList());

        requestData.setPutKVRequests(allPutRequests);
        return new PutKVsRequest.Builder(requestData);
    }

    @Override
    public String apiName() {
        return "PutKVs";
    }

    @Override
    public ApiResult<TopicPartition, PutKVResponse> handleResponse(Node broker, Set<TopicPartition> partitions, AbstractResponse response) {
        PutKVsResponseData responseData = ((PutKVsResponse) response).data();
        List<PutKVResponse> responses = responseData.putKVResponses();
        final Map<TopicPartition, PutKVResponse> completed = new LinkedHashMap<>();
        final Map<TopicPartition, Throwable> failed = new HashMap<>();
        int responseIndex = 0;
        for (TopicPartition tp : orderedPartitions) {
            if (!partitions.contains(tp)) {
                continue;
            }
            if (responseIndex >= responses.size()) {
                failed.put(tp, new IllegalStateException("Missing response for partition"));
                continue;
            }
            PutKVResponse resp = responses.get(responseIndex++);
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

    public static AdminApiFuture.SimpleAdminApiFuture<TopicPartition, PutKVResponse> newFuture(
        Set<TopicPartition> partitions
    ) {
        return AdminApiFuture.forKeys(new HashSet<>(partitions));
    }
}
