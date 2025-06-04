package org.apache.kafka.clients.admin;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.GetKVsResponseData.GetKVResponse;

import java.util.Map;
import java.util.concurrent.ExecutionException;

public class GetNamespacedKVResult {

    private final Map<TopicPartition, KafkaFuture<GetKVResponse>> futures;

    public GetNamespacedKVResult(Map<TopicPartition, KafkaFuture<GetKVResponse>> futures) {
        this.futures = futures;
    }

    public KafkaFuture<Map<TopicPartition, KafkaFuture<GetKVResponse>>> all() throws ExecutionException, InterruptedException {
        return KafkaFuture.completedFuture(futures);
    }
}
