package org.apache.kafka.clients.admin;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.PutKVsResponseData.PutKVResponse;

import java.util.Map;


public class PutNamespacedKVResult {

    private final Map<TopicPartition, KafkaFuture<PutKVResponse>> futures;

    public PutNamespacedKVResult(Map<TopicPartition, KafkaFuture<PutKVResponse>> futures) {
        this.futures = futures;
    }


    public KafkaFuture<Map<TopicPartition, KafkaFuture<PutKVResponse>>> all() {
        return KafkaFuture.completedFuture(futures);
    }

}
