package org.apache.kafka.clients.admin;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

public class DeleteNamespacedKVResult extends AbstractOptions<DeleteNamespacedKVResult> {

    private final Map<TopicPartition, KafkaFuture<Void>> futures;

    public DeleteNamespacedKVResult(Map<TopicPartition, KafkaFuture<Void>> futures) {
        this.futures = futures;
    }

    public KafkaFuture<Map<TopicPartition, KafkaFuture<Void>>> all() {
        return KafkaFuture.completedFuture(futures);
    }
}
