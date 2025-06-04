package org.apache.kafka.clients.admin;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;


public class PutNamespacedKVResult {

    private final Map<TopicPartition, KafkaFuture<Void>> futures;

    /**
     * Return a future which succeeds only if all the records deletions succeed.
     */
    public PutNamespacedKVResult(Map<TopicPartition, KafkaFuture<Void>> futures) {
        this.futures = futures;
    }

    /**
     * Return a future which succeeds only if all the records deletions succeed.
     */
    public KafkaFuture<Map<TopicPartition, KafkaFuture<Void>>> all() {
        return KafkaFuture.completedFuture(futures);
    }

//    /**
//     * Return a future which succeeds if the put operation is successful.
//     */
//    public Map<TopicPartition, KafkaFuture<PutKVRecords>> all() {
//        return futures;
//    }

}
