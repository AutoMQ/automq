package org.apache.kafka.clients.admin;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.DeleteKVsRequestData.DeleteKVRequest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NamespacedKVRecordsToDelete {

    private final Map<TopicPartition, List<DeleteKVRequest>> recordsByPartition;

    public NamespacedKVRecordsToDelete(Map<TopicPartition, List<DeleteKVRequest>> recordsByPartition) {
        this.recordsByPartition = recordsByPartition;
    }

    public static NamespacedKVRecordsToDelete.Builder newBuilder() {
        return new NamespacedKVRecordsToDelete.Builder();
    }

    public Map<TopicPartition, List<DeleteKVRequest>> recordsByPartition() {
        return recordsByPartition;
    }

    public static class Builder {
        private final Map<TopicPartition, List<DeleteKVRequest>> records = new HashMap<>();

        public NamespacedKVRecordsToDelete.Builder addRecord(TopicPartition partition, DeleteKVRequest request) {
            records.computeIfAbsent(partition, k -> new ArrayList<>()).add(request);
            return this;
        }

        public NamespacedKVRecordsToDelete build() {
            return new NamespacedKVRecordsToDelete(records);
        }
    }

}
