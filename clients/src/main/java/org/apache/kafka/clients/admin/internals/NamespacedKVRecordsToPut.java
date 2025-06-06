package org.apache.kafka.clients.admin.internals;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.PutKVsRequestData.PutKVRequest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NamespacedKVRecordsToPut {

    private final Map<TopicPartition, List<PutKVRequest>> recordsByPartition;

    private NamespacedKVRecordsToPut(Map<TopicPartition, List<PutKVRequest>> recordsByPartition) {
        this.recordsByPartition = recordsByPartition;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public Map<TopicPartition, List<PutKVRequest>> recordsByPartition() {
        return recordsByPartition;
    }

    public static class Builder {
        private final Map<TopicPartition, List<PutKVRequest>> records = new HashMap<>();

        public Builder addRecord(TopicPartition partition, PutKVRequest request) {
            records.computeIfAbsent(partition, k -> new ArrayList<>()).add(request);
            return this;
        }

        public NamespacedKVRecordsToPut build() {
            return new NamespacedKVRecordsToPut(records);
        }
    }
}
