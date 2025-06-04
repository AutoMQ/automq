package org.apache.kafka.clients.admin.internals;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.GetKVsRequestData.GetKVRequest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NamespacedKVRecordsToGet {

    private final Map<TopicPartition, List<GetKVRequest>> recordsByPartition;

    public NamespacedKVRecordsToGet(Map<TopicPartition, List<GetKVRequest>> recordsByPartition) {
        this.recordsByPartition = recordsByPartition;
    }

    public static NamespacedKVRecordsToGet.Builder newBuilder() {
        return new NamespacedKVRecordsToGet.Builder();
    }

    public static class Builder {
        private final Map<TopicPartition, List<GetKVRequest>> records = new HashMap<>();
        public Builder addRecord(TopicPartition tp, GetKVRequest req) {
            records.computeIfAbsent(tp, k -> new ArrayList<>()).add(req);
            return this;
        }

        public NamespacedKVRecordsToGet build() {
            return new NamespacedKVRecordsToGet(records);
        }
    }

    public Map<TopicPartition, List<GetKVRequest>> recordsByPartition() {
        return recordsByPartition;
    }
}
