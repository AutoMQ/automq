package kafka.automq.table.transformer;

import java.util.Map;

public interface KafkaRecordConvert<T> {

    T convert(String topic, org.apache.kafka.common.record.Record record, int schemaId);

    /**
     * Configure this class.
     * @param configs configs in key/value pairs
     * @param isKey whether this converter is for a key or a value
     */
    void configure(Map<String, ?> configs, boolean isKey);
}
