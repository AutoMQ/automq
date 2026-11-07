package kafka.automq.table.process;

import org.apache.kafka.common.record.Record;

import java.util.Objects;

/**
 * Provides contextual information to {@link Transform} operations.
 *
 * <p>This immutable class acts as a container for metadata related to the record
 * being processed, but separate from the record's own data. This allows transforms
 * to access information like the original Kafka record's headers or the topic name
 * without polluting the data record itself.</p>
 *
 * @see Transform#apply(org.apache.avro.generic.GenericRecord, TransformContext)
 */
public final class TransformContext {

    private final Record kafkaRecord;
    private final String topicName;
    private final int partition;

    /**
     * Constructs a new TransformContext.
     *
     * @param kafkaRecord the original Kafka Record, can be null if not available.
     * @param topicName the name of the topic from which the record was consumed, must not be null.
     * @param partition the partition number.
     */
    public TransformContext(Record kafkaRecord, String topicName, int partition) {
        this.kafkaRecord = kafkaRecord;
        this.topicName = Objects.requireNonNull(topicName, "topicName cannot be null");
        this.partition = partition;
    }

    public Record getKafkaRecord() {
        return kafkaRecord;
    }

    public String getTopicName() {
        return topicName;
    }

    public int getPartition() {
        return partition;
    }
}
