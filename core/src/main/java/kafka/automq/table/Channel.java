package kafka.automq.table;

import kafka.automq.table.events.AvroCodec;
import kafka.automq.table.events.Envelope;
import kafka.automq.table.events.Event;
import com.automq.enterprise.kafka.utils.ClientUtils;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import kafka.server.KafkaConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Channel {
    public static final long LATEST_OFFSET = -1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(Channel.class);
    private static final String CONTROL_TOPIC = "__automq_table_control";
    private static final String DATA_TOPIC = "__automq_table_data";
    private final Properties clientBaseConfigs;
    private final KafkaProducer<byte[], byte[]> producer;
    private volatile int dataTopicPartitionNums = -1;

    public Channel(KafkaConfig kafkaConfig) {
        clientBaseConfigs = ClientUtils.clusterClientBaseConfig(kafkaConfig);
        Properties props = new Properties();
        props.putAll(clientBaseConfigs);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        producer = new KafkaProducer<>(props);
    }

    public void send(String topic, Event event) throws Exception {
        asyncSend(topic, event).get();
    }

    public CompletableFuture<Void> asyncSend(String topic, Event event) {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        try {
            Callback callback = new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if (e != null) {
                        cf.completeExceptionally(e);
                    } else {
                        cf.complete(null);
                    }
                }
            };
            switch (event.type()) {
                case COMMIT_REQUEST:
                    producer.send(new ProducerRecord<>(CONTROL_TOPIC, 0, null, AvroCodec.encode(event)), callback);
                    break;
                case COMMIT_RESPONSE:
                    int partitionId = Math.abs(topic.hashCode() % dataTopicPartitionNums());
                    producer.send(new ProducerRecord<>(DATA_TOPIC, partitionId, null, AvroCodec.encode(event)), callback);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown event type: " + event.type());
            }
        } catch (Throwable e) {
            cf.completeExceptionally(e);
        }
        return cf;
    }

    public SubChannel subscribeControl() {
        Properties props = new Properties();
        props.putAll(clientBaseConfigs);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "automq-table-control-consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        @SuppressWarnings("resource")
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
        consumer.assign(List.of(new TopicPartition(CONTROL_TOPIC, 0)));
        return new KafkaConsumerSubChannel(consumer);
    }

    public SubChannel subscribeData(String topic, long offset) {
        Properties props = new Properties();
        props.putAll(clientBaseConfigs);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "automq-table-data-consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        @SuppressWarnings("resource")
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
        int partitionNums = dataTopicPartitionNums();
        TopicPartition topicPartition = new TopicPartition(DATA_TOPIC, Math.abs(topic.hashCode() % partitionNums));
        consumer.assign(List.of(topicPartition));
        if (offset == LATEST_OFFSET) {
            consumer.seekToEnd(List.of(topicPartition));
        } else {
            consumer.seek(topicPartition, offset);
        }
        return new KafkaConsumerSubChannel(consumer);
    }

    private int dataTopicPartitionNums() {
        if (dataTopicPartitionNums > 0) {
            return dataTopicPartitionNums;
        }
        synchronized (this) {
            if (dataTopicPartitionNums > 0) {
                return dataTopicPartitionNums;
            }
            dataTopicPartitionNums = producer.partitionsFor(DATA_TOPIC).size();
            return dataTopicPartitionNums;
        }
    }

    public interface SubChannel {
        Envelope poll();

        void close();
    }

    public static class KafkaConsumerSubChannel implements SubChannel {
        private final KafkaConsumer<byte[], byte[]> consumer;
        private final Queue<Envelope> left = new LinkedBlockingQueue<>();

        public KafkaConsumerSubChannel(KafkaConsumer<byte[], byte[]> consumer) {
            this.consumer = consumer;
        }

        @Override
        public Envelope poll() {
            if (!left.isEmpty()) {
                return left.poll();
            }
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(0));
            records.forEach(record -> {
                try {
                    Event event = AvroCodec.decode(record.value());
                    left.add(new Envelope(record.partition(), record.offset(), event));
                } catch (IOException e) {
                    LOGGER.error("decode fail");
                }
            });
            return left.poll();
        }

        @Override
        public void close() {
            consumer.close();
        }
    }

}
