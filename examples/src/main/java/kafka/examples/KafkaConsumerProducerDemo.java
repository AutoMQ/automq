cat << 'EOF' > src/main/java/kafka/examples/KafkaConsumerProducerDemo.java
package kafka.examples;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * KafkaConsumerProducerDemo demonstrates a simple Kafka producer-consumer flow.
 * 
 * Steps:
 * 1. Cleans any previous topic data.
 * 2. Starts a producer thread to send a number of records to a topic.
 * 3. Starts a consumer thread to consume the records from the topic.
 * 
 * Usage:
 * java KafkaConsumerProducerDemo <numRecords> [sync]
 * - numRecords: total number of records to produce
 * - sync: optional argument; if 'sync', sends records synchronously, else async
 */
public class KafkaConsumerProducerDemo {
    public static final String TOPIC = "my-topic";   // Topic name for producer/consumer
    public static final String GROUP = "my-group";   // Consumer group

    public static void main(String[] args) {
        // Validate input arguments
        if (args.length == 0) {
            System.out.println("Usage: java KafkaConsumerProducerDemo <numRecords> [sync]");
            return;
        }

        int numRecords = Integer.parseInt(args[0]);
        boolean isAsync = args.length == 1 || !args[1].equalsIgnoreCase("sync");

        try {
            // Stage 1: Clean the topic from previous runs
            Utils.recreateTopics(KafkaProperties.BOOTSTRAP_SERVERS, -1, TOPIC);
            CountDownLatch latch = new CountDownLatch(2);

            // Stage 2: Start producer thread
            Producer producer = new Producer(
                "producer",
                KafkaProperties.BOOTSTRAP_SERVERS,
                TOPIC,
                isAsync,
                null,
                false,
                numRecords,
                -1,
                latch
            );
            producer.start();

            // Stage 3: Start consumer thread
            Consumer consumer = new Consumer(
                "consumer",
                KafkaProperties.BOOTSTRAP_SERVERS,
                TOPIC,
                GROUP,
                Optional.empty(),
                false,
                numRecords,
                latch
            );
            consumer.start();

            // Wait for both threads to finish (timeout 5 minutes)
            if (!latch.await(5, TimeUnit.MINUTES)) {
                System.err.println("Timeout after 5 minutes. Shutting down producer and consumer.");
                producer.shutdown();
                consumer.shutdown();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
EOF