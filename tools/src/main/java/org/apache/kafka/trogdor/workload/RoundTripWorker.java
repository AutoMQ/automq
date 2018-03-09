/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.trogdor.workload;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.trogdor.common.Platform;
import org.apache.kafka.trogdor.common.ThreadUtils;
import org.apache.kafka.trogdor.common.WorkerUtils;
import org.apache.kafka.trogdor.task.TaskWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class RoundTripWorker implements TaskWorker {
    private static final int THROTTLE_PERIOD_MS = 100;

    private static final int MESSAGE_SIZE = 512;

    private static final int LOG_INTERVAL_MS = 5000;

    private static final int LOG_NUM_MESSAGES = 10;

    private static final String TOPIC_NAME = "round_trip_topic";

    private static final Logger log = LoggerFactory.getLogger(RoundTripWorker.class);

    private final ToReceiveTracker toReceiveTracker = new ToReceiveTracker();

    private final String id;

    private final RoundTripWorkloadSpec spec;

    private final AtomicBoolean running = new AtomicBoolean(false);

    private ExecutorService executor;

    private KafkaFutureImpl<String> doneFuture;

    private KafkaProducer<byte[], byte[]> producer;

    private PayloadGenerator payloadGenerator;

    private KafkaConsumer<byte[], byte[]> consumer;

    private CountDownLatch unackedSends;

    public RoundTripWorker(String id, RoundTripWorkloadSpec spec) {
        this.id = id;
        this.spec = spec;
    }

    @Override
    public void start(Platform platform, AtomicReference<String> status,
                      KafkaFutureImpl<String> doneFuture) throws Exception {
        if (!running.compareAndSet(false, true)) {
            throw new IllegalStateException("RoundTripWorker is already running.");
        }
        log.info("{}: Activating RoundTripWorker.", id);
        this.executor = Executors.newCachedThreadPool(
            ThreadUtils.createThreadFactory("RoundTripWorker%d", false));
        this.doneFuture = doneFuture;
        this.producer = null;
        this.consumer = null;
        this.unackedSends = new CountDownLatch(spec.maxMessages());
        executor.submit(new Prepare());
    }

    class Prepare implements Runnable {
        @Override
        public void run() {
            try {
                if (spec.targetMessagesPerSec() <= 0) {
                    throw new ConfigException("Can't have targetMessagesPerSec <= 0.");
                }
                if ((spec.partitionAssignments() == null) || spec.partitionAssignments().isEmpty()) {
                    throw new ConfigException("Invalid null or empty partitionAssignments.");
                }
                WorkerUtils.createTopics(log, spec.bootstrapServers(),
                    Collections.singletonList(new NewTopic(TOPIC_NAME, spec.partitionAssignments())));
                executor.submit(new ProducerRunnable());
                executor.submit(new ConsumerRunnable());
            } catch (Throwable e) {
                WorkerUtils.abort(log, "Prepare", e, doneFuture);
            }
        }
    }

    private static class ToSendTrackerResult {
        final int index;
        final boolean firstSend;

        ToSendTrackerResult(int index, boolean firstSend) {
            this.index = index;
            this.firstSend = firstSend;
        }
    }

    private static class ToSendTracker {
        private final int maxMessages;
        private final List<Integer> failed = new ArrayList<>();
        private int frontier = 0;

        ToSendTracker(int maxMessages) {
            this.maxMessages = maxMessages;
        }

        synchronized void addFailed(int index) {
            failed.add(index);
        }

        synchronized ToSendTrackerResult next() {
            if (failed.isEmpty()) {
                if (frontier >= maxMessages) {
                    return null;
                } else {
                    return new ToSendTrackerResult(frontier++, true);
                }
            } else {
                return new ToSendTrackerResult(failed.remove(0), false);
            }
        }
    }

    class ProducerRunnable implements Runnable {
        private final Throttle throttle;

        ProducerRunnable() {
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, spec.bootstrapServers());
            props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16 * 1024);
            props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 4 * 16 * 1024L);
            props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 1000L);
            props.put(ProducerConfig.CLIENT_ID_CONFIG, "producer." + id);
            props.put(ProducerConfig.ACKS_CONFIG, "all");
            props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 105000);
            producer = new KafkaProducer<>(props, new ByteArraySerializer(),
                new ByteArraySerializer());
            int perPeriod = WorkerUtils.
                perSecToPerPeriod(spec.targetMessagesPerSec(), THROTTLE_PERIOD_MS);
            this.throttle = new Throttle(perPeriod, THROTTLE_PERIOD_MS);
            payloadGenerator = new PayloadGenerator(MESSAGE_SIZE, PayloadKeyType.KEY_MESSAGE_INDEX);
        }

        @Override
        public void run() {
            final ToSendTracker toSendTracker = new ToSendTracker(spec.maxMessages());
            long messagesSent = 0;
            long uniqueMessagesSent = 0;
            log.debug("{}: Starting RoundTripWorker#ProducerRunnable.", id);
            try {
                while (true) {
                    final ToSendTrackerResult result = toSendTracker.next();
                    if (result == null) {
                        break;
                    }
                    throttle.increment();
                    final int messageIndex = result.index;
                    if (result.firstSend) {
                        toReceiveTracker.addPending(messageIndex);
                        uniqueMessagesSent++;
                    }
                    messagesSent++;
                    // we explicitly specify generator position based on message index
                    ProducerRecord<byte[], byte[]> record = payloadGenerator.nextRecord(TOPIC_NAME, messageIndex);
                    producer.send(record, new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata metadata, Exception exception) {
                            if (exception == null) {
                                unackedSends.countDown();
                            } else {
                                log.info("{}: Got exception when sending message {}: {}",
                                    id, messageIndex, exception.getMessage());
                                toSendTracker.addFailed(messageIndex);
                            }
                        }
                    });
                }
            } catch (Throwable e) {
                WorkerUtils.abort(log, "ProducerRunnable", e, doneFuture);
            } finally {
                log.info("{}: ProducerRunnable is exiting.  messagesSent={}; uniqueMessagesSent={}; " +
                        "ackedSends={}.", id, messagesSent, uniqueMessagesSent,
                        spec.maxMessages() - unackedSends.getCount());
            }
        }
    }

    private class ToReceiveTracker {
        private final TreeSet<Integer> pending = new TreeSet<>();

        synchronized void addPending(int messageIndex) {
            pending.add(messageIndex);
        }

        synchronized boolean removePending(int messageIndex) {
            return pending.remove(messageIndex);
        }

        void log() {
            int numToReceive;
            List<Integer> list = new ArrayList<>(LOG_NUM_MESSAGES);
            synchronized (this) {
                numToReceive = pending.size();
                for (Iterator<Integer> iter = pending.iterator();
                        iter.hasNext() && (list.size() < LOG_NUM_MESSAGES); ) {
                    Integer i = iter.next();
                    list.add(i);
                }
            }
            log.info("{}: consumer waiting for {} message(s), starting with: {}",
                id, numToReceive, Utils.join(list, ", "));
        }
    }

    class ConsumerRunnable implements Runnable {
        private final Properties props;

        ConsumerRunnable() {
            this.props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, spec.bootstrapServers());
            props.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer." + id);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "round-trip-consumer-group-1");
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 105000);
            props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 100000);
            consumer = new KafkaConsumer<>(props, new ByteArrayDeserializer(),
                new ByteArrayDeserializer());
            consumer.subscribe(Collections.singleton(TOPIC_NAME));
        }

        @Override
        public void run() {
            long uniqueMessagesReceived = 0;
            long messagesReceived = 0;
            long pollInvoked = 0;
            log.debug("{}: Starting RoundTripWorker#ConsumerRunnable.", id);
            try {
                long lastLogTimeMs = Time.SYSTEM.milliseconds();
                while (true) {
                    try {
                        pollInvoked++;
                        ConsumerRecords<byte[], byte[]> records = consumer.poll(50);
                        for (ConsumerRecord<byte[], byte[]> record : records.records(TOPIC_NAME)) {
                            int messageIndex = ByteBuffer.wrap(record.key()).getInt();
                            messagesReceived++;
                            if (toReceiveTracker.removePending(messageIndex)) {
                                uniqueMessagesReceived++;
                                if (uniqueMessagesReceived >= spec.maxMessages()) {
                                    log.info("{}: Consumer received the full count of {} unique messages.  " +
                                        "Waiting for all sends to be acked...", id, spec.maxMessages());
                                    unackedSends.await();
                                    log.info("{}: all sends have been acked.", id);
                                    doneFuture.complete("");
                                    return;
                                }
                            }
                        }
                        long curTimeMs = Time.SYSTEM.milliseconds();
                        if (curTimeMs > lastLogTimeMs + LOG_INTERVAL_MS) {
                            toReceiveTracker.log();
                            lastLogTimeMs = curTimeMs;
                        }
                    } catch (WakeupException e) {
                        log.debug("{}: Consumer got WakeupException", id, e);
                    } catch (TimeoutException e) {
                        log.debug("{}: Consumer got TimeoutException", id, e);
                    }
                }
            } catch (Throwable e) {
                WorkerUtils.abort(log, "ConsumerRunnable", e, doneFuture);
            } finally {
                log.info("{}: ConsumerRunnable is exiting.  Invoked poll {} time(s).  " +
                    "messagesReceived = {}; uniqueMessagesReceived = {}.",
                    id, pollInvoked, messagesReceived, uniqueMessagesReceived);
            }
        }
    }

    @Override
    public void stop(Platform platform) throws Exception {
        if (!running.compareAndSet(true, false)) {
            throw new IllegalStateException("ProduceBenchWorker is not running.");
        }
        log.info("{}: Deactivating RoundTripWorkloadWorker.", id);
        doneFuture.complete("");
        executor.shutdownNow();
        executor.awaitTermination(1, TimeUnit.DAYS);
        Utils.closeQuietly(consumer, "consumer");
        Utils.closeQuietly(producer, "producer");
        this.consumer = null;
        this.producer = null;
        this.unackedSends = null;
        this.executor = null;
        this.doneFuture = null;
    }
}
