/*
 * Copyright 2025, AutoMQ HK Limited.
 *
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

package org.apache.kafka.tools.automq.perf;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.tools.automq.perf.TopicService.Topic;

import com.automq.stream.utils.Threads;
import com.google.common.primitives.Longs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Properties;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.tools.automq.perf.UniformRateLimiter.uninterruptibleSleepNs;

public class ProducerService implements AutoCloseable {

    public static final Charset HEADER_KEY_CHARSET = StandardCharsets.UTF_8;
    public static final String HEADER_KEY_SEND_TIME_NANOS = "st";
    public static final double MIN_RATE = 1.0;
    private static final int ADJUST_RATE_INTERVAL_SECONDS = 5;
    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerService.class);

    private final List<Producer> producers = new LinkedList<>();
    private final ScheduledExecutorService adjustRateExecutor =
        Threads.newSingleThreadScheduledExecutor("perf-producer-rate-adjust", true, LOGGER);
    private final ExecutorService executor = Executors.newCachedThreadPool(ThreadUtils.createThreadFactory("perf-producer", false));

    /**
     * A map of rate changes over time. The rate at the given time will be calculated by linear interpolation between
     * the two nearest known rates.
     * If there is no known rate at the given time, the {@link #defaultRate} will be used.
     */
    private final NavigableMap<Long, Double> rateMap = new TreeMap<>();
    private double defaultRate = MIN_RATE;
    private volatile UniformRateLimiter rateLimiter = new UniformRateLimiter(defaultRate);

    private volatile boolean closed = false;
    private final AtomicReference<Throwable> replayFailure = new AtomicReference<>();
    private final AtomicLong replayStretchedNanos = new AtomicLong();

    /**
     * Create producers for the given topics.
     * NOT thread-safe.
     *
     * @param topics   topic names
     * @param config   producer configuration
     * @param callback callback to be called when a message is sent
     * @return number of producers created
     */
    public int createProducers(List<Topic> topics, ProducersConfig config, ProducerCallback callback) {
        int count = 0;
        for (Topic topic : topics) {
            for (int i = 0; i < config.producersPerTopic; i++) {
                Producer producer = createProducer(topic, config, callback);
                producers.add(producer);
                count++;
            }
        }
        return count;
    }

    private Producer createProducer(Topic topic, ProducersConfig config, ProducerCallback callback) {
        KafkaProducer<Object, byte[]> kafkaProducer = new KafkaProducer<>(producerProperties(config));
        return new Producer(kafkaProducer, topic, callback, config.sendTimeHeader, config.sampleReplay);
    }

    static Properties producerProperties(ProducersConfig config) {
        Properties properties = new Properties();
        properties.putAll(config.properties);
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServer);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            config.sampleReplay ? ByteArraySerializer.class.getName() : StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.putIfAbsent(ProducerConfig.PARTITIONER_IGNORE_KEYS_CONFIG, !config.sampleReplay);
        return properties;
    }

    /**
     * Send a message to each partition of each topic to ensure that the topic is created.
     * It throws an exception if any of the producers fail to send the message.
     * NOT thread-safe.
     *
     * @return number of messages sent
     */
    public int probe() {
        List<CompletableFuture<Void>> futures = producers.stream()
            .map(Producer::probe)
            .flatMap(List::stream)
            .collect(Collectors.toList());
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        return futures.size();
    }

    /**
     * Start sending messages using the given payloads at the given rate.
     */
    public void start(Function<String, List<byte[]>> payloads, double rate) {
        adjustRate(rate);
        adjustRateExecutor.scheduleWithFixedDelay(this::adjustRate, 0, ADJUST_RATE_INTERVAL_SECONDS, TimeUnit.SECONDS);
        int processors = Runtime.getRuntime().availableProcessors();
        // shard producers across processors
        int batchSize = Math.max(1, producers.size() / processors);
        for (int i = 0; i < producers.size(); i += batchSize) {
            int from = i;
            int to = Math.min(i + batchSize, producers.size());
            executor.submit(() -> start(producers.subList(from, to), payloads));
        }
    }

    /**
     * Replay validated sample assignments with one dispatch thread per producer.
     * A zero cycle uses the existing aggregate rate limiter and warmup ramp. A positive
     * cycle replays supplied per-producer offsets independently, without a rate ramp.
     * Each topic receives the complete corpus. The caller retains ownership of the shared arrays.
     */
    public void startSample(List<List<SampleRecords.Entry>> assignments, double rate, long cycleNanos, double speed) {
        if (cycleNanos == 0) {
            adjustRate(rate);
            adjustRateExecutor.scheduleWithFixedDelay(this::adjustRate, 0, ADJUST_RATE_INTERVAL_SECONDS, TimeUnit.SECONDS);
        }
        long start = System.nanoTime();
        for (int i = 0; i < producers.size(); i++) {
            Producer producer = producers.get(i);
            List<SampleRecords.Entry> entries = assignments.get(i % assignments.size());
            executor.submit(() -> {
                try {
                    replay(producer, entries, cycleNanos, speed, start);
                } catch (Throwable e) {
                    replayFailure.compareAndSet(null, e);
                }
            });
        }
    }

    private void replay(Producer producer, List<SampleRecords.Entry> entries, long cycleNanos, double speed, long start) {
        ReplaySchedule schedule = cycleNanos == 0 ? null : new ReplaySchedule(entries, cycleNanos, speed, start);
        int index = 0;
        long stretchedNanos = 0;
        try {
            while (!closed && !Thread.currentThread().isInterrupted()) {
                long deadline = schedule == null ? rateLimiter.acquire() : schedule.deadline();
                if (!awaitReplay(deadline)) {
                    return;
                }
                SampleRecords.Entry entry = schedule == null ? entries.get(index) : schedule.current();
                long dispatchedAt = System.nanoTime();
                producer.sendAsync(entry.key, entry.value, null);
                if (schedule != null) {
                    stretchedNanos += schedule.advance(dispatchedAt);
                }
                if (++index == entries.size()) {
                    index = 0;
                }
            }
        } finally {
            replayStretchedNanos.addAndGet(stretchedNanos);
        }
    }

    private boolean awaitReplay(long deadline) {
        while (!closed && !Thread.currentThread().isInterrupted()) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                return true;
            }
            LockSupport.parkNanos(Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(100)));
        }
        return false;
    }

    /** Propagate background replay failures to the controlling command rather than reporting a silent idle test. */
    public void checkReplayFailure() {
        Throwable failure = replayFailure.get();
        if (failure != null) {
            throw new IllegalStateException("Sample replay failed", failure);
        }
    }

    /**
     * Adjust the rate at the given time.
     */
    public void adjustRate(long timeNanos, double rate) {
        rateMap.put(timeNanos, rate);
        adjustRate();
    }

    /**
     * Adjust the default rate.
     */
    public void adjustRate(double defaultRate) {
        this.defaultRate = defaultRate;
        adjustRate();
    }

    private void adjustRate() {
        double rate = calculateRate(System.nanoTime());
        this.rateLimiter = new UniformRateLimiter(rate);
    }

    private double calculateRate(long now) {
        Map.Entry<Long, Double> floorEntry = rateMap.floorEntry(now);
        Map.Entry<Long, Double> ceilingEntry = rateMap.ceilingEntry(now);
        if (null == floorEntry || null == ceilingEntry) {
            return defaultRate;
        }

        long floorTime = floorEntry.getKey();
        double floorRate = floorEntry.getValue();
        long ceilingTime = ceilingEntry.getKey();
        double ceilingRate = ceilingEntry.getValue();

        return calculateY(floorTime, floorRate, ceilingTime, ceilingRate, now);
    }

    private double calculateY(long x1, double y1, long x2, double y2, long x) {
        if (x1 == x2) {
            return y1;
        }
        return y1 + (x - x1) * (y2 - y1) / (x2 - x1);
    }

    private void start(List<Producer> producers, Function<String, List<byte[]>> payloads) {
        executor.submit(() -> {
            try {
                sendMessages(producers, payloads);
            } catch (Exception e) {
                LOGGER.error("Failed to send messages", e);
            }
        });
    }

    private void sendMessages(List<Producer> producers, Function<String, List<byte[]>> payloadsGet) {
        Random random = ThreadLocalRandom.current();
        while (!closed) {
            producers.forEach(
                p -> {
                    List<byte[]> payloads = payloadsGet.apply(p.topic.name);
                    sendMessage(p, payloads.get(random.nextInt(payloads.size())));
                }
            );
        }
    }

    private void sendMessage(Producer producer, byte[] payload) {
        long intendedSendTime = rateLimiter.acquire();
        uninterruptibleSleepNs(intendedSendTime);
        producer.sendAsync(payload).exceptionally(e -> {
            LOGGER.warn("Failed to send message", e);
            return null;
        });
    }

    @Override
    public void close() {
        closed = true;
        adjustRateExecutor.shutdownNow();
        executor.shutdown();
        try {
            if (!executor.awaitTermination(2, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
        }
        producers.forEach(Producer::close);
        if (replayStretchedNanos.get() > 0) {
            LOGGER.info("Timed replay schedule stretch summed across producers: {} ms", replayStretchedNanos.get() / 1_000_000.0);
        }
    }

    @FunctionalInterface
    public interface ProducerCallback {
        /**
         * Called when a message is sent.
         *
         * @param size          the size of the sent message
         * @param sendTimeNanos the time in nanoseconds when the message was sent
         * @param exception     the exception if the message failed to send, or null
         */
        void messageSent(int size, long sendTimeNanos, Exception exception);
    }

    public static class ProducersConfig {
        final String bootstrapServer;
        final int producersPerTopic;
        final Properties properties;
        final boolean sampleReplay;
        final boolean sendTimeHeader;

        public ProducersConfig(String bootstrapServer, int producersPerTopic, Properties properties) {
            this(bootstrapServer, producersPerTopic, properties, false, true);
        }

        /** Configure sampled binary keys and optional benchmark headers without changing synthetic-mode defaults. */
        public ProducersConfig(String bootstrapServer, int producersPerTopic, Properties properties, boolean sampleReplay, boolean sendTimeHeader) {
            this.bootstrapServer = bootstrapServer;
            this.producersPerTopic = producersPerTopic;
            this.properties = properties;
            this.sampleReplay = sampleReplay;
            this.sendTimeHeader = sendTimeHeader;
        }
    }

    static class Producer implements AutoCloseable {

        private static final String[] PRESET_KEYS = new String[16384];

        static {
            byte[] buffer = new byte[7];
            Random random = ThreadLocalRandom.current();
            for (int i = 0; i < PRESET_KEYS.length; i++) {
                random.nextBytes(buffer);
                PRESET_KEYS[i] = Base64.getUrlEncoder().withoutPadding().encodeToString(buffer);
            }
        }

        private final Random random = ThreadLocalRandom.current();
        private final org.apache.kafka.clients.producer.Producer<Object, byte[]> producer;
        private final boolean sendTimeHeader;
        private final boolean binaryKeys;
        private final Topic topic;
        private final ProducerCallback callback;

        Producer(org.apache.kafka.clients.producer.Producer<Object, byte[]> producer, Topic topic, ProducerCallback callback, boolean sendTimeHeader, boolean binaryKeys) {
            this.producer = producer;
            this.sendTimeHeader = sendTimeHeader;
            this.binaryKeys = binaryKeys;
            this.topic = topic;
            this.callback = callback;
        }

        /**
         * Send the payload to a random partition with a random key.
         * NOT thread-safe.
         */
        public CompletableFuture<Void> sendAsync(byte[] payload) {
            return sendAsync(nextKey(), payload, null);
        }

        private String nextKey() {
            return PRESET_KEYS[random.nextInt(PRESET_KEYS.length)];
        }

        /**
         * Send a message to each partition.
         */
        public List<CompletableFuture<Void>> probe() {
            return IntStream.range(0, topic.partitions)
                .mapToObj(i -> sendAsync(binaryKeys ? "probe".getBytes(StandardCharsets.UTF_8) : "probe", new byte[] {1}, i))
                .collect(Collectors.toList());
        }

        /**
         * Send a message to the topic.
         *
         * @param key       the key of the message, optional
         * @param payload   the payload of the message
         * @param partition the partition to send the message to, optional
         * @return a future that completes when the message is sent
         */
        CompletableFuture<Void> sendAsync(Object key, byte[] payload, Integer partition) {
            long sendTimeNanos = StatsCollector.currentNanos();
            List<Header> headers = sendTimeHeader ? List.of(
                new RecordHeader(HEADER_KEY_SEND_TIME_NANOS, Longs.toByteArray(sendTimeNanos))
            ) : Collections.emptyList();
            ProducerRecord<Object, byte[]> record = new ProducerRecord<>(topic.name, partition, key, payload, headers);
            int size = payload == null ? 0 : payload.length;
            CompletableFuture<Void> future = new CompletableFuture<>();
            try {
                producer.send(record, (metadata, exception) -> {
                    callback.messageSent(size, sendTimeNanos, exception);
                    if (exception != null) {
                        future.completeExceptionally(exception);
                    } else {
                        future.complete(null);
                    }
                });
            } catch (InterruptException e) {
                // ignore, as we are closing
                future.completeExceptionally(e);
            } catch (Exception e) {
                callback.messageSent(size, sendTimeNanos, e);
                future.completeExceptionally(e);
            }
            return future;
        }

        @Override
        public void close() {
            producer.close();
        }
    }
}
