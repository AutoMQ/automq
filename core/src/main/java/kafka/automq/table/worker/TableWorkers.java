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

package kafka.automq.table.worker;

import kafka.automq.table.Channel;
import kafka.automq.table.events.CommitRequest;
import kafka.automq.table.events.Envelope;
import kafka.automq.table.transformer.ConverterFactory;
import kafka.automq.table.utils.TableIdentifierUtil;
import kafka.cluster.Partition;
import kafka.server.KafkaConfig;

import com.automq.stream.utils.Systems;
import com.automq.stream.utils.ThreadUtils;
import com.automq.stream.utils.Threads;
import com.automq.stream.utils.threads.EventLoop;

import org.apache.iceberg.catalog.Catalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;

public class TableWorkers {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableWorkers.class);
    public static final ScheduledExecutorService SCHEDULER = Threads.newSingleThreadScheduledExecutor("table-workers", true, LOGGER);
    private final Catalog catalog;
    private final Channel channel;
    private final Channel.SubChannel subChannel;
    private final EventLoopWorker[] workers;
    private final EventLoops eventLoops;
    private final ExecutorService executor = Threads.newFixedThreadPool(1, ThreadUtils.createThreadFactory("table-worker-poll", true), LOGGER);
    private final ExecutorService flushExecutor = Threads.newFixedThreadPool(Systems.CPU_CORES, ThreadUtils.createThreadFactory("table-workers-flush", true), LOGGER);
    private final KafkaConfig config;
    private final ConverterFactory converterFactory;
    private final Semaphore commitLimiter = new Semaphore(Systems.CPU_CORES);
    private volatile boolean closed = false;


    public TableWorkers(Catalog catalog, Channel channel, KafkaConfig config) {
        this.catalog = catalog;
        this.channel = channel;
        this.subChannel = channel.subscribeControl();
        workers = new EventLoopWorker[Math.max(Systems.CPU_CORES / 2, 1)];
        EventLoop[] eventLoops = new EventLoop[workers.length];
        for (int i = 0; i < workers.length; i++) {
            workers[i] = new EventLoopWorker(i);
            eventLoops[i] = workers[i].eventLoop;
        }
        this.eventLoops = new EventLoops(eventLoops);
        executor.submit(new ControlListener());
        this.config = config;
        this.converterFactory = new ConverterFactory(config.tableTopicSchemaRegistryUrl());
    }

    public void add(Partition partition) {
        workers[Math.abs(partition.topic().hashCode() % workers.length)].add(partition);
    }

    public void remove(Partition partition) {
        workers[Math.abs(partition.topic().hashCode() % workers.length)].remove(partition);
    }

    public synchronized void close() {
        closed = true;
        this.subChannel.close();
    }

    class EventLoopWorker {
        private final EventLoop eventLoop;
        private final Map<String, TopicPartitionsWorker> topic2worker = new ConcurrentHashMap<>();

        public EventLoopWorker(int index) {
            eventLoop = new EventLoop("table-worker-" + index);
        }

        public void add(Partition partition) {
            eventLoop.execute(() -> {
                topic2worker.compute(partition.topic(), (topic, worker) -> {
                    if (worker == null) {
                        WorkerConfig config = new WorkerConfig(partition);
                        IcebergWriterFactory writerFactory = new IcebergWriterFactory(catalog,
                            TableIdentifierUtil.of(config.namespace(), partition.topic()), converterFactory, config, partition.topic());
                        worker = new TopicPartitionsWorker(partition.topic(), config,
                            writerFactory, channel, eventLoop, eventLoops, flushExecutor, commitLimiter);
                    }
                    worker.add(partition);
                    return worker;
                });
            });
        }

        public void remove(Partition partition) {
            eventLoop.execute(() -> {
                TopicPartitionsWorker topicPartitionsWorker = topic2worker.get(partition.topic());
                if (topicPartitionsWorker == null) {
                    return;
                }
                topicPartitionsWorker.remove(partition);
                if (topicPartitionsWorker.isEmpty()) {
                    topic2worker.remove(partition.topic());
                }
            });
        }

        public void commit(CommitRequest commitRequest) {
            if (topic2worker.containsKey(commitRequest.topic())) {
                eventLoop.execute(() -> {
                    TopicPartitionsWorker topicPartitionsWorker = topic2worker.get(commitRequest.topic());
                    if (topicPartitionsWorker != null) {
                        topicPartitionsWorker.commit(commitRequest);
                    }
                });
            }
        }
    }

    class ControlListener implements Runnable {

        @Override
        public void run() {
            for (; ; ) {
                Envelope envelope;
                synchronized (TableWorkers.this) {
                    if (closed) {
                        return;
                    }
                    envelope = subChannel.poll();
                }
                if (envelope == null) {
                    Threads.sleep(10);
                    continue;
                }
                CommitRequest commitRequest = envelope.event().payload();
                for (EventLoopWorker worker : workers) {
                    worker.commit(commitRequest);
                }
            }
        }
    }

}
