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

package kafka.automq.table;

import kafka.automq.table.coordinator.TableCoordinator;
import kafka.automq.table.worker.TableWorkers;
import kafka.cluster.Partition;
import kafka.log.streamaspect.ElasticLog;
import kafka.log.streamaspect.ElasticUnifiedLog;
import kafka.log.streamaspect.MetaStream;
import kafka.server.KafkaConfig;
import kafka.server.MetadataCache;
import kafka.server.streamaspect.PartitionLifecycleListener;

import org.apache.kafka.common.TopicPartition;

import com.automq.stream.utils.Systems;
import com.automq.stream.utils.threads.EventLoop;

import org.apache.iceberg.catalog.Catalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TableManager implements PartitionLifecycleListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableManager.class);
    private final Catalog catalog;
    private final Channel channel;
    private final EventLoop[] coordinatorEventLoops;
    private final Map<TopicPartition, TableCoordinator> coordinators = new HashMap<>();
    private final Set<TopicPartition> tableTopicPartitions = new HashSet<>();
    private final TableWorkers tableWorkers;
    private final KafkaConfig config;

    private final MetadataCache metadataCache;

    public TableManager(MetadataCache metadataCache, KafkaConfig config) {
        this.metadataCache = metadataCache;
        this.config = config;
        this.catalog = new CatalogFactory().newCatalog(config);
        if (this.catalog == null) {
            channel = null;
            coordinatorEventLoops = null;
            tableWorkers = null;
            return;
        }
        this.channel = new Channel(config);
        this.tableWorkers = new TableWorkers(catalog, channel, config);
        this.coordinatorEventLoops = new EventLoop[Math.max(Systems.CPU_CORES / 2, 1)];
        for (int i = 0; i < coordinatorEventLoops.length; i++) {
            this.coordinatorEventLoops[i] = new EventLoop("table-coordinator-" + i);
        }
    }

    @Override
    public synchronized void onOpen(Partition partition) {
        if (catalog == null) {
            return;
        }
        ElasticUnifiedLog log = (ElasticUnifiedLog) partition.log().get();
        log.addConfigChangeListener((l, config) -> {
            synchronized (TableManager.this) {
                if (config.tableTopicEnable && !tableTopicPartitions.contains(partition.topicPartition())) {
                    add(partition);
                } else if (!config.tableTopicEnable && tableTopicPartitions.contains(partition.topicPartition())) {
                    remove(partition);
                }
            }
        });
        if (log.config().tableTopicEnable && !tableTopicPartitions.contains(partition.topicPartition())) {
            add(partition);
        }
    }

    private synchronized void add(Partition partition) {
        try {
            String topic = partition.topicPartition().topic();
            int partitionId = partition.topicPartition().partition();
            if (partitionId == 0) {
                // start coordinator
                EventLoop eventLoop = coordinatorEventLoops[Math.abs(topic.hashCode() % coordinatorEventLoops.length)];
                MetaStream metaStream = ((ElasticLog) (partition.log().get().localLog())).metaStream();
                //noinspection resource
                TableCoordinator coordinator = new TableCoordinator(catalog, topic, metaStream, channel, eventLoop, metadataCache, () -> partition.log().get().config());
                coordinators.put(partition.topicPartition(), coordinator);
                coordinator.start();
            }
            // start worker
            tableWorkers.add(partition);
            tableTopicPartitions.add(partition.topicPartition());
        } catch (Throwable e) {
            LOGGER.error("[TABLE_TOPIC_PARTITION_ADD],{}", partition.topicPartition(), e);
        }
    }

    @Override
    public void onClose(Partition partition) {
        remove(partition);
    }

    private synchronized void remove(Partition partition) {
        if (catalog == null) {
            return;
        }
        try {
            int partitionId = partition.topicPartition().partition();
            if (partitionId == 0) {
                TableCoordinator coordinator = coordinators.remove(partition.topicPartition());
                if (coordinator != null) {
                    coordinator.close();
                }
            }
            tableWorkers.remove(partition);
            tableTopicPartitions.remove(partition.topicPartition());
        } catch (Throwable e) {
            LOGGER.error("[TABLE_TOPIC_PARTITION_DELETE],{}", partition.topicPartition(), e);
        }
    }

    public void close() {
        if (catalog == null) {
            return;
        }
        try {
            tableWorkers.close();
        } catch (Throwable e) {
            LOGGER.error("[TABLE_MANAGER_CLOSE_FAIL]", e);
        }
    }
}
