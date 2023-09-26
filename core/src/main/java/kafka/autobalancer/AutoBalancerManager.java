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

package kafka.autobalancer;

import kafka.autobalancer.config.AutoBalancerControllerConfig;
import kafka.autobalancer.listeners.BrokerStatusListener;
import kafka.autobalancer.listeners.ClusterStatusListenerRegistry;
import kafka.autobalancer.listeners.TopicPartitionStatusListener;
import kafka.autobalancer.model.ClusterModel;
import kafka.server.KafkaConfig;
import org.apache.kafka.common.metadata.BrokerRegistrationChangeRecord;
import org.apache.kafka.common.metadata.MetadataRecordType;
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.RemoveTopicRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.metadata.UnregisterBrokerRecord;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.controller.QuorumController;
import org.apache.kafka.queue.KafkaEventQueue;
import org.apache.kafka.raft.Batch;
import org.apache.kafka.raft.BatchReader;
import org.apache.kafka.raft.KafkaRaftClient;
import org.apache.kafka.raft.LeaderAndEpoch;
import org.apache.kafka.raft.RaftClient;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.snapshot.SnapshotReader;
import org.slf4j.Logger;

import java.util.List;

public class AutoBalancerManager {
    private final Logger logger;
    private final LoadRetriever loadRetriever;
    private final AnomalyDetector anomalyDetector;
    private final KafkaEventQueue queue;
    private final QuorumController quorumController;

    public AutoBalancerManager(Time time, KafkaConfig kafkaConfig, QuorumController quorumController, KafkaRaftClient<ApiMessageAndVersion> raftClient) {
        LogContext logContext = new LogContext(String.format("[AutoBalancerManager id=%d] ", quorumController.nodeId()));
        logger = logContext.logger(AutoBalancerManager.class);
        AutoBalancerControllerConfig config = new AutoBalancerControllerConfig(kafkaConfig.props(), false);
        ClusterModel clusterModel = new ClusterModel(config, new LogContext(String.format("[ClusterModel id=%d] ", quorumController.nodeId())));
        this.loadRetriever = new LoadRetriever(config, quorumController, clusterModel,
                new LogContext(String.format("[LoadRetriever id=%d] ", quorumController.nodeId())));
        ExecutionManager executionManager = new ExecutionManager(config, quorumController,
                new LogContext(String.format("[ExecutionManager id=%d] ", quorumController.nodeId())));
        this.anomalyDetector = new AnomalyDetector(config, clusterModel, executionManager,
                new LogContext(String.format("[AnomalyDetector id=%d] ", quorumController.nodeId())));
        this.queue = new KafkaEventQueue(time, new LogContext(), "auto-balancer-");
        this.quorumController = quorumController;
        ClusterStatusListenerRegistry registry = new ClusterStatusListenerRegistry();
        registry.register((BrokerStatusListener) clusterModel);
        registry.register((TopicPartitionStatusListener) clusterModel);
        registry.register(executionManager);
        registry.register(this.loadRetriever);
        raftClient.register(new AutoBalancerListener(registry, this.loadRetriever, this.anomalyDetector));
    }

    public void start() {
        loadRetriever.start();
        anomalyDetector.start();
        logger.info("Started");
    }

    public void shutdown() throws InterruptedException {
        anomalyDetector.shutdown();
        loadRetriever.shutdown();
        queue.close();
        logger.info("Shutdown completed");
    }

    class AutoBalancerListener implements RaftClient.Listener<ApiMessageAndVersion> {
        private final ClusterStatusListenerRegistry registry;
        private final LoadRetriever loadRetriever;
        private final AnomalyDetector anomalyDetector;

        public AutoBalancerListener(ClusterStatusListenerRegistry registry, LoadRetriever loadRetriever, AnomalyDetector anomalyDetector) {
            this.registry = registry;
            this.loadRetriever = loadRetriever;
            this.anomalyDetector = anomalyDetector;
        }

        private void handleMessage(ApiMessage message) {
            MetadataRecordType type = MetadataRecordType.fromId(message.apiKey());
            switch (type) {
                case REGISTER_BROKER_RECORD:
                    for (BrokerStatusListener listener : this.registry.brokerListeners()) {
                        listener.onBrokerRegister((RegisterBrokerRecord) message);
                    }
                    break;
                case UNREGISTER_BROKER_RECORD:
                    for (BrokerStatusListener listener : this.registry.brokerListeners()) {
                        listener.onBrokerUnregister((UnregisterBrokerRecord) message);
                    }
                    break;
                case BROKER_REGISTRATION_CHANGE_RECORD:
                    for (BrokerStatusListener listener : this.registry.brokerListeners()) {
                        listener.onBrokerRegistrationChanged((BrokerRegistrationChangeRecord) message);
                    }
                    break;
                case TOPIC_RECORD:
                    for (TopicPartitionStatusListener listener : this.registry.topicPartitionListeners()) {
                        listener.onTopicCreate((TopicRecord) message);
                    }
                    break;
                case REMOVE_TOPIC_RECORD:
                    for (TopicPartitionStatusListener listener : this.registry.topicPartitionListeners()) {
                        listener.onTopicDelete((RemoveTopicRecord) message);
                    }
                    break;
                case PARTITION_RECORD:
                    for (TopicPartitionStatusListener listener : this.registry.topicPartitionListeners()) {
                        listener.onPartitionCreate((PartitionRecord) message);
                    }
                    break;
                case PARTITION_CHANGE_RECORD:
                    for (TopicPartitionStatusListener listener : this.registry.topicPartitionListeners()) {
                        listener.onPartitionChange((PartitionChangeRecord) message);
                    }
                    break;
                default:
                    break;
            }
        }

        @Override
        public void handleCommit(BatchReader<ApiMessageAndVersion> reader) {
            queue.append(() -> {
                try (reader) {
                    while (reader.hasNext()) {
                        Batch<ApiMessageAndVersion> batch = reader.next();
                        List<ApiMessageAndVersion> messages = batch.records();
                        for (ApiMessageAndVersion apiMessage : messages) {
                            handleMessage(apiMessage.message());
                        }
                    }
                }
            });
        }

        @Override
        public void handleSnapshot(SnapshotReader<ApiMessageAndVersion> reader) {
            queue.append(() -> {
                try (reader) {
                    while (reader.hasNext()) {
                        Batch<ApiMessageAndVersion> batch = reader.next();
                        List<ApiMessageAndVersion> messages = batch.records();
                        for (ApiMessageAndVersion apiMessage : messages) {
                            handleMessage(apiMessage.message());
                        }
                    }
                }
            });
        }

        @Override
        public void handleLeaderChange(LeaderAndEpoch leader) {
            queue.append(() -> {
                if (leader.leaderId().isEmpty()) {
                    return;
                }
                boolean isLeader = leader.isLeader(quorumController.nodeId());
                if (isLeader) {
                    this.anomalyDetector.resume();
                } else {
                    this.anomalyDetector.pause();
                }
                this.loadRetriever.onLeaderChanged(isLeader);
            });
        }

        @Override
        public void beginShutdown() {
            RaftClient.Listener.super.beginShutdown();
        }
    }

}
