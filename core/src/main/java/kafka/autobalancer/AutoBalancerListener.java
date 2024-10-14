/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.autobalancer;

import kafka.autobalancer.common.AutoBalancerConstants;
import kafka.autobalancer.listeners.BrokerStatusListener;
import kafka.autobalancer.listeners.ClusterStatusListenerRegistry;
import kafka.autobalancer.listeners.LeaderChangeListener;
import kafka.autobalancer.listeners.TopicPartitionStatusListener;

import org.apache.kafka.common.metadata.BrokerRegistrationChangeRecord;
import org.apache.kafka.common.metadata.MetadataRecordType;
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.RemoveTopicRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.metadata.UnregisterBrokerRecord;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.queue.KafkaEventQueue;
import org.apache.kafka.raft.Batch;
import org.apache.kafka.raft.BatchReader;
import org.apache.kafka.raft.LeaderAndEpoch;
import org.apache.kafka.raft.RaftClient;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.snapshot.SnapshotReader;

import com.automq.stream.utils.LogContext;

import org.slf4j.Logger;

import java.util.List;

public class AutoBalancerListener implements RaftClient.Listener<ApiMessageAndVersion> {
    private final int nodeId;
    private final Logger logger;
    private final KafkaEventQueue queue;
    private final ClusterStatusListenerRegistry registry;

    public AutoBalancerListener(int nodeId, Time time, ClusterStatusListenerRegistry registry) {
        this.nodeId = nodeId;
        this.logger = new LogContext(String.format("[AutoBalancerListener id=%d] ", nodeId)).logger(AutoBalancerConstants.AUTO_BALANCER_LOGGER_CLAZZ);
        this.queue = new KafkaEventQueue(time, new org.apache.kafka.common.utils.LogContext(), "auto-balancer-listener-");
        this.registry = registry;
    }

    private void handleMessageBatch(Batch<ApiMessageAndVersion> messageBatch) {
        List<ApiMessageAndVersion> messages = messageBatch.records();
        for (ApiMessageAndVersion apiMessage : messages) {
            try {
                handleMessage(apiMessage.message());
            } catch (Exception e) {
                logger.error("Failed to handle message", e);
            }
        }
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
                    handleMessageBatch(reader.next());
                }
            }
        });
    }

    @Override
    public void handleLoadSnapshot(SnapshotReader<ApiMessageAndVersion> reader) {
        queue.append(() -> {
            try (reader) {
                while (reader.hasNext()) {
                    handleMessageBatch(reader.next());
                }
            }
        });
    }

    @Override
    public void handleLeaderChange(LeaderAndEpoch leader) {
        queue.append(() -> {
            handleLeaderChange0(leader);
        });
    }

    void handleLeaderChange0(LeaderAndEpoch leader) {
        if (leader.leaderId().isEmpty()) {
            return;
        }
        boolean isLeader = leader.isLeader(nodeId);
        for (LeaderChangeListener listener : this.registry.leaderChangeListeners()) {
            listener.onLeaderChanged(isLeader);
        }
    }

    @Override
    public void beginShutdown() {
        this.queue.beginShutdown("AutoBalancerListenerShutdown");
    }
}
