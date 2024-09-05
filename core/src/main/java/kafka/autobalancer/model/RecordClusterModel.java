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

package kafka.autobalancer.model;

import com.automq.stream.utils.LogContext;
import java.util.Optional;
import kafka.autobalancer.common.Utils;
import kafka.autobalancer.listeners.BrokerStatusListener;
import kafka.autobalancer.listeners.TopicPartitionStatusListener;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.BrokerRegistrationChangeRecord;
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.RemoveTopicRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.metadata.UnregisterBrokerRecord;
import org.apache.kafka.metadata.LeaderConstants;

public class RecordClusterModel extends ClusterModel implements BrokerStatusListener, TopicPartitionStatusListener {

    public RecordClusterModel() {
        super();
    }

    public RecordClusterModel(LogContext logContext) {
        super(logContext);
    }

    @Override
    public void onBrokerRegister(RegisterBrokerRecord record) {
        registerBroker(record.brokerId(), record.rack(), !Utils.isBrokerFenced(record));
    }

    @Override
    public void onBrokerUnregister(UnregisterBrokerRecord record) {
        unregisterBroker(record.brokerId());
    }

    @Override
    public void onBrokerRegistrationChanged(BrokerRegistrationChangeRecord record) {
        Optional<Boolean> isBrokerFenced = Utils.isBrokerFenced(record);
        isBrokerFenced.ifPresent(isFenced -> changeBrokerStatus(record.brokerId(), !isFenced));
    }

    @Override
    public void onTopicCreate(TopicRecord record) {
        createTopic(record.topicId(), record.name());
    }

    @Override
    public void onTopicDelete(RemoveTopicRecord record) {
        deleteTopic(record.topicId());
    }

    @Override
    public void onPartitionCreate(PartitionRecord record) {
        changePartition(record.topicId(), record.partitionId(), record.leader());
    }

    @Override
    public void onPartitionChange(PartitionChangeRecord record) {
        changePartition(record.topicId(), record.partitionId(), record.leader());
    }

    private void changePartition(Uuid topicId, int partitionId, int leader) {
        if (leader == LeaderConstants.NO_LEADER) {
            deletePartition(topicId, partitionId);
            return;
        } else if (leader == LeaderConstants.NO_LEADER_CHANGE) {
            return;
        } else if (leader < 0) {
            throw new IllegalStateException("Unexpected leader value: " + leader);
        }
        reassignPartition(topicId, partitionId, leader);
    }
}
