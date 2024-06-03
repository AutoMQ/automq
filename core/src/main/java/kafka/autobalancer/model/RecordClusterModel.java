/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.autobalancer.model;

import com.automq.stream.utils.LogContext;
import kafka.autobalancer.listeners.BrokerStatusListener;
import kafka.autobalancer.listeners.TopicPartitionStatusListener;
import org.apache.kafka.common.metadata.BrokerRegistrationChangeRecord;
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.RemoveTopicRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.metadata.UnregisterBrokerRecord;
import org.apache.kafka.metadata.BrokerRegistrationFencingChange;
import org.apache.kafka.metadata.BrokerRegistrationInControlledShutdownChange;

public class RecordClusterModel extends ClusterModel implements BrokerStatusListener, TopicPartitionStatusListener {

    public RecordClusterModel() {
        super();
    }

    public RecordClusterModel(LogContext logContext) {
        super(logContext);
    }

    @Override
    public void onBrokerRegister(RegisterBrokerRecord record) {
        registerBroker(record.brokerId(), record.rack());
    }

    @Override
    public void onBrokerUnregister(UnregisterBrokerRecord record) {
        unregisterBroker(record.brokerId());
    }

    @Override
    public void onBrokerRegistrationChanged(BrokerRegistrationChangeRecord record) {
        BrokerRegistrationFencingChange fencingChange =
                BrokerRegistrationFencingChange.fromValue(record.fenced()).orElseThrow(
                        () -> new IllegalStateException(String.format("Unable to replay %s: unknown " +
                                "value for fenced field: %x", record, record.fenced())));
        BrokerRegistrationInControlledShutdownChange inControlledShutdownChange =
                BrokerRegistrationInControlledShutdownChange.fromValue(record.inControlledShutdown()).orElseThrow(
                        () -> new IllegalStateException(String.format("Unable to replay %s: unknown " +
                                "value for inControlledShutdown field: %x", record, record.inControlledShutdown())));
        if (fencingChange == BrokerRegistrationFencingChange.FENCE
                || inControlledShutdownChange == BrokerRegistrationInControlledShutdownChange.IN_CONTROLLED_SHUTDOWN) {
            changeBrokerStatus(record.brokerId(), false);
        } else if (fencingChange == BrokerRegistrationFencingChange.UNFENCE) {
            changeBrokerStatus(record.brokerId(), true);
        }
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
        if (record.leader() < 0) {
            logger.error("Illegal replica leader {} for {}-{}", record.leader(), record.topicId(), record.partitionId());
            return;
        }
        createPartition(record.topicId(), record.partitionId(), record.leader());
    }

    @Override
    public void onPartitionChange(PartitionChangeRecord record) {
        if (record.leader() < 0) {
            // simply ignore the record if the leader is illegal
            return;
        }
        reassignPartition(record.topicId(), record.partitionId(), record.leader());
    }
}
