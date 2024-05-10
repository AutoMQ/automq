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

package kafka.autobalancer.goals;

import kafka.autobalancer.common.types.Resource;
import kafka.autobalancer.model.BrokerUpdater;
import kafka.autobalancer.model.ClusterModelSnapshot;
import kafka.autobalancer.model.TopicPartitionReplicaUpdater;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class AbstractResourceGoalTest extends GoalTestBase {

    @Test
    public void testValidAction() {
        AbstractResourceGoal goal = Mockito.mock(AbstractResourceGoal.class);
        Mockito.doCallRealMethod().when(goal).validateAction(Mockito.anyInt(), Mockito.anyInt(), Mockito.any(), Mockito.any());
        ClusterModelSnapshot cluster = new ClusterModelSnapshot();
        BrokerUpdater.Broker srcBroker = createBroker(cluster, "", 0);
        BrokerUpdater.Broker destBroker = createBroker(cluster, "", 1);
        TopicPartitionReplicaUpdater.TopicPartitionReplica replica = createTopicPartition(cluster, 0, TOPIC_0, 0);
        TopicPartition tp = new TopicPartition(TOPIC_0, 0);

        // test valid action on trusted brokers
        srcBroker.setLoad(Resource.NW_IN, 100, true);
        destBroker.setLoad(Resource.NW_IN, 0, true);
        replica.setLoad(Resource.NW_IN, 100, true);
        Assertions.assertTrue(goal.validateAction(srcBroker.getBrokerId(), destBroker.getBrokerId(), tp, cluster));

        // test valid action on untrusted brokers
        srcBroker.setLoad(Resource.NW_IN, 100, false);
        destBroker.setLoad(Resource.NW_IN, 0, false);
        replica.setLoad(Resource.NW_IN, 0, true);
        Assertions.assertTrue(goal.validateAction(srcBroker.getBrokerId(), destBroker.getBrokerId(), tp, cluster));

        // test invalid action
        srcBroker.setLoad(Resource.NW_IN, 100, false);
        destBroker.setLoad(Resource.NW_IN, 0, false);
        replica.setLoad(Resource.NW_IN, 100, true);
        Assertions.assertFalse(goal.validateAction(srcBroker.getBrokerId(), destBroker.getBrokerId(), tp, cluster));

        srcBroker.setLoad(Resource.NW_IN, 100, true);
        destBroker.setLoad(Resource.NW_IN, 0, false);
        replica.setLoad(Resource.NW_IN, 100, true);
        Assertions.assertFalse(goal.validateAction(srcBroker.getBrokerId(), destBroker.getBrokerId(), tp, cluster));

        srcBroker.setLoad(Resource.NW_IN, 100, false);
        destBroker.setLoad(Resource.NW_IN, 0, true);
        replica.setLoad(Resource.NW_IN, 100, true);
        Assertions.assertFalse(goal.validateAction(srcBroker.getBrokerId(), destBroker.getBrokerId(), tp, cluster));

        srcBroker.setLoad(Resource.NW_IN, 100, false);
        destBroker.setLoad(Resource.NW_IN, 0, true);
        replica.setLoad(Resource.NW_IN, 100, false);
        Assertions.assertFalse(goal.validateAction(srcBroker.getBrokerId(), destBroker.getBrokerId(), tp, cluster));
    }
}
