/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.server

import kafka.test.ClusterInstance
import kafka.test.annotation.{ClusterConfigProperty, ClusterTest, ClusterTestDefaults, Type}
import kafka.test.junit.ClusterTestExtensions
import org.apache.kafka.common.message.DescribeGroupsResponseData.{DescribedGroup, DescribedGroupMember}
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.coordinator.group.classic.ClassicGroupState
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{Tag, Timeout}
import org.junit.jupiter.api.extension.ExtendWith

import scala.jdk.CollectionConverters._

@Timeout(120)
@ExtendWith(value = Array(classOf[ClusterTestExtensions]))
@ClusterTestDefaults(clusterType = Type.KRAFT, brokers = 1)
@Tag("integration")
class DescribeGroupsRequestTest(cluster: ClusterInstance) extends GroupCoordinatorBaseRequestTest(cluster) {
  @ClusterTest(serverProperties = Array(
    new ClusterConfigProperty(key = "group.coordinator.new.enable", value = "true"),
    new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
    new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
  ))
  def testDescribeGroupsWithOldConsumerGroupProtocolAndNewGroupCoordinator(): Unit = {
    testDescribeGroups()
  }

  @ClusterTest(clusterType = Type.ALL, serverProperties = Array(
    new ClusterConfigProperty(key = "group.coordinator.new.enable", value = "false"),
    new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
    new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
  ))
  def testDescribeGroupsWithOldConsumerGroupProtocolAndOldGroupCoordinator(): Unit = {
    testDescribeGroups()
  }

  private def testDescribeGroups(): Unit = {
    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    createOffsetsTopic()

    // Create the topic.
    createTopic(
      topic = "foo",
      numPartitions = 3
    )

    // Join the consumer group. Complete the rebalance so that grp-1 is in STABLE state.
    val (memberId1, _) = joinDynamicConsumerGroupWithOldProtocol(
      groupId = "grp-1",
      metadata = Array(1, 2, 3),
      assignment = Array(4, 5, 6)
    )
    // Join the consumer group. Not complete the rebalance so that grp-2 is in COMPLETING_REBALANCE state.
    val (memberId2, _) = joinDynamicConsumerGroupWithOldProtocol(
      groupId = "grp-2",
      metadata = Array(1, 2, 3),
      completeRebalance = false
    )

    for (version <- ApiKeys.DESCRIBE_GROUPS.oldestVersion() to ApiKeys.DESCRIBE_GROUPS.latestVersion(isUnstableApiEnabled)) {
      assertEquals(
        List(
          new DescribedGroup()
            .setGroupId("grp-1")
            .setGroupState(ClassicGroupState.STABLE.toString)
            .setProtocolType("consumer")
            .setProtocolData("consumer-range")
            .setMembers(List(
              new DescribedGroupMember()
                .setMemberId(memberId1)
                .setGroupInstanceId(null)
                .setClientId("client-id")
                .setClientHost("/127.0.0.1")
                .setMemberMetadata(Array(1, 2, 3))
                .setMemberAssignment(Array(4, 5, 6))
            ).asJava),
          new DescribedGroup()
            .setGroupId("grp-2")
            .setGroupState(ClassicGroupState.COMPLETING_REBALANCE.toString)
            .setProtocolType("consumer")
            .setMembers(List(
              new DescribedGroupMember()
                .setMemberId(memberId2)
                .setGroupInstanceId(null)
                .setClientId("client-id")
                .setClientHost("/127.0.0.1")
                .setMemberMetadata(Array.empty)
                .setMemberAssignment(Array.empty)
            ).asJava),
          new DescribedGroup()
            .setGroupId("grp-unknown")
            .setGroupState(ClassicGroupState.DEAD.toString) // Return DEAD group when the group does not exist.
        ),
        describeGroups(
          groupIds = List("grp-1", "grp-2", "grp-unknown"),
          version = version.toShort
        )
      )
    }
  }
}
