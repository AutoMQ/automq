/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
package kafka.zk

import java.util.{Properties, UUID}
import java.nio.charset.StandardCharsets.UTF_8

import kafka.api.ApiVersion
import kafka.cluster.{Broker, EndPoint}
import kafka.log.LogConfig
import kafka.security.auth._
import kafka.server.ConfigType
import kafka.utils.CoreUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.zookeeper.KeeperException.NodeExistsException
import org.junit.Assert._
import org.junit.Test

class KafkaZkClientTest extends ZooKeeperTestHarness {

  private val group = "my-group"
  private val topicPartition = new TopicPartition("topic", 0)

  @Test
  def testSetAndGetConsumerOffset() {
    val offset = 123L
    // None if no committed offsets
    assertTrue(zkClient.getConsumerOffset(group, topicPartition).isEmpty)
    // Set and retrieve an offset
    zkClient.setOrCreateConsumerOffset(group, topicPartition, offset)
    assertEquals(offset, zkClient.getConsumerOffset(group, topicPartition).get)
    // Update an existing offset and retrieve it
    zkClient.setOrCreateConsumerOffset(group, topicPartition, offset + 2L)
    assertEquals(offset + 2L, zkClient.getConsumerOffset(group, topicPartition).get)
  }

  @Test
  def testGetConsumerOffsetNoData() {
    zkClient.createRecursive(ConsumerOffset.path(group, topicPartition.topic, topicPartition.partition))
    assertTrue(zkClient.getConsumerOffset(group, topicPartition).isEmpty)
  }

  @Test
  def testDeleteRecursive() {
    zkClient.deleteRecursive("/delete/does-not-exist")

    zkClient.createRecursive("/delete/some/random/path")
    assertTrue(zkClient.pathExists("/delete/some/random/path"))
    zkClient.deleteRecursive("/delete")
    assertFalse(zkClient.pathExists("/delete/some/random/path"))
    assertFalse(zkClient.pathExists("/delete/some/random"))
    assertFalse(zkClient.pathExists("/delete/some"))
    assertFalse(zkClient.pathExists("/delete"))

    intercept[IllegalArgumentException](zkClient.deleteRecursive("delete-invalid-path"))
  }

  @Test
  def testCreateRecursive() {
    zkClient.createRecursive("/create-newrootpath")
    assertTrue(zkClient.pathExists("/create-newrootpath"))

    zkClient.createRecursive("/create/some/random/long/path")
    assertTrue(zkClient.pathExists("/create/some/random/long/path"))
    zkClient.createRecursive("/create/some/random/long/path", throwIfPathExists = false) // no errors if path already exists

    intercept[IllegalArgumentException](zkClient.createRecursive("create-invalid-path"))
  }

  @Test
  def testTopicAssignmentMethods() {
    val topic1 = "topic1"
    val topic2 = "topic2"

    // test with non-existing topic
    assertTrue(zkClient.getTopicPartitionCount(topic1).isEmpty)
    assertTrue(zkClient.getPartitionAssignmentForTopics(Set(topic1)).isEmpty)
    assertTrue(zkClient.getPartitionsForTopics(Set(topic1)).isEmpty)
    assertTrue(zkClient.getReplicasForPartition(new TopicPartition(topic1, 2)).isEmpty)

    val assignment = Map(
      new TopicPartition(topic1, 0) -> Seq(0, 1),
      new TopicPartition(topic1, 1) -> Seq(0, 1),
      new TopicPartition(topic1, 2) -> Seq(1, 2, 3)
    )

    // create a topic assignment
    zkClient.createTopicAssignment(topic1, assignment)

    val expectedAssignment = assignment map { topicAssignment =>
      val partition = topicAssignment._1.partition
      val assignment = topicAssignment._2
      partition -> assignment
    }

    assertEquals(assignment.size, zkClient.getTopicPartitionCount(topic1).get)
    assertEquals(expectedAssignment, zkClient.getPartitionAssignmentForTopics(Set(topic1)).get(topic1).get)
    assertEquals(Set(0, 1, 2), zkClient.getPartitionsForTopics(Set(topic1)).get(topic1).get.toSet)
    assertEquals(Set(1, 2, 3), zkClient.getReplicasForPartition(new TopicPartition(topic1, 2)).toSet)

    val updatedAssignment = assignment - new TopicPartition(topic1, 2)

    zkClient.setTopicAssignment(topic1, updatedAssignment)
    assertEquals(updatedAssignment.size, zkClient.getTopicPartitionCount(topic1).get)

    // add second topic
    val secondAssignment = Map(
      new TopicPartition(topic2, 0) -> Seq(0, 1),
      new TopicPartition(topic2, 1) -> Seq(0, 1)
    )

    zkClient.createTopicAssignment(topic2, secondAssignment)

    assertEquals(Set(topic1, topic2), zkClient.getAllTopicsInCluster.toSet)
  }

  @Test
  def testGetDataAndVersion() {
    val path = "/testpath"

    // test with non-existing path
    val (data0, version0) = zkClient.getDataAndVersion(path)
    assertTrue(data0.isEmpty)
    assertEquals(-1, version0)

    // create a test path
    zkClient.createRecursive(path)
    zkClient.conditionalUpdatePath(path, "version1".getBytes(UTF_8), 0)

    // test with existing path
    val (data1, version1) = zkClient.getDataAndVersion(path)
    assertEquals("version1", new String(data1.get, UTF_8))
    assertEquals(1, version1)

    zkClient.conditionalUpdatePath(path, "version2".getBytes(UTF_8), 1)
    val (data2, version2) = zkClient.getDataAndVersion(path)
    assertEquals("version2", new String(data2.get, UTF_8))
    assertEquals(2, version2)
  }

  @Test
  def testConditionalUpdatePath() {
    val path = "/testconditionalpath"

    // test with non-existing path
    var statusAndVersion = zkClient.conditionalUpdatePath(path, "version0".getBytes(UTF_8), 0)
    assertFalse(statusAndVersion._1)
    assertEquals(-1, statusAndVersion._2)

    // create path
    zkClient.createRecursive(path)

    // test with valid expected version
    statusAndVersion = zkClient.conditionalUpdatePath(path, "version1".getBytes(UTF_8), 0)
    assertTrue(statusAndVersion._1)
    assertEquals(1, statusAndVersion._2)

    // test with invalid expected version
    statusAndVersion = zkClient.conditionalUpdatePath(path, "version2".getBytes(UTF_8), 2)
    assertFalse(statusAndVersion._1)
    assertEquals(-1, statusAndVersion._2)
  }

  @Test
  def testCreateSequentialPersistentPath(): Unit = {
    val path = "/testpath"
    zkClient.createRecursive(path)

    var result = zkClient.createSequentialPersistentPath(path + "/sequence_", null)
    assertEquals(s"$path/sequence_0000000000", result)
    assertTrue(zkClient.pathExists(s"$path/sequence_0000000000"))
    assertEquals(None, dataAsString(s"$path/sequence_0000000000"))

    result = zkClient.createSequentialPersistentPath(path + "/sequence_", "some value".getBytes(UTF_8))
    assertEquals(s"$path/sequence_0000000001", result)
    assertTrue(zkClient.pathExists(s"$path/sequence_0000000001"))
    assertEquals(Some("some value"), dataAsString(s"$path/sequence_0000000001"))
  }

  @Test
  def testPropagateIsrChanges(): Unit = {
    zkClient.createRecursive("/isr_change_notification")

    zkClient.propagateIsrChanges(Set(new TopicPartition("topic-a", 0), new TopicPartition("topic-b", 0)))
    var expectedPath = "/isr_change_notification/isr_change_0000000000"
    assertTrue(zkClient.pathExists(expectedPath))
    assertEquals(Some("""{"version":1,"partitions":[{"topic":"topic-a","partition":0},{"topic":"topic-b","partition":0}]}"""),
      dataAsString(expectedPath))

    zkClient.propagateIsrChanges(Set(new TopicPartition("topic-b", 0)))
    expectedPath = "/isr_change_notification/isr_change_0000000001"
    assertTrue(zkClient.pathExists(expectedPath))
    assertEquals(Some("""{"version":1,"partitions":[{"topic":"topic-b","partition":0}]}"""), dataAsString(expectedPath))
  }

  @Test
  def testPropagateLogDir(): Unit = {
    zkClient.createRecursive("/log_dir_event_notification")

    val brokerId = 3

    zkClient.propagateLogDirEvent(brokerId)
    var expectedPath = "/log_dir_event_notification/log_dir_event_0000000000"
    assertTrue(zkClient.pathExists(expectedPath))
    assertEquals(Some("""{"version":1,"broker":3,"event":1}"""), dataAsString(expectedPath))

    zkClient.propagateLogDirEvent(brokerId)
    expectedPath = "/log_dir_event_notification/log_dir_event_0000000001"
    assertTrue(zkClient.pathExists(expectedPath))
    assertEquals(Some("""{"version":1,"broker":3,"event":1}"""), dataAsString(expectedPath))

    val anotherBrokerId = 4
    zkClient.propagateLogDirEvent(anotherBrokerId)
    expectedPath = "/log_dir_event_notification/log_dir_event_0000000002"
    assertTrue(zkClient.pathExists(expectedPath))
    assertEquals(Some("""{"version":1,"broker":4,"event":1}"""), dataAsString(expectedPath))
  }

  @Test
  def testSetGetAndDeletePartitionReassignment() {
    zkClient.createRecursive(AdminZNode.path)

    assertEquals(Map.empty, zkClient.getPartitionReassignment)

    val reassignment = Map(
      new TopicPartition("topic_a", 0) -> Seq(0, 1, 3),
      new TopicPartition("topic_a", 1) -> Seq(2, 1, 3),
      new TopicPartition("topic_b", 0) -> Seq(4, 5),
      new TopicPartition("topic_c", 0) -> Seq(5, 3)
    )
    zkClient.setOrCreatePartitionReassignment(reassignment)
    assertEquals(reassignment, zkClient.getPartitionReassignment)

    val updatedReassingment = reassignment - new TopicPartition("topic_b", 0)
    zkClient.setOrCreatePartitionReassignment(updatedReassingment)
    assertEquals(updatedReassingment, zkClient.getPartitionReassignment)

    zkClient.deletePartitionReassignment()
    assertEquals(Map.empty, zkClient.getPartitionReassignment)

    zkClient.createPartitionReassignment(reassignment)
    assertEquals(reassignment, zkClient.getPartitionReassignment)
  }

  @Test
  def testGetDataAndStat() {
    val path = "/testpath"

    // test with non-existing path
    val (data0, version0) = zkClient.getDataAndStat(path)
    assertTrue(data0.isEmpty)
    assertEquals(0, version0.getVersion)

    // create a test path
    zkClient.createRecursive(path)
    zkClient.conditionalUpdatePath(path, "version1".getBytes(UTF_8), 0)

    // test with existing path
    val (data1, version1) = zkClient.getDataAndStat(path)
    assertEquals("version1", new String(data1.get, UTF_8))
    assertEquals(1, version1.getVersion)

    zkClient.conditionalUpdatePath(path, "version2".getBytes(UTF_8), 1)
    val (data2, version2) = zkClient.getDataAndStat(path)
    assertEquals("version2", new String(data2.get, UTF_8))
    assertEquals(2, version2.getVersion)
  }

  @Test
  def testGetChildren() {
    val path = "/testpath"

    // test with non-existing path
    assertTrue(zkClient.getChildren(path).isEmpty)

    // create child nodes
    zkClient.createRecursive( "/testpath/child1")
    zkClient.createRecursive( "/testpath/child2")
    zkClient.createRecursive( "/testpath/child3")

    val children = zkClient.getChildren(path)

    assertEquals(3, children.size)
    assertEquals(Set("child1","child2","child3"), children.toSet)
  }

  @Test
  def testAclManagementMethods() {

    assertFalse(zkClient.pathExists(AclZNode.path))
    assertFalse(zkClient.pathExists(AclChangeNotificationZNode.path))
    ResourceType.values.foreach(resource => assertFalse(zkClient.pathExists(ResourceTypeZNode.path(resource.name))))

    // create acl paths
    zkClient.createAclPaths

    assertTrue(zkClient.pathExists(AclZNode.path))
    assertTrue(zkClient.pathExists(AclChangeNotificationZNode.path))
    ResourceType.values.foreach(resource => assertTrue(zkClient.pathExists(ResourceTypeZNode.path(resource.name))))

    val resource1 = new Resource(Topic, UUID.randomUUID().toString)
    val resource2 = new Resource(Topic, UUID.randomUUID().toString)

    // try getting acls for non-existing resource
    var versionedAcls = zkClient.getVersionedAclsForResource(resource1)
    assertTrue(versionedAcls.acls.isEmpty)
    assertEquals(-1, versionedAcls.zkVersion)
    assertFalse(zkClient.resourceExists(resource1))


    val acl1 = new Acl(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "alice"), Deny, "host1" , Read)
    val acl2 = new Acl(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "bob"), Allow, "*", Read)
    val acl3 = new Acl(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "bob"), Deny, "host1", Read)

    //create acls for resources
    zkClient.conditionalSetOrCreateAclsForResource(resource1, Set(acl1, acl2), 0)
    zkClient.conditionalSetOrCreateAclsForResource(resource2, Set(acl1, acl3), 0)

    versionedAcls = zkClient.getVersionedAclsForResource(resource1)
    assertEquals(Set(acl1, acl2), versionedAcls.acls)
    assertEquals(0, versionedAcls.zkVersion)
    assertTrue(zkClient.resourceExists(resource1))

    //update acls for resource
    zkClient.conditionalSetOrCreateAclsForResource(resource1, Set(acl1, acl3), 0)

    versionedAcls = zkClient.getVersionedAclsForResource(resource1)
    assertEquals(Set(acl1, acl3), versionedAcls.acls)
    assertEquals(1, versionedAcls.zkVersion)

    //get resource Types
    assertTrue(ResourceType.values.map( rt => rt.name).toSet == zkClient.getResourceTypes().toSet)

    //get resource name
    val resourceNames = zkClient.getResourceNames(Topic.name)
    assertEquals(2, resourceNames.size)
    assertTrue(Set(resource1.name,resource2.name) == resourceNames.toSet)

    //delete resource
    assertTrue(zkClient.deleteResource(resource1))
    assertFalse(zkClient.resourceExists(resource1))

    //delete with invalid expected zk version
    assertFalse(zkClient.conditionalDelete(resource2, 10))
    //delete with valid expected zk version
    assertTrue(zkClient.conditionalDelete(resource2, 0))


    zkClient.createAclChangeNotification("resource1")
    zkClient.createAclChangeNotification("resource2")

    assertEquals(2, zkClient.getChildren(AclChangeNotificationZNode.path).size)

    zkClient.deleteAclChangeNotifications()
    assertTrue(zkClient.getChildren(AclChangeNotificationZNode.path).isEmpty)
  }

  @Test
  def testDeleteTopicPathMethods() {
    val topic1 = "topic1"
    val topic2 = "topic2"

    assertFalse(zkClient.isTopicMarkedForDeletion(topic1))
    assertTrue(zkClient.getTopicDeletions.isEmpty)

    zkClient.createDeleteTopicPath(topic1)
    zkClient.createDeleteTopicPath(topic2)

    assertTrue(zkClient.isTopicMarkedForDeletion(topic1))
    assertEquals(Set(topic1, topic2), zkClient.getTopicDeletions.toSet)

    zkClient.deleteTopicDeletions(Seq(topic1, topic2))
    assertTrue(zkClient.getTopicDeletions.isEmpty)
  }

  @Test
  def testEntityConfigManagementMethods() {
    val topic1 = "topic1"
    val topic2 = "topic2"

    assertTrue(zkClient.getEntityConfigs(ConfigType.Topic, topic1).isEmpty)

    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, "1024")
    logProps.put(LogConfig.SegmentIndexBytesProp, "1024")
    logProps.put(LogConfig.CleanupPolicyProp, LogConfig.Compact)

    zkClient.setOrCreateEntityConfigs(ConfigType.Topic, topic1, logProps)
    assertEquals(logProps, zkClient.getEntityConfigs(ConfigType.Topic, topic1))

    logProps.remove(LogConfig.CleanupPolicyProp)
    zkClient.setOrCreateEntityConfigs(ConfigType.Topic, topic1, logProps)
    assertEquals(logProps, zkClient.getEntityConfigs(ConfigType.Topic, topic1))

    zkClient.setOrCreateEntityConfigs(ConfigType.Topic, topic2, logProps)
    assertEquals(Set(topic1, topic2), zkClient.getAllEntitiesWithConfig(ConfigType.Topic).toSet)

    zkClient.deleteTopicConfigs(Seq(topic1, topic2))
    assertTrue(zkClient.getEntityConfigs(ConfigType.Topic, topic1).isEmpty)
  }

  @Test
  def testBrokerRegistrationMethods() {
    zkClient.createTopLevelPaths()

    val brokerInfo = BrokerInfo(Broker(1,
      Seq(new EndPoint("test.host", 9999, ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT), SecurityProtocol.PLAINTEXT)),
      rack = None), ApiVersion.latestVersion, jmxPort = 9998)

    zkClient.registerBrokerInZk(brokerInfo)
    assertEquals(Some(brokerInfo.broker), zkClient.getBroker(1))
  }

  @Test
  def testClusterIdMethods() {
    val clusterId = CoreUtils.generateUuidAsBase64

    zkClient.createOrGetClusterId(clusterId)
    assertEquals(clusterId, zkClient.getClusterId.getOrElse(fail("No cluster id found")))
   }

  @Test
  def testBrokerSequenceIdMethods() {
    val sequenceId = zkClient.generateBrokerSequenceId()
    assertEquals(sequenceId + 1, zkClient.generateBrokerSequenceId)
  }

  @Test
  def testCreateTopLevelPaths() {
    zkClient.createTopLevelPaths()

    ZkData.PersistentZkPaths.foreach(path => assertTrue(zkClient.pathExists(path)))
  }

  @Test
  def testPreferredReplicaElectionMethods() {

    assertTrue(zkClient.getPreferredReplicaElection.isEmpty)

    val topic1 = "topic1"
    val electionPartitions = Set(new TopicPartition(topic1, 0), new TopicPartition(topic1, 1))

    zkClient.createPreferredReplicaElection(electionPartitions)
    assertEquals(electionPartitions, zkClient.getPreferredReplicaElection)

    intercept[NodeExistsException] {
      zkClient.createPreferredReplicaElection(electionPartitions)
    }

    zkClient.deletePreferredReplicaElection()
    assertTrue(zkClient.getPreferredReplicaElection.isEmpty)
  }

  private def dataAsString(path: String): Option[String] = {
    val (data, _) = zkClient.getDataAndStat(path)
    data.map(new String(_, UTF_8))
  }

}
