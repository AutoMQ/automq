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
package kafka.admin

import java.util.{Collections, Properties}

import kafka.admin.TopicCommand.{AdminClientTopicService, TopicCommandOptions}
import kafka.common.AdminCommandFailedException
import kafka.integration.KafkaServerTestHarness
import kafka.server.{ConfigType, KafkaConfig}
import kafka.utils.{Exit, Logging, TestUtils}
import kafka.zk.{ConfigEntityChangeNotificationZNode, DeleteTopicsTopicZNode}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.{ConfigException, ConfigResource, TopicConfig}
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}
import org.junit.rules.TestName
import org.junit.{After, Before, Rule, Test}
import org.scalatest.Assertions.{fail, intercept}

import scala.collection.JavaConverters._
import scala.collection.Seq
import scala.concurrent.ExecutionException
import scala.util.Random

class TopicCommandWithAdminClientTest extends KafkaServerTestHarness with Logging with RackAwareTest {

  /**
    * Implementations must override this method to return a set of KafkaConfigs. This method will be invoked for every
    * test and should not reuse previous configurations unless they select their ports randomly when servers are started.
    */
  override def generateConfigs: Seq[KafkaConfig] = TestUtils.createBrokerConfigs(
    numConfigs = 6,
    zkConnect = zkConnect,
    rackInfo = Map(0 -> "rack1", 1 -> "rack2", 2 -> "rack2", 3 -> "rack1", 4 -> "rack3", 5 -> "rack3"),
    numPartitions = numPartitions,
    defaultReplicationFactor = defaultReplicationFactor
    ).map(KafkaConfig.fromProps)

  private val numPartitions = 1
  private val defaultReplicationFactor = 1.toShort

  private var topicService: AdminClientTopicService = _
  private var adminClient: Admin = _
  private var testTopicName: String = _

  private val _testName = new TestName
  @Rule def testName = _testName

  def assertExitCode(expected: Int, method: () => Unit): Unit = {
    def mockExitProcedure(exitCode: Int, exitMessage: Option[String]): Nothing = {
      assertEquals(expected, exitCode)
      throw new RuntimeException
    }
    Exit.setExitProcedure(mockExitProcedure)
    try {
      intercept[RuntimeException] {
        method()
      }
    } finally {
      Exit.resetExitProcedure()
    }
  }

  def assertCheckArgsExitCode(expected: Int, options: TopicCommandOptions): Unit = {
    assertExitCode(expected, options.checkArgs _)
  }

  def createAndWaitTopic(opts: TopicCommandOptions): Unit = {
    topicService.createTopic(opts)
    waitForTopicCreated(opts.topic.get)
  }

  def waitForTopicCreated(topicName: String, timeout: Int = 10000): Unit = {
    val finishTime = System.currentTimeMillis() + timeout
    var result = false
    while (System.currentTimeMillis() < finishTime || !result) {
      val topics = adminClient.listTopics(new ListTopicsOptions().listInternal(true)).names().get()
      result = topics.contains(topicName)
      Thread.sleep(100)
    }
    assertTrue(s"Topic $topicName has not been created within the given $timeout time", result)
  }

  @Before
  def setup(): Unit = {
    // create adminClient
    val props = new Properties()
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    adminClient = AdminClient.create(props)
    topicService = AdminClientTopicService(adminClient)
    testTopicName = s"${testName.getMethodName}-${Random.alphanumeric.take(10).mkString}"
  }

  @After
  def close(): Unit = {
    // adminClient is closed by topicService
    if (topicService != null)
      topicService.close()
  }

  @Test
  def testParseAssignment(): Unit = {
    val actualAssignment = TopicCommand.parseReplicaAssignment("5:4,3:2,1:0")
    val expectedAssignment = Map(0 -> List(5, 4), 1 -> List(3, 2), 2 -> List(1, 0))
    assertEquals(expectedAssignment, actualAssignment)
  }

  @Test
  def testParseAssignmentDuplicateEntries(): Unit = {
    intercept[AdminCommandFailedException] {
      TopicCommand.parseReplicaAssignment("5:5")
    }
  }

  @Test
  def testParseAssignmentPartitionsOfDifferentSize(): Unit = {
    intercept[AdminOperationException] {
      TopicCommand.parseReplicaAssignment("5:4:3,2:1")
    }
  }

  @Test
  def testConfigOptWithBootstrapServers(): Unit = {
    assertCheckArgsExitCode(1,
      new TopicCommandOptions(Array("--bootstrap-server", brokerList ,"--alter", "--topic", testTopicName, "--partitions", "3", "--config", "cleanup.policy=compact")))
    assertCheckArgsExitCode(1,
      new TopicCommandOptions(Array("--bootstrap-server", brokerList ,"--alter", "--topic", testTopicName, "--partitions", "3", "--delete-config", "cleanup.policy")))
    val opts =
      new TopicCommandOptions(Array("--bootstrap-server", brokerList ,"--create", "--topic", testTopicName, "--partitions", "3", "--replication-factor", "3", "--config", "cleanup.policy=compact"))
    opts.checkArgs()
    assertTrue(opts.hasCreateOption)
    assertEquals(brokerList, opts.bootstrapServer.get)
    assertEquals("cleanup.policy=compact", opts.topicConfig.get.get(0))

  }

  @Test
  def testCreate(): Unit = {
    createAndWaitTopic(new TopicCommandOptions(
      Array("--partitions", "2", "--replication-factor", "1", "--topic", testTopicName)))

    adminClient.listTopics().names().get().contains(testTopicName)
  }

  @Test
  def testCreateWithDefaults(): Unit = {
    createAndWaitTopic(new TopicCommandOptions(
      Array("--topic", testTopicName)))

    val partitions = adminClient
      .describeTopics(Collections.singletonList(testTopicName))
      .all()
      .get()
      .get(testTopicName)
      .partitions()
    assertEquals(partitions.size(), numPartitions)
    assertEquals(partitions.get(0).replicas().size(), defaultReplicationFactor)
  }

  @Test
  def testCreateWithDefaultReplication(): Unit = {
    createAndWaitTopic(new TopicCommandOptions(
      Array("--topic", testTopicName, "--partitions", "2")))

    val partitions = adminClient
      .describeTopics(Collections.singletonList(testTopicName))
      .all()
      .get()
      .get(testTopicName)
      .partitions()
    assertEquals(partitions.size(), 2)
    assertEquals(partitions.get(0).replicas().size(), defaultReplicationFactor)
  }

  @Test
  def testCreateWithDefaultPartitions(): Unit = {
    createAndWaitTopic(new TopicCommandOptions(
      Array("--topic", testTopicName, "--replication-factor", "2")))

    val partitions = adminClient
      .describeTopics(Collections.singletonList(testTopicName))
      .all()
      .get()
      .get(testTopicName)
      .partitions()

    assertEquals(partitions.size(), numPartitions)
    assertEquals(partitions.get(0).replicas().size(), 2)
  }

  @Test
  def testCreateWithConfigs(): Unit = {
    val configResource = new ConfigResource(ConfigResource.Type.TOPIC, testTopicName)
    createAndWaitTopic(new TopicCommandOptions(
      Array("--partitions", "2", "--replication-factor", "2", "--topic", testTopicName, "--config", "delete.retention.ms=1000")))

    val configs = adminClient
      .describeConfigs(Collections.singleton(configResource))
      .all().get().get(configResource)
    assertEquals(1000, Integer.valueOf(configs.get("delete.retention.ms").value()))
  }

  @Test
  def testCreateIfItAlreadyExists(): Unit = {
    val numPartitions = 1

    // create the topic
    val createOpts = new TopicCommandOptions(
      Array("--partitions", numPartitions.toString, "--replication-factor", "1", "--topic", testTopicName))
    createAndWaitTopic(createOpts)

    // try to re-create the topic
    intercept[IllegalArgumentException] {
      topicService.createTopic(createOpts)
    }
  }

  @Test
  def testCreateWithReplicaAssignment(): Unit = {
    // create the topic
    val createOpts = new TopicCommandOptions(
      Array("--replica-assignment", "5:4,3:2,1:0", "--topic", testTopicName))
    createAndWaitTopic(createOpts)

    val partitions = adminClient
      .describeTopics(Collections.singletonList(testTopicName))
      .all()
      .get()
      .get(testTopicName)
      .partitions()
    assertEquals(3, partitions.size())
    assertEquals(List(5, 4), partitions.get(0).replicas().asScala.map(_.id()))
    assertEquals(List(3, 2), partitions.get(1).replicas().asScala.map(_.id()))
    assertEquals(List(1, 0), partitions.get(2).replicas().asScala.map(_.id()))
  }

  @Test
  def testCreateWithInvalidReplicationFactor(): Unit = {
    intercept[IllegalArgumentException] {
      topicService.createTopic(new TopicCommandOptions(
        Array("--partitions", "2", "--replication-factor", (Short.MaxValue+1).toString, "--topic", testTopicName)))
    }
  }

  @Test
  def testCreateWithNegativeReplicationFactor(): Unit = {
    intercept[IllegalArgumentException] {
      topicService.createTopic(new TopicCommandOptions(
        Array("--partitions", "2", "--replication-factor", "-1", "--topic", testTopicName)))
    }
  }

  @Test
  def testCreateWithAssignmentAndPartitionCount(): Unit = {
    assertCheckArgsExitCode(1,
      new TopicCommandOptions(
        Array("--bootstrap-server", brokerList,
          "--create",
          "--replica-assignment", "3:0,5:1",
          "--partitions", "2",
          "--topic", "testTopic")))
  }

  @Test
  def testCreateWithAssignmentAndReplicationFactor(): Unit = {
    assertCheckArgsExitCode(1,
      new TopicCommandOptions(
        Array("--bootstrap-server", brokerList,
          "--create",
          "--replica-assignment", "3:0,5:1",
          "--replication-factor", "2",
          "--topic", "testTopic")))
  }

  @Test
  def testCreateWithNegativePartitionCount(): Unit = {
    intercept[IllegalArgumentException] {
      topicService.createTopic(new TopicCommandOptions(
        Array("--partitions", "-1", "--replication-factor", "1", "--topic", testTopicName)))
    }
  }

  @Test
  def testCreateWithUnspecifiedReplicationFactorAndPartitionsWithZkClient(): Unit = {
    assertExitCode(1, () =>
      new TopicCommandOptions(Array("--create", "--zookeeper", "zk", "--topic", testTopicName)).checkArgs()
    )
  }

  @Test
  def testInvalidTopicLevelConfig(): Unit = {
    val createOpts = new TopicCommandOptions(
      Array("--partitions", "1", "--replication-factor", "1", "--topic", testTopicName,
        "--config", "message.timestamp.type=boom"))
    intercept[ConfigException] {
      topicService.createTopic(createOpts)
    }
  }

  @Test
  def testListTopics(): Unit = {
    createAndWaitTopic(new TopicCommandOptions(
      Array("--partitions", "1", "--replication-factor", "1", "--topic", testTopicName)))

    val output = TestUtils.grabConsoleOutput(
      topicService.listTopics(new TopicCommandOptions(Array())))

    assertTrue(output.contains(testTopicName))
  }

  @Test
  def testListTopicsWithWhitelist(): Unit = {
    val topic1 = "kafka.testTopic1"
    val topic2 = "kafka.testTopic2"
    val topic3 = "oooof.testTopic1"
    adminClient.createTopics(
      List(new NewTopic(topic1, 2, 2.toShort),
        new NewTopic(topic2, 2, 2.toShort),
        new NewTopic(topic3, 2, 2.toShort)).asJavaCollection)
      .all().get()
    waitForTopicCreated(topic1)
    waitForTopicCreated(topic2)
    waitForTopicCreated(topic3)

    val output = TestUtils.grabConsoleOutput(
      topicService.listTopics(new TopicCommandOptions(Array("--topic", "kafka.*"))))

    assertTrue(output.contains(topic1))
    assertTrue(output.contains(topic2))
    assertFalse(output.contains(topic3))
  }

  @Test
  def testListTopicsWithExcludeInternal(): Unit = {
    val topic1 = "kafka.testTopic1"
    adminClient.createTopics(
      List(new NewTopic(topic1, 2, 2.toShort),
        new NewTopic(Topic.GROUP_METADATA_TOPIC_NAME, 2, 2.toShort)).asJavaCollection)
      .all().get()
    waitForTopicCreated(topic1)

    val output = TestUtils.grabConsoleOutput(
      topicService.listTopics(new TopicCommandOptions(Array("--exclude-internal"))))

    assertTrue(output.contains(topic1))
    assertFalse(output.contains(Topic.GROUP_METADATA_TOPIC_NAME))
  }

  @Test
  def testAlterPartitionCount(): Unit = {
    adminClient.createTopics(
      List(new NewTopic(testTopicName, 2, 2.toShort)).asJavaCollection).all().get()
    waitForTopicCreated(testTopicName)

    topicService.alterTopic(new TopicCommandOptions(
      Array("--topic", testTopicName, "--partitions", "3")))

    val topicDescription = adminClient.describeTopics(Collections.singletonList(testTopicName)).values().get(testTopicName).get()
    assertTrue(topicDescription.partitions().size() == 3)
  }

  @Test
  def testAlterAssignment(): Unit = {
    adminClient.createTopics(
      Collections.singletonList(new NewTopic(testTopicName, 2, 2.toShort))).all().get()
    waitForTopicCreated(testTopicName)

    topicService.alterTopic(new TopicCommandOptions(
      Array("--topic", testTopicName, "--replica-assignment", "5:3,3:1,4:2", "--partitions", "3")))

    val topicDescription = adminClient.describeTopics(Collections.singletonList(testTopicName)).values().get(testTopicName).get()
    assertTrue(topicDescription.partitions().size() == 3)
    assertEquals(List(4,2), topicDescription.partitions().get(2).replicas().asScala.map(_.id()))
  }

  @Test
  def testAlterAssignmentWithMoreAssignmentThanPartitions(): Unit = {
    adminClient.createTopics(
      List(new NewTopic(testTopicName, 2, 2.toShort)).asJavaCollection).all().get()
    waitForTopicCreated(testTopicName)

    intercept[ExecutionException] {
      topicService.alterTopic(new TopicCommandOptions(
        Array("--topic", testTopicName, "--replica-assignment", "5:3,3:1,4:2,3:2", "--partitions", "3")))
    }
  }

  @Test
  def testAlterAssignmentWithMorePartitionsThanAssignment(): Unit = {
    adminClient.createTopics(
      List(new NewTopic(testTopicName, 2, 2.toShort)).asJavaCollection).all().get()
    waitForTopicCreated(testTopicName)

    intercept[ExecutionException] {
      topicService.alterTopic(new TopicCommandOptions(
        Array("--topic", testTopicName, "--replica-assignment", "5:3,3:1,4:2", "--partitions", "6")))
    }
  }

  @Test
  def testAlterWithInvalidPartitionCount(): Unit = {
    createAndWaitTopic(new TopicCommandOptions(
      Array("--partitions", "1", "--replication-factor", "1", "--topic", testTopicName)))

    intercept[ExecutionException] {
      topicService.alterTopic(new TopicCommandOptions(
        Array("--partitions", "-1", "--topic", testTopicName)))
    }
  }

  @Test
  def testAlterWithUnspecifiedPartitionCount(): Unit = {
    assertCheckArgsExitCode(1, new TopicCommandOptions(
      Array("--bootstrap-server", brokerList ,"--alter", "--topic", testTopicName)))
  }

  @Test
  def testAlterWhenTopicDoesntExist(): Unit = {
    // alter a topic that does not exist without --if-exists
    val alterOpts = new TopicCommandOptions(Array("--topic", testTopicName, "--partitions", "1"))
    val topicService = AdminClientTopicService(adminClient)
    intercept[IllegalArgumentException] {
      topicService.alterTopic(alterOpts)
    }
  }

  @Test
  def testIfExistsAndIfNotExistsOptionsInvalidWithBootstrapServers(): Unit = {
    // alter a topic that does not exist without --if-exists
    assertCheckArgsExitCode(1, new TopicCommandOptions(Array("--bootstrap-server", "server1:9092", "--alter", "--if-exists", "--topic", testTopicName, "--partitions", "1")))
    assertCheckArgsExitCode(1, new TopicCommandOptions(Array("--bootstrap-server", "server1:9092", "--create", "--if-not-exists", "--topic", testTopicName, "--partitions", "1", "--replication-factor", "1")))
  }

  @Test
  def testCreateAlterTopicWithRackAware(): Unit = {
    val rackInfo = Map(0 -> "rack1", 1 -> "rack2", 2 -> "rack2", 3 -> "rack1", 4 -> "rack3", 5 -> "rack3")

    val numPartitions = 18
    val replicationFactor = 3
    val createOpts = new TopicCommandOptions(Array(
      "--partitions", numPartitions.toString,
      "--replication-factor", replicationFactor.toString,
      "--topic", testTopicName))
    createAndWaitTopic(createOpts)

    var assignment = zkClient.getReplicaAssignmentForTopics(Set(testTopicName)).map { case (tp, replicas) =>
      tp.partition -> replicas
    }
    checkReplicaDistribution(assignment, rackInfo, rackInfo.size, numPartitions, replicationFactor)

    val alteredNumPartitions = 36
    // verify that adding partitions will also be rack aware
    val alterOpts = new TopicCommandOptions(Array(
      "--partitions", alteredNumPartitions.toString,
      "--topic", testTopicName))
    topicService.alterTopic(alterOpts)
    assignment = zkClient.getReplicaAssignmentForTopics(Set(testTopicName)).map { case (tp, replicas) =>
      tp.partition -> replicas
    }
    checkReplicaDistribution(assignment, rackInfo, rackInfo.size, alteredNumPartitions, replicationFactor)
  }

  @Test
  def testConfigPreservationAcrossPartitionAlteration(): Unit = {
    val numPartitionsOriginal = 1
    val cleanupKey = "cleanup.policy"
    val cleanupVal = "compact"

    // create the topic
    val createOpts = new TopicCommandOptions(Array("--partitions", numPartitionsOriginal.toString,
      "--replication-factor", "1",
      "--config", cleanupKey + "=" + cleanupVal,
      "--topic", testTopicName))
    createAndWaitTopic(createOpts)
    val props = adminZkClient.fetchEntityConfig(ConfigType.Topic, testTopicName)
    assertTrue("Properties after creation don't contain " + cleanupKey, props.containsKey(cleanupKey))
    assertTrue("Properties after creation have incorrect value", props.getProperty(cleanupKey).equals(cleanupVal))

    // pre-create the topic config changes path to avoid a NoNodeException
    zkClient.makeSurePersistentPathExists(ConfigEntityChangeNotificationZNode.path)

    // modify the topic to add new partitions
    val numPartitionsModified = 3
    val alterOpts = new TopicCommandOptions(Array("--partitions", numPartitionsModified.toString, "--topic", testTopicName))
    topicService.alterTopic(alterOpts)
    val newProps = adminZkClient.fetchEntityConfig(ConfigType.Topic, testTopicName)
    assertTrue("Updated properties do not contain " + cleanupKey, newProps.containsKey(cleanupKey))
    assertTrue("Updated properties have incorrect value", newProps.getProperty(cleanupKey).equals(cleanupVal))
  }

  @Test
  def testTopicDeletion(): Unit = {
    // create the NormalTopic
    val createOpts = new TopicCommandOptions(Array("--partitions", "1",
      "--replication-factor", "1",
      "--topic", testTopicName))
    createAndWaitTopic(createOpts)

    // delete the NormalTopic
    val deleteOpts = new TopicCommandOptions(Array("--topic", testTopicName))

    val deletePath = DeleteTopicsTopicZNode.path(testTopicName)
    assertFalse("Delete path for topic shouldn't exist before deletion.", zkClient.pathExists(deletePath))
    topicService.deleteTopic(deleteOpts)
    TestUtils.verifyTopicDeletion(zkClient, testTopicName, 1, servers)
  }

  @Test
  def testDeleteInternalTopic(): Unit = {
    // create the offset topic
    val createOffsetTopicOpts = new TopicCommandOptions(Array("--partitions", "1",
      "--replication-factor", "1",
      "--topic", Topic.GROUP_METADATA_TOPIC_NAME))
    createAndWaitTopic(createOffsetTopicOpts)

    // Try to delete the Topic.GROUP_METADATA_TOPIC_NAME which is allowed by default.
    // This is a difference between the new and the old command as the old one didn't allow internal topic deletion.
    // If deleting internal topics is not desired, ACLS should be used to control it.
    val deleteOffsetTopicOpts = new TopicCommandOptions(Array("--topic", Topic.GROUP_METADATA_TOPIC_NAME))
    val deleteOffsetTopicPath = DeleteTopicsTopicZNode.path(Topic.GROUP_METADATA_TOPIC_NAME)
    assertFalse("Delete path for topic shouldn't exist before deletion.", zkClient.pathExists(deleteOffsetTopicPath))
    topicService.deleteTopic(deleteOffsetTopicOpts)
    TestUtils.verifyTopicDeletion(zkClient, Topic.GROUP_METADATA_TOPIC_NAME, 1, servers)
  }

  @Test
  def testDeleteIfExists(): Unit = {
    // delete a topic that does not exist
    val deleteOpts = new TopicCommandOptions(Array("--topic", testTopicName))
    intercept[IllegalArgumentException] {
      topicService.deleteTopic(deleteOpts)
    }
  }

  @Test
  def testDescribe(): Unit = {
    adminClient.createTopics(
      Collections.singletonList(new NewTopic(testTopicName, 2, 2.toShort))).all().get()
    waitForTopicCreated(testTopicName)

    val output = TestUtils.grabConsoleOutput(
      topicService.describeTopic(new TopicCommandOptions(Array("--topic", testTopicName))))
    val rows = output.split("\n")
    assertEquals(3, rows.size)
    rows(0).startsWith(s"Topic:$testTopicName\tPartitionCount:2")
  }

  @Test
  def testDescribeUnavailablePartitions(): Unit = {
    adminClient.createTopics(
      Collections.singletonList(new NewTopic(testTopicName, 6, 1.toShort))).all().get()
    waitForTopicCreated(testTopicName)

    try {
      // check which partition is on broker 0 which we'll kill
      val testTopicDescription = adminClient.describeTopics(Collections.singletonList(testTopicName))
        .all().get().asScala(testTopicName)
      val partitionOnBroker0 = testTopicDescription.partitions().asScala.find(_.leader().id() == 0).get.partition()

      killBroker(0)

      // wait until the topic metadata for the test topic is propagated to each alive broker
      TestUtils.waitUntilTrue(() => {
        servers
          .filterNot(_.config.brokerId == 0)
          .foldLeft(true) {
            (result, server) => {
              val topicMetadatas = server.dataPlaneRequestProcessor.metadataCache
                .getTopicMetadata(Set(testTopicName), ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT))
              val testPartitionMetadata = topicMetadatas.find(_.topic().equals(testTopicName)).get.partitionMetadata().asScala.find(_.partition() == partitionOnBroker0)
              testPartitionMetadata match {
                case None => fail(s"Partition metadata is not found in metadata cache")
                case Some(metadata) => {
                  result && metadata.error() == Errors.LEADER_NOT_AVAILABLE
                }
              }
            }
          }
      }, s"Partition metadata for $testTopicName is not propagated")

      // grab the console output and assert
      val output = TestUtils.grabConsoleOutput(
          topicService.describeTopic(new TopicCommandOptions(Array("--topic", testTopicName, "--unavailable-partitions"))))
      val rows = output.split("\n")
      assertTrue(rows(0).startsWith(s"\tTopic: $testTopicName"))
      assertTrue(rows(0).contains("Leader: none\tReplicas: 0\tIsr:"))
    } finally {
      restartDeadBrokers()
    }
  }

  @Test
  def testDescribeUnderReplicatedPartitions(): Unit = {
    adminClient.createTopics(
      Collections.singletonList(new NewTopic(testTopicName, 1, 6.toShort))).all().get()
    waitForTopicCreated(testTopicName)

    try {
      killBroker(0)
      val aliveServers = servers.filterNot(_.config.brokerId == 0)
      TestUtils.waitUntilMetadataIsPropagated(aliveServers, testTopicName, 0)
      val output = TestUtils.grabConsoleOutput(
        topicService.describeTopic(new TopicCommandOptions(Array("--under-replicated-partitions"))))
      val rows = output.split("\n")
      assertTrue(rows(0).startsWith(s"\tTopic: $testTopicName"))
    } finally {
      restartDeadBrokers()
    }
  }

  @Test
  def testDescribeUnderMinIsrPartitions(): Unit = {
    val configMap = new java.util.HashMap[String, String]()
    configMap.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "6")

    adminClient.createTopics(
      Collections.singletonList(new NewTopic(testTopicName, 1, 6.toShort).configs(configMap))).all().get()
    waitForTopicCreated(testTopicName)

    try {
      killBroker(0)
      val aliveServers = servers.filterNot(_.config.brokerId == 0)
      TestUtils.waitUntilMetadataIsPropagated(aliveServers, testTopicName, 0)
      val output = TestUtils.grabConsoleOutput(
        topicService.describeTopic(new TopicCommandOptions(Array("--under-min-isr-partitions"))))
      val rows = output.split("\n")
      assertTrue(rows(0).startsWith(s"\tTopic: $testTopicName"))
    } finally {
      restartDeadBrokers()
    }
  }

  @Test
  def testDescribeUnderReplicatedPartitionsWhenReassignmentIsInProgress(): Unit = {
    val configMap = new java.util.HashMap[String, String]()
    val replicationFactor: Short = 1
    val partitions = 1

    adminClient.createTopics(
      Collections.singletonList(new NewTopic(testTopicName, partitions, replicationFactor).configs(configMap))).all().get()
    waitForTopicCreated(testTopicName)

    for (msg <- 0 to 10) {
      TestUtils.produceMessage(servers, testTopicName, s"$msg")
    }

    val brokerIds = servers.map(_.config.brokerId)
    TestUtils.throttleAllBrokersReplication(adminClient, brokerIds, throttleBytes = 1)

    val testTopicDesc = adminClient.describeTopics(Collections.singleton(testTopicName)).all().get().get(testTopicName)
    val firstPartition = testTopicDesc.partitions().asScala.head
    val firstTopicPartition = new TopicPartition(testTopicName, firstPartition.partition())
    val replicasOfFirstPartition = firstPartition.replicas().asScala.map(_.id())
    val targetReplica = brokerIds.diff(replicasOfFirstPartition).head

    adminClient.alterPartitionReassignments(Collections.singletonMap(firstTopicPartition,
      NewPartitionReassignment.of(Collections.singletonList(targetReplica))))

    // let's wait until the LAIR is propagated
    TestUtils.waitUntilTrue(() => {
      val reassignments = adminClient.listPartitionReassignments(Collections.singleton(firstTopicPartition)).reassignments().get()
      !reassignments.get(firstTopicPartition).addingReplicas().isEmpty
    }, "Reassignment didn't add the second node")

    // describe the topic and test if it's under-replicated
    val simpleDescribeOutput = TestUtils.grabConsoleOutput(
      topicService.describeTopic(new TopicCommandOptions(Array("--topic", testTopicName))))
    val simpleDescribeOutputRows = simpleDescribeOutput.split("\n")
    assertTrue(simpleDescribeOutputRows(0).startsWith(s"Topic: $testTopicName"))
    assertEquals(2, simpleDescribeOutputRows.size)

    val underReplicatedOutput = TestUtils.grabConsoleOutput(
      topicService.describeTopic(new TopicCommandOptions(Array("--under-replicated-partitions"))))
    assertTrue("--under-replicated-partitions shouldn't return anything", underReplicatedOutput.isEmpty)

    TestUtils.resetBrokersThrottle(adminClient, brokerIds)
    TestUtils.waitForAllReassignmentsToComplete(adminClient)
  }

  @Test
  def testDescribeAtMinIsrPartitions(): Unit = {
    val configMap = new java.util.HashMap[String, String]()
    configMap.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "4")

    adminClient.createTopics(
      Collections.singletonList(new NewTopic(testTopicName, 1, 6.toShort).configs(configMap))).all().get()
    waitForTopicCreated(testTopicName)

    try {
      killBroker(0)
      killBroker(1)
      val output = TestUtils.grabConsoleOutput(
        topicService.describeTopic(new TopicCommandOptions(Array("--at-min-isr-partitions"))))
      val rows = output.split("\n")
      assertTrue(rows(0).startsWith(s"\tTopic: $testTopicName"))
      assertEquals(1, rows.length);
    } finally {
      restartDeadBrokers()
    }
  }

  /**
    * Test describe --under-min-isr-partitions option with four topics:
    *   (1) topic with partition under the configured min ISR count
    *   (2) topic with under-replicated partition (but not under min ISR count)
    *   (3) topic with offline partition
    *   (4) topic with fully replicated partition
    *
    * Output should only display the (1) topic with partition under min ISR count and (3) topic with offline partition
    */
  @Test
  def testDescribeUnderMinIsrPartitionsMixed(): Unit = {
    val underMinIsrTopic = "under-min-isr-topic"
    val notUnderMinIsrTopic = "not-under-min-isr-topic"
    val offlineTopic = "offline-topic"
    val fullyReplicatedTopic = "fully-replicated-topic"

    val configMap = new java.util.HashMap[String, String]()
    configMap.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "6")

    adminClient.createTopics(
      java.util.Arrays.asList(
        new NewTopic(underMinIsrTopic, 1, 6.toShort).configs(configMap),
        new NewTopic(notUnderMinIsrTopic, 1, 6.toShort),
        new NewTopic(offlineTopic, Collections.singletonMap(0, Collections.singletonList(0))),
        new NewTopic(fullyReplicatedTopic, Collections.singletonMap(0, java.util.Arrays.asList(1, 2, 3))))).all().get()

    waitForTopicCreated(underMinIsrTopic)
    waitForTopicCreated(notUnderMinIsrTopic)
    waitForTopicCreated(offlineTopic)
    waitForTopicCreated(fullyReplicatedTopic)

    try {
      killBroker(0)
      val aliveServers = servers.filterNot(_.config.brokerId == 0)
      TestUtils.waitUntilMetadataIsPropagated(aliveServers, underMinIsrTopic, 0)
      val output = TestUtils.grabConsoleOutput(
        topicService.describeTopic(new TopicCommandOptions(Array("--under-min-isr-partitions"))))
      val rows = output.split("\n")
      assertTrue(rows(0).startsWith(s"\tTopic: $underMinIsrTopic"))
      assertTrue(rows(1).startsWith(s"\tTopic: $offlineTopic"))
      assertEquals(2, rows.length);
    } finally {
      restartDeadBrokers()
    }
  }

  @Test
  def testDescribeReportOverriddenConfigs(): Unit = {
    val config = "file.delete.delay.ms=1000"
    createAndWaitTopic(new TopicCommandOptions(
      Array("--partitions", "2", "--replication-factor", "2", "--topic", testTopicName, "--config", config)))
    val output = TestUtils.grabConsoleOutput(
      topicService.describeTopic(new TopicCommandOptions(Array())))
    assertTrue(s"Describe output should have contained $config", output.contains(config))
  }

  @Test
  def testDescribeAndListTopicsWithoutInternalTopics(): Unit = {
    createAndWaitTopic(
      new TopicCommandOptions(Array("--partitions", "1", "--replication-factor", "1", "--topic", testTopicName)))
    // create a internal topic
    createAndWaitTopic(
      new TopicCommandOptions(Array("--partitions", "1", "--replication-factor", "1", "--topic", Topic.GROUP_METADATA_TOPIC_NAME)))

    // test describe
    var output = TestUtils.grabConsoleOutput(topicService.describeTopic(new TopicCommandOptions(Array("--describe", "--exclude-internal"))))
    assertTrue(s"Output should have contained $testTopicName", output.contains(testTopicName))
    assertFalse(output.contains(Topic.GROUP_METADATA_TOPIC_NAME))

    // test list
    output = TestUtils.grabConsoleOutput(topicService.listTopics(new TopicCommandOptions(Array("--list", "--exclude-internal"))))
    assertTrue(output.contains(testTopicName))
    assertFalse(output.contains(Topic.GROUP_METADATA_TOPIC_NAME))
  }
}
