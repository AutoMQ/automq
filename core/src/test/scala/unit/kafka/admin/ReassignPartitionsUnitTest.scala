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

package kafka.admin

import java.util.concurrent.ExecutionException
import java.util.{Arrays, Collections}

import kafka.admin.ReassignPartitionsCommand._
import kafka.common.AdminCommandFailedException
import kafka.utils.Exit
import org.apache.kafka.clients.admin.{Config, MockAdminClient, PartitionReassignment}
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.errors.{InvalidReplicationFactorException, UnknownTopicOrPartitionException}
import org.apache.kafka.common.{Node, TopicPartition, TopicPartitionInfo, TopicPartitionReplica}
import org.junit.Assert.{assertEquals, assertFalse, assertThrows, assertTrue}
import org.junit.function.ThrowingRunnable
import org.junit.rules.Timeout
import org.junit.{After, Assert, Before, Rule, Test}

import scala.collection.mutable
import scala.jdk.CollectionConverters._

class ReassignPartitionsUnitTest {
  @Rule
  def globalTimeout: Timeout = Timeout.millis(60000)

  @Before
  def setUp(): Unit = {
    Exit.setExitProcedure((_, message) => throw new IllegalArgumentException(message.orNull))
  }

  @After
  def tearDown(): Unit = {
    Exit.resetExitProcedure()
  }

  @Test
  def testCompareTopicPartitions(): Unit = {
    assertTrue(compareTopicPartitions(new TopicPartition("abc", 0),
      new TopicPartition("abc", 1)))
    assertFalse(compareTopicPartitions(new TopicPartition("def", 0),
      new TopicPartition("abc", 1)))
  }

  @Test
  def testCompareTopicPartitionReplicas(): Unit = {
    assertTrue(compareTopicPartitionReplicas(new TopicPartitionReplica("def", 0, 0),
      new TopicPartitionReplica("abc", 0, 1)))
    assertFalse(compareTopicPartitionReplicas(new TopicPartitionReplica("def", 0, 0),
      new TopicPartitionReplica("cde", 0, 0)))
  }

  @Test
  def testPartitionReassignStatesToString(): Unit = {
    assertEquals(Seq(
      "Status of partition reassignment:",
      "Reassignment of partition bar-0 is still in progress.",
      "Reassignment of partition foo-0 is complete.",
      "Reassignment of partition foo-1 is still in progress.").
        mkString(System.lineSeparator()),
      partitionReassignmentStatesToString(Map(
        new TopicPartition("foo", 0) ->
          new PartitionReassignmentState(Seq(1, 2, 3), Seq(1, 2, 3), true),
        new TopicPartition("foo", 1) ->
          new PartitionReassignmentState(Seq(1, 2, 3), Seq(1, 2, 4), false),
        new TopicPartition("bar", 0) ->
          new PartitionReassignmentState(Seq(1, 2, 3), Seq(1, 2, 4), false),
      )))
  }

  private def addTopics(adminClient: MockAdminClient): Unit = {
    val b = adminClient.brokers()
    adminClient.addTopic(false, "foo", Arrays.asList(
      new TopicPartitionInfo(0, b.get(0),
        Arrays.asList(b.get(0), b.get(1), b.get(2)),
        Arrays.asList(b.get(0), b.get(1))),
      new TopicPartitionInfo(1, b.get(1),
        Arrays.asList(b.get(1), b.get(2), b.get(3)),
        Arrays.asList(b.get(1), b.get(2), b.get(3)))
    ), Collections.emptyMap())
    adminClient.addTopic(false, "bar", Arrays.asList(
      new TopicPartitionInfo(0, b.get(2),
        Arrays.asList(b.get(2), b.get(3), b.get(0)),
        Arrays.asList(b.get(2), b.get(3), b.get(0)))
    ), Collections.emptyMap())
  }

  @Test
  def testFindPartitionReassignmentStates(): Unit = {
    val adminClient = new MockAdminClient.Builder().numBrokers(4).build()
    try {
      addTopics(adminClient)
      // Create a reassignment and test findPartitionReassignmentStates.
      val reassignmentResult: Map[TopicPartition, Class[_ <: Throwable]] = alterPartitionReassignments(adminClient, Map(
        new TopicPartition("foo", 0) -> Seq(0,1,3),
        new TopicPartition("quux", 0) -> Seq(1,2,3))).map { case (k, v) => k -> v.getClass }.toMap
      assertEquals(Map(new TopicPartition("quux", 0) -> classOf[UnknownTopicOrPartitionException]),
        reassignmentResult)
      assertEquals((Map(
          new TopicPartition("foo", 0) -> PartitionReassignmentState(Seq(0,1,2), Seq(0,1,3), false),
          new TopicPartition("foo", 1) -> PartitionReassignmentState(Seq(1,2,3), Seq(1,2,3), true)
        ), true),
        findPartitionReassignmentStates(adminClient, Seq(
          (new TopicPartition("foo", 0), Seq(0,1,3)),
          (new TopicPartition("foo", 1), Seq(1,2,3))
        )))
      // Cancel the reassignment and test findPartitionReassignmentStates again.
      val cancelResult: Map[TopicPartition, Class[_ <: Throwable]] = cancelPartitionReassignments(adminClient,
        Set(new TopicPartition("foo", 0), new TopicPartition("quux", 2))).map { case (k, v) =>
          k -> v.getClass
        }.toMap
      assertEquals(Map(new TopicPartition("quux", 2) -> classOf[UnknownTopicOrPartitionException]),
        cancelResult)
      assertEquals((Map(
          new TopicPartition("foo", 0) -> PartitionReassignmentState(Seq(0,1,2), Seq(0,1,3), true),
          new TopicPartition("foo", 1) -> PartitionReassignmentState(Seq(1,2,3), Seq(1,2,3), true)
        ), false),
          findPartitionReassignmentStates(adminClient, Seq(
            (new TopicPartition("foo", 0), Seq(0,1,3)),
            (new TopicPartition("foo", 1), Seq(1,2,3))
          )))
    } finally {
      adminClient.close()
    }
  }

  @Test
  def testFindLogDirMoveStates(): Unit = {
    val adminClient = new MockAdminClient.Builder().
      numBrokers(4).
      brokerLogDirs(Arrays.asList(
          Arrays.asList("/tmp/kafka-logs0", "/tmp/kafka-logs1"),
          Arrays.asList("/tmp/kafka-logs0", "/tmp/kafka-logs1"),
          Arrays.asList("/tmp/kafka-logs0", "/tmp/kafka-logs1"),
          Arrays.asList("/tmp/kafka-logs0", null))).
      build();
    try {
      addTopics(adminClient)
      val b = adminClient.brokers()
      adminClient.addTopic(false, "quux", Arrays.asList(
        new TopicPartitionInfo(0, b.get(2),
            Arrays.asList(b.get(1), b.get(2), b.get(3)),
            Arrays.asList(b.get(1), b.get(2), b.get(3)))),
        Collections.emptyMap())
      adminClient.alterReplicaLogDirs(Map(
        new TopicPartitionReplica("foo", 0, 0) -> "/tmp/kafka-logs1",
        new TopicPartitionReplica("quux", 0, 0) -> "/tmp/kafka-logs1"
      ).asJava).all().get()
      assertEquals(Map(
        new TopicPartitionReplica("bar", 0, 0) -> new CompletedMoveState("/tmp/kafka-logs0"),
        new TopicPartitionReplica("foo", 0, 0) -> new ActiveMoveState("/tmp/kafka-logs0",
            "/tmp/kafka-logs1", "/tmp/kafka-logs1"),
        new TopicPartitionReplica("foo", 1, 0) -> new CancelledMoveState("/tmp/kafka-logs0",
          "/tmp/kafka-logs1"),
        new TopicPartitionReplica("quux", 1, 0) -> new MissingLogDirMoveState("/tmp/kafka-logs1"),
        new TopicPartitionReplica("quuz", 0, 0) -> new MissingReplicaMoveState("/tmp/kafka-logs0")
      ), findLogDirMoveStates(adminClient, Map(
        new TopicPartitionReplica("bar", 0, 0) -> "/tmp/kafka-logs0",
        new TopicPartitionReplica("foo", 0, 0) -> "/tmp/kafka-logs1",
        new TopicPartitionReplica("foo", 1, 0) -> "/tmp/kafka-logs1",
        new TopicPartitionReplica("quux", 1, 0) -> "/tmp/kafka-logs1",
        new TopicPartitionReplica("quuz", 0, 0) -> "/tmp/kafka-logs0"
      )))
    } finally {
      adminClient.close()
    }
  }

  @Test
  def testReplicaMoveStatesToString(): Unit = {
    assertEquals(Seq(
      "Reassignment of replica bar-0-0 completed successfully.",
      "Reassignment of replica foo-0-0 is still in progress.",
      "Partition foo-1 on broker 0 is not being moved from log dir /tmp/kafka-logs0 to /tmp/kafka-logs1.",
      "Partition quux-0 cannot be found in any live log directory on broker 0.",
      "Partition quux-1 on broker 1 is being moved to log dir /tmp/kafka-logs2 instead of /tmp/kafka-logs1.",
      "Partition quux-2 is not found in any live log dir on broker 1. " +
          "There is likely an offline log directory on the broker.").mkString(System.lineSeparator()),
        replicaMoveStatesToString(Map(
          new TopicPartitionReplica("bar", 0, 0) -> new CompletedMoveState("/tmp/kafka-logs0"),
          new TopicPartitionReplica("foo", 0, 0) -> new ActiveMoveState("/tmp/kafka-logs0",
            "/tmp/kafka-logs1", "/tmp/kafka-logs1"),
          new TopicPartitionReplica("foo", 1, 0) -> new CancelledMoveState("/tmp/kafka-logs0",
            "/tmp/kafka-logs1"),
          new TopicPartitionReplica("quux", 0, 0) -> new MissingReplicaMoveState("/tmp/kafka-logs1"),
          new TopicPartitionReplica("quux", 1, 1) -> new ActiveMoveState("/tmp/kafka-logs0",
            "/tmp/kafka-logs1", "/tmp/kafka-logs2"),
          new TopicPartitionReplica("quux", 2, 1) -> new MissingLogDirMoveState("/tmp/kafka-logs1")
        )))
  }

  @Test
  def testGetReplicaAssignments(): Unit = {
    val adminClient = new MockAdminClient.Builder().numBrokers(4).build()
    try {
      addTopics(adminClient)
      assertEquals(Map(
          new TopicPartition("foo", 0) -> Seq(0, 1, 2),
          new TopicPartition("foo", 1) -> Seq(1, 2, 3),
        ),
        getReplicaAssignmentForTopics(adminClient, Seq("foo")))
      assertEquals(Map(
          new TopicPartition("foo", 0) -> Seq(0, 1, 2),
          new TopicPartition("bar", 0) -> Seq(2, 3, 0),
        ),
        getReplicaAssignmentForPartitions(adminClient, Set(
          new TopicPartition("foo", 0), new TopicPartition("bar", 0))))
    } finally {
      adminClient.close()
    }
  }

  @Test
  def testGetBrokerRackInformation(): Unit = {
    val adminClient = new MockAdminClient.Builder().
      brokers(Arrays.asList(new Node(0, "locahost", 9092, "rack0"),
        new Node(1, "locahost", 9093, "rack1"),
        new Node(2, "locahost", 9094, null))).
      build()
    try {
      assertEquals(Seq(
        new BrokerMetadata(0, Some("rack0")),
        new BrokerMetadata(1, Some("rack1"))
      ), getBrokerMetadata(adminClient, Seq(0, 1), true))
      assertEquals(Seq(
        new BrokerMetadata(0, None),
        new BrokerMetadata(1, None)
      ), getBrokerMetadata(adminClient, Seq(0, 1), false))
      assertStartsWith("Not all brokers have rack information",
        assertThrows(classOf[AdminOperationException], new ThrowingRunnable {
          override def run(): Unit = getBrokerMetadata(adminClient, Seq(1, 2), true)
        }).getMessage)
      assertEquals(Seq(
        new BrokerMetadata(1, None),
        new BrokerMetadata(2, None)
      ), getBrokerMetadata(adminClient, Seq(1, 2), false))
    } finally {
      adminClient.close()
    }
  }

  @Test
  def testParseGenerateAssignmentArgs(): Unit = {
    assertStartsWith("Broker list contains duplicate entries",
      assertThrows("Expected to detect duplicate broker list entries",
        classOf[AdminCommandFailedException], new ThrowingRunnable {
          override def run():Unit = parseGenerateAssignmentArgs(
            """{"topics": [{"topic": "foo"}], "version":1}""", "1,1,2")
        }).getMessage)
    assertStartsWith("Broker list contains duplicate entries",
      assertThrows("Expected to detect duplicate broker list entries",
        classOf[AdminCommandFailedException], new ThrowingRunnable {
          override def run():Unit = parseGenerateAssignmentArgs(
            """{"topics": [{"topic": "foo"}], "version":1}""", "5,2,3,4,5")
        }).getMessage)
    assertEquals((Seq(5,2,3,4),Seq("foo")),
      parseGenerateAssignmentArgs("""{"topics": [{"topic": "foo"}], "version":1}""",
        "5,2,3,4"))
    assertStartsWith("List of topics to reassign contains duplicate entries",
      assertThrows("Expected to detect duplicate topic entries",
        classOf[AdminCommandFailedException], new ThrowingRunnable {
          override def run():Unit = parseGenerateAssignmentArgs(
            """{"topics": [{"topic": "foo"},{"topic": "foo"}], "version":1}""", "5,2,3,4")
        }).getMessage)
    assertEquals((Seq(5,3,4),Seq("foo","bar")),
      parseGenerateAssignmentArgs(
        """{"topics": [{"topic": "foo"},{"topic": "bar"}], "version":1}""",
        "5,3,4"))
  }

  @Test
  def testGenerateAssignmentFailsWithoutEnoughReplicas(): Unit = {
    val adminClient = new MockAdminClient.Builder().numBrokers(4).build()
    try {
      addTopics(adminClient)
      assertStartsWith("Replication factor: 3 larger than available brokers: 2",
        assertThrows("Expected generateAssignment to fail",
          classOf[InvalidReplicationFactorException], new ThrowingRunnable {
            override def run():Unit = {
              generateAssignment(adminClient,
                """{"topics":[{"topic":"foo"},{"topic":"bar"}]}""", "0,1", false)
            }
          }).getMessage)
    } finally {
      adminClient.close()
    }
  }

  @Test
  def testGenerateAssignmentWithInconsistentRacks(): Unit = {
    val adminClient = new MockAdminClient.Builder().
      brokers(Arrays.asList(
        new Node(0, "locahost", 9092, "rack0"),
        new Node(1, "locahost", 9093, "rack0"),
        new Node(2, "locahost", 9094, null),
        new Node(3, "locahost", 9095, "rack1"),
        new Node(4, "locahost", 9096, "rack1"),
        new Node(5, "locahost", 9097, "rack2"))).
      build()
    try {
      addTopics(adminClient)
      assertStartsWith("Not all brokers have rack information.",
        assertThrows("Expected generateAssignment to fail",
          classOf[AdminOperationException], new ThrowingRunnable {
            override def run():Unit = {
              generateAssignment(adminClient,
                """{"topics":[{"topic":"foo"}]}""", "0,1,2,3", true)
            }
          }).getMessage)
      // It should succeed when --disable-rack-aware is used.
      val (_, current) = generateAssignment(adminClient,
        """{"topics":[{"topic":"foo"}]}""", "0,1,2,3", false)
      assertEquals(Map(
        new TopicPartition("foo", 0) -> Seq(0, 1, 2),
        new TopicPartition("foo", 1) -> Seq(1, 2, 3),
      ), current)
    } finally {
      adminClient.close()
    }
  }

  @Test
  def testGenerateAssignmentWithFewerBrokers(): Unit = {
    val adminClient = new MockAdminClient.Builder().numBrokers(4).build()
    try {
      addTopics(adminClient)
      val goalBrokers = Set(0,1,3)
      val (proposed, current) = generateAssignment(adminClient,
        """{"topics":[{"topic":"foo"},{"topic":"bar"}]}""",
        goalBrokers.mkString(","), false)
      assertEquals(Map(
        new TopicPartition("foo", 0) -> Seq(0, 1, 2),
        new TopicPartition("foo", 1) -> Seq(1, 2, 3),
        new TopicPartition("bar", 0) -> Seq(2, 3, 0)
      ), current)

      // The proposed assignment should only span the provided brokers
      proposed.values.foreach {
        case replicas => {
          if (!replicas.forall(goalBrokers.contains(_))) {
            Assert.fail(s"Proposed assignment ${proposed} puts replicas on brokers " +
              s"other than ${goalBrokers}")
          }
        }
      }
    } finally {
      adminClient.close()
    }
  }

  @Test
  def testCurrentPartitionReplicaAssignmentToString(): Unit = {
    assertEquals(Seq(
        """Current partition replica assignment""",
        """""",
        """{"version":1,"partitions":""" +
          """[{"topic":"bar","partition":0,"replicas":[7,8],"log_dirs":["any","any"]},""" +
          """{"topic":"foo","partition":1,"replicas":[4,5,6],"log_dirs":["any","any","any"]}]""" +
        """}""",
        """""",
        """Save this to use as the --reassignment-json-file option during rollback"""
      ).mkString(System.lineSeparator()),
      currentPartitionReplicaAssignmentToString(Map(
          new TopicPartition("foo", 1) -> Seq(1,2,3),
          new TopicPartition("bar", 0) -> Seq(7,8,9)
        ),
        Map(
          new TopicPartition("foo", 0) -> Seq(1,2,3),
          new TopicPartition("foo", 1) -> Seq(4,5,6),
          new TopicPartition("bar", 0) -> Seq(7,8),
          new TopicPartition("baz", 0) -> Seq(10,11,12)
        ),
      ))
  }

  @Test
  def testMoveMap(): Unit = {
    val moveMap = calculateMoveMap(Map(
      new TopicPartition("foo", 0) -> new PartitionReassignment(
        Arrays.asList(1,2,3),Arrays.asList(4),Arrays.asList(3)),
      new TopicPartition("foo", 1) -> new PartitionReassignment(
        Arrays.asList(4,5,6),Arrays.asList(7, 8),Arrays.asList(4, 5))
    ), Map(
      new TopicPartition("foo", 0) -> Seq(1,2,5),
      new TopicPartition("bar", 0) -> Seq(1,2,3)
    ), Map(
      new TopicPartition("foo", 0) -> Seq(1,2,3),
      new TopicPartition("foo", 1) -> Seq(4,5,6),
      new TopicPartition("bar", 0) -> Seq(2,3,4),
      new TopicPartition("baz", 0) -> Seq(1,2,3)
    ))
    assertEquals(
      mutable.Map("foo" -> mutable.Map(
          0 -> new PartitionMove(mutable.Set(1,2,3), mutable.Set(5)),
          1 -> new PartitionMove(mutable.Set(4,5,6), mutable.Set(7, 8))
        ),
        "bar" -> mutable.Map(
          0 -> new PartitionMove(mutable.Set(2,3,4), mutable.Set(1)),
        )
      ), moveMap)
    assertEquals(Map(
        "foo" -> "0:1,0:2,0:3,1:4,1:5,1:6",
        "bar" -> "0:2,0:3,0:4"
      ), calculateLeaderThrottles(moveMap))
    assertEquals(Map(
        "foo" -> "0:5,1:7,1:8",
        "bar" -> "0:1"
      ), calculateFollowerThrottles(moveMap))
    assertEquals(Set(1,2,3,4,5,6,7,8), calculateReassigningBrokers(moveMap))
    assertEquals(Set(0,2), calculateMovingBrokers(
      Set(new TopicPartitionReplica("quux", 0, 0),
          new TopicPartitionReplica("quux", 1, 2))))
  }

  @Test
  def testParseExecuteAssignmentArgs(): Unit = {
    assertStartsWith("Partition reassignment list cannot be empty",
      assertThrows("Expected to detect empty partition reassignment list",
        classOf[AdminCommandFailedException], new ThrowingRunnable {
          override def run():Unit =
            parseExecuteAssignmentArgs("""{"version":1,"partitions":[]}""")
        }).getMessage)
    assertStartsWith("Partition reassignment contains duplicate topic partitions",
      assertThrows("Expected to detect a partition list with duplicate entries",
        classOf[AdminCommandFailedException], new ThrowingRunnable {
          override def run():Unit =
            parseExecuteAssignmentArgs(
              """{"version":1,"partitions":""" +
                """[{"topic":"foo","partition":0,"replicas":[0,1],"log_dirs":["any","any"]},""" +
                """{"topic":"foo","partition":0,"replicas":[2,3,4],"log_dirs":["any","any","any"]}""" +
                """]}""")
        }).getMessage)
    assertStartsWith("Partition reassignment contains duplicate topic partitions",
      assertThrows("Expected to detect a partition replica list with duplicate entries",
        classOf[AdminCommandFailedException], new ThrowingRunnable {
          override def run():Unit =
            parseExecuteAssignmentArgs(
              """{"version":1,"partitions":""" +
                """[{"topic":"foo","partition":0,"replicas":[0,1],"log_dirs":["/abc","/def"]},""" +
                """{"topic":"foo","partition":0,"replicas":[2,3],"log_dirs":["/abc","/def"]}""" +
                """]}""")
        }).getMessage)
    assertStartsWith("Partition replica lists may not contain duplicate entries",
      assertThrows("Expected to detect a partition replica list with duplicate entries",
        classOf[AdminCommandFailedException], new ThrowingRunnable {
          override def run():Unit =
            parseExecuteAssignmentArgs(
              """{"version":1,"partitions":""" +
                """[{"topic":"foo","partition":0,"replicas":[0,0],"log_dirs":["/abc","/def"]},""" +
                """{"topic":"foo","partition":1,"replicas":[2,3],"log_dirs":["/abc","/def"]}""" +
                """]}""")
        }).getMessage)
    assertEquals((Map(
        new TopicPartition("foo", 0) -> Seq(1, 2, 3),
        new TopicPartition("foo", 1) -> Seq(3, 4, 5),
      ), Map(
      )),
      parseExecuteAssignmentArgs(
        """{"version":1,"partitions":""" +
          """[{"topic":"foo","partition":0,"replicas":[1,2,3],"log_dirs":["any","any","any"]},""" +
          """{"topic":"foo","partition":1,"replicas":[3,4,5],"log_dirs":["any","any","any"]}""" +
          """]}"""))
    assertEquals((Map(
      new TopicPartition("foo", 0) -> Seq(1, 2, 3),
    ), Map(
      new TopicPartitionReplica("foo", 0, 1) -> "/tmp/a",
      new TopicPartitionReplica("foo", 0, 2) -> "/tmp/b",
      new TopicPartitionReplica("foo", 0, 3) -> "/tmp/c"
    )),
      parseExecuteAssignmentArgs(
        """{"version":1,"partitions":""" +
          """[{"topic":"foo","partition":0,"replicas":[1,2,3],"log_dirs":["/tmp/a","/tmp/b","/tmp/c"]}""" +
          """]}"""))
  }

  @Test
  def testExecuteWithInvalidPartitionsFails(): Unit = {
    val adminClient = new MockAdminClient.Builder().numBrokers(5).build()
    try {
      addTopics(adminClient)
      assertStartsWith("Topic quux not found",
        assertThrows("Expected reassignment with non-existent topic to fail",
          classOf[ExecutionException], new ThrowingRunnable {
            override def run():Unit =
              executeAssignment(adminClient, false,
                """{"version":1,"partitions":""" +
                """[{"topic":"foo","partition":0,"replicas":[0,1],"log_dirs":["any","any"]},""" +
                """{"topic":"quux","partition":0,"replicas":[2,3,4],"log_dirs":["any","any","any"]}""" +
                """]}""")
          }).getCause.getMessage)
    } finally {
      adminClient.close()
    }
  }

  @Test
  def testExecuteWithInvalidBrokerIdFails(): Unit = {
    val adminClient = new MockAdminClient.Builder().numBrokers(4).build()
    try {
      addTopics(adminClient)
      assertStartsWith("Unknown broker id 4",
        assertThrows("Expected reassignment with non-existent broker id to fail",
          classOf[AdminCommandFailedException], new ThrowingRunnable {
            override def run():Unit =
              executeAssignment(adminClient, false,
                """{"version":1,"partitions":""" +
                  """[{"topic":"foo","partition":0,"replicas":[0,1],"log_dirs":["any","any"]},""" +
                  """{"topic":"foo","partition":1,"replicas":[2,3,4],"log_dirs":["any","any","any"]}""" +
                  """]}""")
          }).getMessage)
    } finally {
      adminClient.close()
    }
  }

  @Test
  def testModifyBrokerThrottles(): Unit = {
    val adminClient = new MockAdminClient.Builder().numBrokers(4).build()
    try {
      modifyBrokerThrottles(adminClient, Set(0, 1, 2), 1000, Set(0, 1, 2), 2000)
      modifyBrokerThrottles(adminClient, Set(0, 3), 100, Set(0, 3), -1)
      val brokers = Seq(0, 1, 2, 3).map(
        id => new ConfigResource(ConfigResource.Type.BROKER, id.toString))
      val results = adminClient.describeConfigs(brokers.asJava).all().get()
      verifyBrokerThrottleResults(results.get(brokers(0)), 100, 2000)
      verifyBrokerThrottleResults(results.get(brokers(1)), 1000, 2000)
      verifyBrokerThrottleResults(results.get(brokers(2)), 1000, 2000)
      verifyBrokerThrottleResults(results.get(brokers(3)), 100, -1)
    } finally {
      adminClient.close()
    }
  }

  @Test
  def testCurReassignmentsToString(): Unit = {
    val adminClient = new MockAdminClient.Builder().numBrokers(4).build()
    try {
      addTopics(adminClient)
      assertEquals("No partition reassignments found.", curReassignmentsToString(adminClient))
      val reassignmentResult: Map[TopicPartition, Class[_ <: Throwable]] = alterPartitionReassignments(adminClient,
        Map(
          new TopicPartition("foo", 1) -> Seq(4,5,3),
          new TopicPartition("foo", 0) -> Seq(0,1,4,2),
          new TopicPartition("bar", 0) -> Seq(2,3)
        )
      ).map { case (k, v) => k -> v.getClass }.toMap
      assertEquals(Map(), reassignmentResult)
      assertEquals(Seq("Current partition reassignments:",
                       "bar-0: replicas: 2,3,0. removing: 0.",
                       "foo-0: replicas: 0,1,2. adding: 4.",
                       "foo-1: replicas: 1,2,3. adding: 4,5. removing: 1,2.").mkString(System.lineSeparator()),
                  curReassignmentsToString(adminClient))
    } finally {
      adminClient.close()
    }
  }

  private def verifyBrokerThrottleResults(config: Config,
                                          expectedInterBrokerThrottle: Long,
                                          expectedReplicaAlterLogDirsThrottle: Long): Unit = {
    val configs = new mutable.HashMap[String, String]
    config.entries.forEach(entry => configs.put(entry.name, entry.value))
    if (expectedInterBrokerThrottle >= 0) {
      assertEquals(expectedInterBrokerThrottle.toString,
        configs.getOrElse(brokerLevelLeaderThrottle, ""))
      assertEquals(expectedInterBrokerThrottle.toString,
        configs.getOrElse(brokerLevelFollowerThrottle, ""))
    }
    if (expectedReplicaAlterLogDirsThrottle >= 0) {
      assertEquals(expectedReplicaAlterLogDirsThrottle.toString,
        configs.getOrElse(brokerLevelLogDirThrottle, ""))
    }
  }

  @Test
  def testModifyTopicThrottles(): Unit = {
    val adminClient = new MockAdminClient.Builder().numBrokers(4).build()
    try {
      addTopics(adminClient)
      modifyTopicThrottles(adminClient,
        Map("foo" -> "leaderFoo", "bar" -> "leaderBar"),
        Map("bar" -> "followerBar"))
      val topics = Seq("bar", "foo").map(
        id => new ConfigResource(ConfigResource.Type.TOPIC, id.toString))
      val results = adminClient.describeConfigs(topics.asJava).all().get()
      verifyTopicThrottleResults(results.get(topics(0)), "leaderBar", "followerBar")
      verifyTopicThrottleResults(results.get(topics(1)), "leaderFoo", "")
    } finally {
      adminClient.close()
    }
  }

  private def verifyTopicThrottleResults(config: Config,
                                         expectedLeaderThrottle: String,
                                         expectedFollowerThrottle: String): Unit = {
    val configs = new mutable.HashMap[String, String]
    config.entries.forEach(entry => configs.put(entry.name, entry.value))
    assertEquals(expectedLeaderThrottle.toString,
      configs.getOrElse(topicLevelLeaderThrottle, ""))
    assertEquals(expectedFollowerThrottle.toString,
      configs.getOrElse(topicLevelFollowerThrottle, ""))
  }

  @Test
  def testAlterReplicaLogDirs(): Unit = {
    val adminClient = new MockAdminClient.Builder().
      numBrokers(4).
      brokerLogDirs(Collections.nCopies(4,
        Arrays.asList("/tmp/kafka-logs0", "/tmp/kafka-logs1"))).
      build()
    try {
      addTopics(adminClient)
      assertEquals(Set(
          new TopicPartitionReplica("foo", 0, 0)
        ),
        alterReplicaLogDirs(adminClient, Map(
          new TopicPartitionReplica("foo", 0, 0) -> "/tmp/kafka-logs1",
          new TopicPartitionReplica("quux", 1, 0) -> "/tmp/kafka-logs1"
        )))
    } finally {
      adminClient.close()
    }
  }

  def assertStartsWith(prefix: String, str: String): Unit = {
    assertTrue("Expected the string to start with %s, but it was %s".format(prefix, str),
      str.startsWith(prefix))
  }
}
