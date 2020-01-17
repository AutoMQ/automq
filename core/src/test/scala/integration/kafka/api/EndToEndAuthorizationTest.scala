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

package kafka.api

import com.yammer.metrics.Metrics
import com.yammer.metrics.core.Gauge
import java.io.File
import java.util.Collections
import java.util.concurrent.ExecutionException

import kafka.admin.AclCommand
import kafka.security.authorizer.AclAuthorizer
import kafka.security.authorizer.AclEntry.WildcardHost
import kafka.server._
import kafka.utils._
import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig, ConsumerRecords}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.acl._
import org.apache.kafka.common.acl.AclOperation._
import org.apache.kafka.common.acl.AclPermissionType._
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.apache.kafka.common.errors.{GroupAuthorizationException, TopicAuthorizationException}
import org.apache.kafka.common.resource._
import org.apache.kafka.common.resource.ResourceType._
import org.apache.kafka.common.resource.PatternType.{LITERAL, PREFIXED}
import org.junit.Assert._
import org.junit.{After, Before, Test}
import org.scalatest.Assertions.{assertThrows, fail, intercept}

import scala.collection.JavaConverters._

/**
  * The test cases here verify that a producer authorized to publish to a topic
  * is able to, and that consumers in a group authorized to consume are able to
  * to do so.
  *
  * This test relies on a chain of test harness traits to set up. It directly
  * extends IntegrationTestHarness. IntegrationTestHarness creates producers and
  * consumers, and it extends KafkaServerTestHarness. KafkaServerTestHarness starts
  * brokers, but first it initializes a ZooKeeper server and client, which happens
  * in ZooKeeperTestHarness.
  *
  * To start brokers we need to set a cluster ACL, which happens optionally in KafkaServerTestHarness.
  * The remaining ACLs to enable access to producers and consumers are set here. To set ACLs, we use AclCommand directly.
  *
  * Finally, we rely on SaslSetup to bootstrap and setup Kerberos. We don't use
  * SaslTestHarness here directly because it extends ZooKeeperTestHarness, and we
  * would end up with ZooKeeperTestHarness twice.
  */
abstract class EndToEndAuthorizationTest extends IntegrationTestHarness with SaslSetup {
  override val brokerCount = 3

  override def configureSecurityBeforeServersStart(): Unit = {
    AclCommand.main(clusterActionArgs)
    AclCommand.main(topicBrokerReadAclArgs)
  }

  val numRecords = 1
  val groupPrefix = "gr"
  val group = s"${groupPrefix}oup"
  val topicPrefix = "e2e"
  val topic = s"${topicPrefix}topic"
  val wildcard = "*"
  val part = 0
  val tp = new TopicPartition(topic, part)
  val clientPrincipal: String
  val kafkaPrincipal: String

  override protected lazy val trustStoreFile = Some(File.createTempFile("truststore", ".jks"))
  protected def authorizerClass: Class[_] = classOf[AclAuthorizer]

  val topicResource = new ResourcePattern(TOPIC, topic, LITERAL)
  val groupResource =  new ResourcePattern(GROUP, group, LITERAL)
  val clusterResource = new ResourcePattern(CLUSTER, Resource.CLUSTER_NAME, LITERAL)
  val prefixedTopicResource =  new ResourcePattern(TOPIC, topicPrefix, PREFIXED)
  val prefixedGroupResource =  new ResourcePattern(GROUP, groupPrefix, PREFIXED)
  val wildcardTopicResource =  new ResourcePattern(TOPIC, wildcard, LITERAL)
  val wildcardGroupResource =  new ResourcePattern(GROUP, wildcard, LITERAL)
  def kafkaPrincipalStr = s"$kafkaPrincipalType:$kafkaPrincipal"
  def clientPrincipalStr = s"$kafkaPrincipalType:$clientPrincipal"

  // Arguments to AclCommand to set ACLs.
  def clusterActionArgs: Array[String] = Array("--authorizer-properties",
                                          s"zookeeper.connect=$zkConnect",
                                          s"--add",
                                          s"--cluster",
                                          s"--operation=ClusterAction",
                                          s"--allow-principal=$kafkaPrincipalStr")
  def topicBrokerReadAclArgs: Array[String] = Array("--authorizer-properties",
                                          s"zookeeper.connect=$zkConnect",
                                          s"--add",
                                          s"--topic=$wildcard",
                                          s"--operation=Read",
                                          s"--allow-principal=$kafkaPrincipalStr")
  def produceAclArgs(topic: String): Array[String] = Array("--authorizer-properties",
                                          s"zookeeper.connect=$zkConnect",
                                          s"--add",
                                          s"--topic=$topic",
                                          s"--producer",
                                          s"--allow-principal=$clientPrincipalStr")
  def describeAclArgs: Array[String] = Array("--authorizer-properties",
                                          s"zookeeper.connect=$zkConnect",
                                          s"--add",
                                          s"--topic=$topic",
                                          s"--operation=Describe",
                                          s"--allow-principal=$clientPrincipalStr")
  def deleteDescribeAclArgs: Array[String] = Array("--authorizer-properties",
                                          s"zookeeper.connect=$zkConnect",
                                          s"--remove",
                                          s"--force",
                                          s"--topic=$topic",
                                          s"--operation=Describe",
                                          s"--allow-principal=$clientPrincipalStr")
  def deleteWriteAclArgs: Array[String] = Array("--authorizer-properties",
                                          s"zookeeper.connect=$zkConnect",
                                          s"--remove",
                                          s"--force",
                                          s"--topic=$topic",
                                          s"--operation=Write",
                                          s"--allow-principal=$clientPrincipalStr")
  def consumeAclArgs(topic: String): Array[String] = Array("--authorizer-properties",
                                          s"zookeeper.connect=$zkConnect",
                                          s"--add",
                                          s"--topic=$topic",
                                          s"--group=$group",
                                          s"--consumer",
                                          s"--allow-principal=$clientPrincipalStr")
  def groupAclArgs: Array[String] = Array("--authorizer-properties",
                                          s"zookeeper.connect=$zkConnect",
                                          s"--add",
                                          s"--group=$group",
                                          s"--operation=Read",
                                          s"--allow-principal=$clientPrincipalStr")
  def produceConsumeWildcardAclArgs: Array[String] = Array("--authorizer-properties",
                                          s"zookeeper.connect=$zkConnect",
                                          s"--add",
                                          s"--topic=$wildcard",
                                          s"--group=$wildcard",
                                          s"--consumer",
                                          s"--producer",
                                          s"--allow-principal=$clientPrincipalStr")
  def produceConsumePrefixedAclsArgs: Array[String] = Array("--authorizer-properties",
                                          s"zookeeper.connect=$zkConnect",
                                          s"--add",
                                          s"--topic=$topicPrefix",
                                          s"--group=$groupPrefix",
                                          s"--resource-pattern-type=prefixed",
                                          s"--consumer",
                                          s"--producer",
                                          s"--allow-principal=$clientPrincipalStr")

  def ClusterActionAcl = Set(new AccessControlEntry(kafkaPrincipalStr, WildcardHost, CLUSTER_ACTION, ALLOW))
  def TopicBrokerReadAcl = Set(new AccessControlEntry(kafkaPrincipalStr, WildcardHost, READ, ALLOW))
  def GroupReadAcl = Set(new AccessControlEntry(clientPrincipalStr, WildcardHost, READ, ALLOW))
  def TopicReadAcl = Set(new AccessControlEntry(clientPrincipalStr, WildcardHost, READ, ALLOW))
  def TopicWriteAcl = Set(new AccessControlEntry(clientPrincipalStr, WildcardHost, WRITE, ALLOW))
  def TopicDescribeAcl = Set(new AccessControlEntry(clientPrincipalStr, WildcardHost, DESCRIBE, ALLOW))
  def TopicCreateAcl = Set(new AccessControlEntry(clientPrincipalStr, WildcardHost, CREATE, ALLOW))
  // The next two configuration parameters enable ZooKeeper secure ACLs
  // and sets the Kafka authorizer, both necessary to enable security.
  this.serverConfig.setProperty(KafkaConfig.ZkEnableSecureAclsProp, "true")
  this.serverConfig.setProperty(KafkaConfig.AuthorizerClassNameProp, authorizerClass.getName)
  // Some needed configuration for brokers, producers, and consumers
  this.serverConfig.setProperty(KafkaConfig.OffsetsTopicPartitionsProp, "1")
  this.serverConfig.setProperty(KafkaConfig.OffsetsTopicReplicationFactorProp, "3")
  this.serverConfig.setProperty(KafkaConfig.MinInSyncReplicasProp, "3")
  this.serverConfig.setProperty(KafkaConfig.DefaultReplicationFactorProp, "3")
  this.serverConfig.setProperty(KafkaConfig.ConnectionsMaxReauthMsProp, "1500")
  this.consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group")
  this.consumerConfig.setProperty(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "1500")

  /**
    * Starts MiniKDC and only then sets up the parent trait.
    */
  @Before
  override def setUp(): Unit = {
    super.setUp()
    servers.foreach { s =>
      TestUtils.waitAndVerifyAcls(ClusterActionAcl, s.dataPlaneRequestProcessor.authorizer.get, clusterResource)
      TestUtils.waitAndVerifyAcls(TopicBrokerReadAcl, s.dataPlaneRequestProcessor.authorizer.get, new ResourcePattern(TOPIC, "*", LITERAL))
    }
    // create the test topic with all the brokers as replicas
    createTopic(topic, 1, 3)
  }

  /**
    * Closes MiniKDC last when tearing down.
    */
  @After
  override def tearDown(): Unit = {
    super.tearDown()
    closeSasl()
  }

  /**
    * Tests the ability of producing and consuming with the appropriate ACLs set.
    */
  @Test
  def testProduceConsumeViaAssign(): Unit = {
    setAclsAndProduce(tp)
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    consumeRecords(consumer, numRecords)
    confirmReauthenticationMetrics()
  }

  protected def confirmReauthenticationMetrics(): Unit = {
    val expiredConnectionsKilledCountTotal = getGauge("ExpiredConnectionsKilledCount").value()
    servers.foreach { s =>
        val numExpiredKilled = TestUtils.totalMetricValue(s, "expired-connections-killed-count")
        assertTrue("Should have been zero expired connections killed: " + numExpiredKilled + "(total=" + expiredConnectionsKilledCountTotal + ")", numExpiredKilled == 0)
    }
    assertEquals("Should have been zero expired connections killed total", 0, expiredConnectionsKilledCountTotal, 0.0)
    servers.foreach { s =>
      assertTrue("failed re-authentications not 0", TestUtils.totalMetricValue(s, "failed-reauthentication-total") == 0)
    }
  }

  private def getGauge(metricName: String) = {
    Metrics.defaultRegistry.allMetrics.asScala
           .filterKeys(k => k.getName == metricName)
           .headOption
           .getOrElse { fail( "Unable to find metric " + metricName ) }
           ._2.asInstanceOf[Gauge[Double]]
  }

  @Test
  def testProduceConsumeViaSubscribe(): Unit = {
    setAclsAndProduce(tp)
    val consumer = createConsumer()
    consumer.subscribe(List(topic).asJava)
    consumeRecords(consumer, numRecords)
    confirmReauthenticationMetrics()
  }

  @Test
  def testProduceConsumeWithWildcardAcls(): Unit = {
    setWildcardResourceAcls()
    val producer = createProducer()
    sendRecords(producer, numRecords, tp)
    val consumer = createConsumer()
    consumer.subscribe(List(topic).asJava)
    consumeRecords(consumer, numRecords)
    confirmReauthenticationMetrics()
  }

  @Test
  def testProduceConsumeWithPrefixedAcls(): Unit = {
    setPrefixedResourceAcls()
    val producer = createProducer()
    sendRecords(producer, numRecords, tp)
    val consumer = createConsumer()
    consumer.subscribe(List(topic).asJava)
    consumeRecords(consumer, numRecords)
    confirmReauthenticationMetrics()
  }

  @Test
  def testProduceConsumeTopicAutoCreateTopicCreateAcl(): Unit = {
    // topic2 is not created on setup()
    val tp2 = new TopicPartition("topic2", 0)
    setAclsAndProduce(tp2)
    val consumer = createConsumer()
    consumer.assign(List(tp2).asJava)
    consumeRecords(consumer, numRecords, topic = tp2.topic)
    confirmReauthenticationMetrics()
  }

  private def setWildcardResourceAcls(): Unit = {
    AclCommand.main(produceConsumeWildcardAclArgs)
    servers.foreach { s =>
      TestUtils.waitAndVerifyAcls(TopicReadAcl ++ TopicWriteAcl ++ TopicDescribeAcl ++ TopicCreateAcl ++ TopicBrokerReadAcl, s.dataPlaneRequestProcessor.authorizer.get, wildcardTopicResource)
      TestUtils.waitAndVerifyAcls(GroupReadAcl, s.dataPlaneRequestProcessor.authorizer.get, wildcardGroupResource)
    }
  }

  private def setPrefixedResourceAcls(): Unit = {
    AclCommand.main(produceConsumePrefixedAclsArgs)
    servers.foreach { s =>
      TestUtils.waitAndVerifyAcls(TopicReadAcl ++ TopicWriteAcl ++ TopicDescribeAcl ++ TopicCreateAcl, s.dataPlaneRequestProcessor.authorizer.get, prefixedTopicResource)
      TestUtils.waitAndVerifyAcls(GroupReadAcl, s.dataPlaneRequestProcessor.authorizer.get, prefixedGroupResource)
    }
  }

  private def setReadAndWriteAcls(tp: TopicPartition): Unit = {
    AclCommand.main(produceAclArgs(tp.topic))
    AclCommand.main(consumeAclArgs(tp.topic))
    servers.foreach { s =>
      TestUtils.waitAndVerifyAcls(TopicReadAcl ++ TopicWriteAcl ++ TopicDescribeAcl ++ TopicCreateAcl, s.dataPlaneRequestProcessor.authorizer.get,
        new ResourcePattern(TOPIC, tp.topic, LITERAL))
      TestUtils.waitAndVerifyAcls(GroupReadAcl, s.dataPlaneRequestProcessor.authorizer.get, groupResource)
    }
  }

  protected def setAclsAndProduce(tp: TopicPartition): Unit = {
    setReadAndWriteAcls(tp)
    val producer = createProducer()
    sendRecords(producer, numRecords, tp)
  }

  private def setConsumerGroupAcls(): Unit = {
    AclCommand.main(groupAclArgs)
    servers.foreach { s =>
      TestUtils.waitAndVerifyAcls(GroupReadAcl, s.dataPlaneRequestProcessor.authorizer.get, groupResource)
    }
  }

  /**
    * Tests that producer, consumer and adminClient fail to publish messages, consume
    * messages and describe topics respectively when the describe ACL isn't set.
    * Also verifies that subsequent publish, consume and describe to authorized topic succeeds.
    */
  @Test
  def testNoDescribeProduceOrConsumeWithoutTopicDescribeAcl(): Unit = {
    // Set consumer group acls since we are testing topic authorization
    setConsumerGroupAcls()

    // Verify produce/consume/describe throw TopicAuthorizationException
    val producer = createProducer()
    assertThrows[TopicAuthorizationException] { sendRecords(producer, numRecords, tp) }
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    assertThrows[TopicAuthorizationException] { consumeRecords(consumer, numRecords, topic = tp.topic) }
    val adminClient = createAdminClient()
    val e1 = intercept[ExecutionException] { adminClient.describeTopics(Set(topic).asJava).all().get() }
    assertTrue("Unexpected exception " + e1.getCause, e1.getCause.isInstanceOf[TopicAuthorizationException])

    // Verify successful produce/consume/describe on another topic using the same producer, consumer and adminClient
    val topic2 = "topic2"
    val tp2 = new TopicPartition(topic2, 0)
    setReadAndWriteAcls(tp2)
    sendRecords(producer, numRecords, tp2)
    consumer.assign(List(tp2).asJava)
    consumeRecords(consumer, numRecords, topic = topic2)
    val describeResults = adminClient.describeTopics(Set(topic, topic2).asJava).values
    assertEquals(1, describeResults.get(topic2).get().partitions().size())
    val e2 = intercept[ExecutionException] { adminClient.describeTopics(Set(topic).asJava).all().get() }
    assertTrue("Unexpected exception " + e2.getCause, e2.getCause.isInstanceOf[TopicAuthorizationException])

    // Verify that consumer manually assigning both authorized and unauthorized topic doesn't consume
    // from the unauthorized topic and throw; since we can now return data during the time we are updating
    // metadata / fetching positions, it is possible that the authorized topic record is returned during this time.
    consumer.assign(List(tp, tp2).asJava)
    sendRecords(producer, numRecords, tp2)
    var topic2RecordConsumed = false
    def verifyNoRecords(records: ConsumerRecords[Array[Byte], Array[Byte]]): Boolean = {
      assertEquals("Consumed records with unexpected partitions: " + records, Collections.singleton(tp2), records.partitions())
      topic2RecordConsumed = true
      false
    }
    assertThrows[TopicAuthorizationException] {
      TestUtils.pollRecordsUntilTrue(consumer, verifyNoRecords, "Consumer didn't fail with authorization exception within timeout")
    }

    // Add ACLs and verify successful produce/consume/describe on first topic
    setReadAndWriteAcls(tp)
    if (!topic2RecordConsumed) {
      consumeRecordsIgnoreOneAuthorizationException(consumer, numRecords, startingOffset = 1, topic2)
    }
    sendRecords(producer, numRecords, tp)
    consumeRecordsIgnoreOneAuthorizationException(consumer, numRecords, startingOffset = 0, topic)
    val describeResults2 = adminClient.describeTopics(Set(topic, topic2).asJava).values
    assertEquals(1, describeResults2.get(topic).get().partitions().size())
    assertEquals(1, describeResults2.get(topic2).get().partitions().size())
  }

  @Test
  def testNoProduceWithDescribeAcl(): Unit = {
    AclCommand.main(describeAclArgs)
    servers.foreach { s =>
      TestUtils.waitAndVerifyAcls(TopicDescribeAcl, s.dataPlaneRequestProcessor.authorizer.get, topicResource)
    }
    try{
      val producer = createProducer()
      sendRecords(producer, numRecords, tp)
      fail("exception expected")
    } catch {
      case e: TopicAuthorizationException =>
        assertEquals(Set(topic).asJava, e.unauthorizedTopics())
    }
    confirmReauthenticationMetrics()
  }

   /**
    * Tests that a consumer fails to consume messages without the appropriate
    * ACL set.
    */
  @Test(expected = classOf[KafkaException])
  def testNoConsumeWithoutDescribeAclViaAssign(): Unit = {
    noConsumeWithoutDescribeAclSetup()
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    // the exception is expected when the consumer attempts to lookup offsets
    consumeRecords(consumer)
    confirmReauthenticationMetrics()
  }

  @Test
  def testNoConsumeWithoutDescribeAclViaSubscribe(): Unit = {
    noConsumeWithoutDescribeAclSetup()
    val consumer = createConsumer()
    consumer.subscribe(List(topic).asJava)
    // this should timeout since the consumer will not be able to fetch any metadata for the topic
    assertThrows[TopicAuthorizationException] { consumeRecords(consumer, timeout = 3000) }

    // Verify that no records are consumed even if one of the requested topics is authorized
    setReadAndWriteAcls(tp)
    consumer.subscribe(List(topic, "topic2").asJava)
    assertThrows[TopicAuthorizationException] { consumeRecords(consumer, timeout = 3000) }

    // Verify that records are consumed if all topics are authorized
    consumer.subscribe(List(topic).asJava)
    consumeRecordsIgnoreOneAuthorizationException(consumer)
  }

  private def noConsumeWithoutDescribeAclSetup(): Unit = {
    AclCommand.main(produceAclArgs(tp.topic))
    AclCommand.main(groupAclArgs)
    servers.foreach { s =>
      TestUtils.waitAndVerifyAcls(TopicWriteAcl ++ TopicDescribeAcl ++ TopicCreateAcl, s.dataPlaneRequestProcessor.authorizer.get, topicResource)
      TestUtils.waitAndVerifyAcls(GroupReadAcl, s.dataPlaneRequestProcessor.authorizer.get, groupResource)
    }

    val producer = createProducer()
    sendRecords(producer, numRecords, tp)

    AclCommand.main(deleteDescribeAclArgs)
    AclCommand.main(deleteWriteAclArgs)
    servers.foreach { s =>
      TestUtils.waitAndVerifyAcls(GroupReadAcl, s.dataPlaneRequestProcessor.authorizer.get, groupResource)
    }
  }

  @Test
  def testNoConsumeWithDescribeAclViaAssign(): Unit = {
    noConsumeWithDescribeAclSetup()
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)

    try {
      consumeRecords(consumer)
      fail("Topic authorization exception expected")
    } catch {
      case e: TopicAuthorizationException =>
        assertEquals(Set(topic).asJava, e.unauthorizedTopics())
    }
    confirmReauthenticationMetrics()
  }

  @Test
  def testNoConsumeWithDescribeAclViaSubscribe(): Unit = {
    noConsumeWithDescribeAclSetup()
    val consumer = createConsumer()
    consumer.subscribe(List(topic).asJava)

    try {
      consumeRecords(consumer)
      fail("Topic authorization exception expected")
    } catch {
      case e: TopicAuthorizationException =>
        assertEquals(Set(topic).asJava, e.unauthorizedTopics())
    }
    confirmReauthenticationMetrics()
  }

  private def noConsumeWithDescribeAclSetup(): Unit = {
    AclCommand.main(produceAclArgs(tp.topic))
    AclCommand.main(groupAclArgs)
    servers.foreach { s =>
      TestUtils.waitAndVerifyAcls(TopicWriteAcl ++ TopicDescribeAcl ++ TopicCreateAcl, s.dataPlaneRequestProcessor.authorizer.get, topicResource)
      TestUtils.waitAndVerifyAcls(GroupReadAcl, s.dataPlaneRequestProcessor.authorizer.get, groupResource)
    }
    val producer = createProducer()
    sendRecords(producer, numRecords, tp)
  }

  /**
    * Tests that a consumer fails to consume messages without the appropriate
    * ACL set.
    */
  @Test
  def testNoGroupAcl(): Unit = {
    AclCommand.main(produceAclArgs(tp.topic))
    servers.foreach { s =>
      TestUtils.waitAndVerifyAcls(TopicWriteAcl ++ TopicDescribeAcl ++ TopicCreateAcl, s.dataPlaneRequestProcessor.authorizer.get, topicResource)
    }
    val producer = createProducer()
    sendRecords(producer, numRecords, tp)

    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    try {
      consumeRecords(consumer)
      fail("Topic authorization exception expected")
    } catch {
      case e: GroupAuthorizationException =>
        assertEquals(group, e.groupId())
    }
    confirmReauthenticationMetrics()
  }

  protected final def sendRecords(producer: KafkaProducer[Array[Byte], Array[Byte]],
                                  numRecords: Int, tp: TopicPartition): Unit = {
    val futures = (0 until numRecords).map { i =>
      val record = new ProducerRecord(tp.topic(), tp.partition(), s"$i".getBytes, s"$i".getBytes)
      debug(s"Sending this record: $record")
      producer.send(record)
    }
    try {
      futures.foreach(_.get)
    } catch {
      case e: ExecutionException => throw e.getCause
    }
  }

  protected final def consumeRecords(consumer: Consumer[Array[Byte], Array[Byte]],
                                     numRecords: Int = 1,
                                     startingOffset: Int = 0,
                                     topic: String = topic,
                                     part: Int = part,
                                     timeout: Long = 10000): Unit = {
    val records = TestUtils.consumeRecords(consumer, numRecords, timeout)

    for (i <- 0 until numRecords) {
      val record = records(i)
      val offset = startingOffset + i
      assertEquals(topic, record.topic)
      assertEquals(part, record.partition)
      assertEquals(offset.toLong, record.offset)
    }
  }

  // Consume records, ignoring at most one TopicAuthorization exception from previously sent request
  private def consumeRecordsIgnoreOneAuthorizationException(consumer: Consumer[Array[Byte], Array[Byte]],
                                                            numRecords: Int = 1,
                                                            startingOffset: Int = 0,
                                                            topic: String = topic): Unit = {
    try {
      consumeRecords(consumer, numRecords, startingOffset, topic)
    } catch {
      case _: TopicAuthorizationException => consumeRecords(consumer, numRecords, startingOffset, topic)
    }
  }
}

