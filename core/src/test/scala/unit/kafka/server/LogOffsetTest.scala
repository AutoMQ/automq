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

package kafka.server

import kafka.log.{ClientRecordDeletion, LogSegment, UnifiedLog}
import kafka.utils.{MockTime, TestUtils}
import org.apache.kafka.common.message.ListOffsetsRequestData.{ListOffsetsPartition, ListOffsetsTopic}
import org.apache.kafka.common.message.ListOffsetsResponseData.{ListOffsetsPartitionResponse, ListOffsetsTopicResponse}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{FetchRequest, FetchResponse, ListOffsetsRequest, ListOffsetsResponse}
import org.apache.kafka.common.{IsolationLevel, TopicPartition}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.Mockito.{mock, when}

import java.io.File
import java.util.concurrent.atomic.AtomicInteger
import java.util.{Optional, Properties, Random}
import scala.collection.mutable.Buffer
import scala.jdk.CollectionConverters._

class LogOffsetTest extends BaseRequestTest {

  private lazy val time = new MockTime

  override def brokerCount = 1

  protected override def brokerTime(brokerId: Int) = time

  protected override def brokerPropertyOverrides(props: Properties): Unit = {
    props.put("log.flush.interval.messages", "1")
    props.put("num.partitions", "20")
    props.put("log.retention.hours", "10")
    props.put("log.retention.check.interval.ms", (5 * 1000 * 60).toString)
    props.put("log.segment.bytes", "140")
  }

  @deprecated("ListOffsetsRequest V0", since = "")
  @Test
  def testGetOffsetsForUnknownTopic(): Unit = {
    val topicPartition = new TopicPartition("foo", 0)
    val request = ListOffsetsRequest.Builder.forConsumer(false, IsolationLevel.READ_UNCOMMITTED, false)
      .setTargetTimes(buildTargetTimes(topicPartition, ListOffsetsRequest.LATEST_TIMESTAMP, 10).asJava).build(0)
    val response = sendListOffsetsRequest(request)
    assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION.code, findPartition(response.topics.asScala, topicPartition).errorCode)
  }

  @deprecated("ListOffsetsRequest V0", since = "")
  @Test
  def testGetOffsetsAfterDeleteRecords(): Unit = {
    val topic = "kafka-"
    val topicPartition = new TopicPartition(topic, 0)
    val log = createTopicAndGetLog(topic, topicPartition)

    for (_ <- 0 until 20)
      log.appendAsLeader(TestUtils.singletonRecords(value = Integer.toString(42).getBytes()), leaderEpoch = 0)
    log.flush(false)

    log.updateHighWatermark(log.logEndOffset)
    log.maybeIncrementLogStartOffset(3, ClientRecordDeletion)
    log.deleteOldSegments()

    val offsets = log.legacyFetchOffsetsBefore(ListOffsetsRequest.LATEST_TIMESTAMP, 15)
    assertEquals(Seq(20L, 18L, 16L, 14L, 12L, 10L, 8L, 6L, 4L, 3L), offsets)

    TestUtils.waitUntilTrue(() => TestUtils.isLeaderLocalOnBroker(topic, topicPartition.partition, server),
      "Leader should be elected")
    val request = ListOffsetsRequest.Builder.forReplica(0, 0)
      .setTargetTimes(buildTargetTimes(topicPartition, ListOffsetsRequest.LATEST_TIMESTAMP, 15).asJava).build()
    val consumerOffsets = findPartition(sendListOffsetsRequest(request).topics.asScala, topicPartition).oldStyleOffsets.asScala
    assertEquals(Seq(20L, 18L, 16L, 14L, 12L, 10L, 8L, 6L, 4L, 3L), consumerOffsets)
  }

  @Test
  def testFetchOffsetByTimestampForMaxTimestampAfterTruncate(): Unit = {
    val topic = "kafka-"
    val topicPartition = new TopicPartition(topic, 0)
    val log = createTopicAndGetLog(topic, topicPartition)

    for (timestamp <- 0 until 20)
      log.appendAsLeader(TestUtils.singletonRecords(value = Integer.toString(42).getBytes(), timestamp = timestamp.toLong), leaderEpoch = 0)
    log.flush(false)

    log.updateHighWatermark(log.logEndOffset)

    val firstOffset = log.fetchOffsetByTimestamp(ListOffsetsRequest.MAX_TIMESTAMP)
    assertEquals(19L, firstOffset.get.offset)
    assertEquals(19L, firstOffset.get.timestamp)

    log.truncateTo(0)

    val secondOffset = log.fetchOffsetByTimestamp(ListOffsetsRequest.MAX_TIMESTAMP)
    assertEquals(0L, secondOffset.get.offset)
    assertEquals(-1L, secondOffset.get.timestamp)
  }

  @Test
  def testFetchOffsetByTimestampForMaxTimestampWithUnorderedTimestamps(): Unit = {
    val topic = "kafka-"
    val topicPartition = new TopicPartition(topic, 0)
    val log = createTopicAndGetLog(topic, topicPartition)

    for (timestamp <- List(0L, 1L, 2L, 3L, 4L, 6L, 5L))
      log.appendAsLeader(TestUtils.singletonRecords(value = Integer.toString(42).getBytes(), timestamp = timestamp), leaderEpoch = 0)
    log.flush(false)

    log.updateHighWatermark(log.logEndOffset)

    val maxTimestampOffset = log.fetchOffsetByTimestamp(ListOffsetsRequest.MAX_TIMESTAMP)
    assertEquals(7L, log.logEndOffset)
    assertEquals(5L, maxTimestampOffset.get.offset)
    assertEquals(6L, maxTimestampOffset.get.timestamp)
  }

  @Test
  def testGetOffsetsBeforeLatestTime(): Unit = {
    val topic = "kafka-"
    val topicPartition = new TopicPartition(topic, 0)
    val log = createTopicAndGetLog(topic, topicPartition)

    val topicIds = getTopicIds().asJava
    val topicNames = topicIds.asScala.map(_.swap).asJava
    val topicId = topicIds.get(topic)

    for (_ <- 0 until 20)
      log.appendAsLeader(TestUtils.singletonRecords(value = Integer.toString(42).getBytes()), leaderEpoch = 0)
    log.flush(false)

    val offsets = log.legacyFetchOffsetsBefore(ListOffsetsRequest.LATEST_TIMESTAMP, 15)
    assertEquals(Seq(20L, 18L, 16L, 14L, 12L, 10L, 8L, 6L, 4L, 2L, 0L), offsets)

    TestUtils.waitUntilTrue(() => TestUtils.isLeaderLocalOnBroker(topic, 0, server),
      "Leader should be elected")
    val request = ListOffsetsRequest.Builder.forReplica(0, 0)
      .setTargetTimes(buildTargetTimes(topicPartition, ListOffsetsRequest.LATEST_TIMESTAMP, 15).asJava).build()
    val consumerOffsets = findPartition(sendListOffsetsRequest(request).topics.asScala, topicPartition).oldStyleOffsets.asScala
    assertEquals(Seq(20L, 18L, 16L, 14L, 12L, 10L, 8L, 6L, 4L, 2L, 0L), consumerOffsets)

    // try to fetch using latest offset
    val fetchRequest = FetchRequest.Builder.forConsumer(ApiKeys.FETCH.latestVersion, 0, 1,
      Map(topicPartition -> new FetchRequest.PartitionData(topicId, consumerOffsets.head, FetchRequest.INVALID_LOG_START_OFFSET,
        300 * 1024, Optional.empty())).asJava).build()
    val fetchResponse = sendFetchRequest(fetchRequest)
    assertFalse(FetchResponse.recordsOrFail(fetchResponse.responseData(topicNames, ApiKeys.FETCH.latestVersion).get(topicPartition)).batches.iterator.hasNext)
  }

  @Test
  def testEmptyLogsGetOffsets(): Unit = {
    val random = new Random
    val topic = "kafka-"
    val topicPartition = new TopicPartition(topic, random.nextInt(10))
    val topicPartitionPath = s"${TestUtils.tempDir().getAbsolutePath}/$topic-${topicPartition.partition}"
    val topicLogDir = new File(topicPartitionPath)
    topicLogDir.mkdir()

    createTopic(topic, numPartitions = 1, replicationFactor = 1)

    var offsetChanged = false
    for (_ <- 1 to 14) {
      val topicPartition = new TopicPartition(topic, 0)
      val request = ListOffsetsRequest.Builder.forReplica(0, 0)
        .setTargetTimes(buildTargetTimes(topicPartition, ListOffsetsRequest.EARLIEST_TIMESTAMP, 1).asJava).build()
      val consumerOffsets = findPartition(sendListOffsetsRequest(request).topics.asScala, topicPartition).oldStyleOffsets.asScala
      if (consumerOffsets.head == 1)
        offsetChanged = true
    }
    assertFalse(offsetChanged)
  }

  @Test
  def testFetchOffsetByTimestampForMaxTimestampWithEmptyLog(): Unit = {
    val topic = "kafka-"
    val topicPartition = new TopicPartition(topic, 0)
    val log = createTopicAndGetLog(topic, topicPartition)

    log.updateHighWatermark(log.logEndOffset)

    val maxTimestampOffset = log.fetchOffsetByTimestamp(ListOffsetsRequest.MAX_TIMESTAMP)
    assertEquals(0L, log.logEndOffset)
    assertEquals(0L, maxTimestampOffset.get.offset)
    assertEquals(-1L, maxTimestampOffset.get.timestamp)
  }

  @deprecated("legacyFetchOffsetsBefore", since = "")
  @Test
  def testGetOffsetsBeforeNow(): Unit = {
    val random = new Random
    val topic = "kafka-"
    val topicPartition = new TopicPartition(topic, random.nextInt(3))

    createTopic(topic, 3, 1)

    val logManager = server.getLogManager
    val log = logManager.getOrCreateLog(topicPartition, topicId = None)

    for (_ <- 0 until 20)
      log.appendAsLeader(TestUtils.singletonRecords(value = Integer.toString(42).getBytes()), leaderEpoch = 0)
    log.flush(false)

    val now = time.milliseconds + 30000 // pretend it is the future to avoid race conditions with the fs

    val offsets = log.legacyFetchOffsetsBefore(now, 15)
    assertEquals(Seq(20L, 18L, 16L, 14L, 12L, 10L, 8L, 6L, 4L, 2L, 0L), offsets)

    TestUtils.waitUntilTrue(() => TestUtils.isLeaderLocalOnBroker(topic, topicPartition.partition, server),
      "Leader should be elected")
    val request = ListOffsetsRequest.Builder.forReplica(0, 0)
      .setTargetTimes(buildTargetTimes(topicPartition, now, 15).asJava).build()
    val consumerOffsets = findPartition(sendListOffsetsRequest(request).topics.asScala, topicPartition).oldStyleOffsets.asScala
    assertEquals(Seq(20L, 18L, 16L, 14L, 12L, 10L, 8L, 6L, 4L, 2L, 0L), consumerOffsets)
  }

  @deprecated("legacyFetchOffsetsBefore", since = "")
  @Test
  def testGetOffsetsBeforeEarliestTime(): Unit = {
    val random = new Random
    val topic = "kafka-"
    val topicPartition = new TopicPartition(topic, random.nextInt(3))

    createTopic(topic, 3, 1)

    val logManager = server.getLogManager
    val log = logManager.getOrCreateLog(topicPartition, topicId = None)
    for (_ <- 0 until 20)
      log.appendAsLeader(TestUtils.singletonRecords(value = Integer.toString(42).getBytes()), leaderEpoch = 0)
    log.flush(false)

    val offsets = log.legacyFetchOffsetsBefore(ListOffsetsRequest.EARLIEST_TIMESTAMP, 10)

    assertEquals(Seq(0L), offsets)

    TestUtils.waitUntilTrue(() => TestUtils.isLeaderLocalOnBroker(topic, topicPartition.partition, server),
      "Leader should be elected")
    val request = ListOffsetsRequest.Builder.forReplica(0, 0)
      .setTargetTimes(buildTargetTimes(topicPartition, ListOffsetsRequest.EARLIEST_TIMESTAMP, 10).asJava).build()
    val consumerOffsets = findPartition(sendListOffsetsRequest(request).topics.asScala, topicPartition).oldStyleOffsets.asScala
    assertEquals(Seq(0L), consumerOffsets)
  }

  /* We test that `fetchOffsetsBefore` works correctly if `LogSegment.size` changes after each invocation (simulating
   * a race condition) */
  @Test
  def testFetchOffsetsBeforeWithChangingSegmentSize(): Unit = {
    val log: UnifiedLog = mock(classOf[UnifiedLog])
    val logSegment: LogSegment = mock(classOf[LogSegment])
    when(logSegment.size).thenAnswer(_ => {
      val value = new AtomicInteger(0)
      def answer: Int = value.getAndIncrement()
      answer
    })
    val logSegments = Seq(logSegment)
    when(log.logSegments).thenReturn(logSegments)
    log.legacyFetchOffsetsBefore(System.currentTimeMillis, 100)
  }

  /* We test that `fetchOffsetsBefore` works correctly if `Log.logSegments` content and size are
   * different (simulating a race condition) */
  @Test
  def testFetchOffsetsBeforeWithChangingSegments(): Unit = {
    val log: UnifiedLog = mock(classOf[UnifiedLog])
    val logSegment: LogSegment = mock(classOf[LogSegment])
    when(log.logSegments).thenAnswer { _ =>
      def answer = new Iterable[LogSegment] {
        override def size = 2
        def iterator = Seq(logSegment).iterator
      }
      answer
    }
    log.legacyFetchOffsetsBefore(System.currentTimeMillis, 100)
  }

  private def server: KafkaServer = servers.head

  private def sendListOffsetsRequest(request: ListOffsetsRequest): ListOffsetsResponse = {
    connectAndReceive[ListOffsetsResponse](request)
  }

  private def sendFetchRequest(request: FetchRequest): FetchResponse = {
    connectAndReceive[FetchResponse](request)
  }

  private def buildTargetTimes(tp: TopicPartition, timestamp: Long, maxNumOffsets: Int): List[ListOffsetsTopic] = {
    List(new ListOffsetsTopic()
      .setName(tp.topic)
      .setPartitions(List(new ListOffsetsPartition()
        .setPartitionIndex(tp.partition)
        .setTimestamp(timestamp)
        .setMaxNumOffsets(maxNumOffsets)).asJava)
    )
  }

  private def findPartition(topics: Buffer[ListOffsetsTopicResponse], tp: TopicPartition): ListOffsetsPartitionResponse = {
    topics.find(_.name == tp.topic).get
      .partitions.asScala.find(_.partitionIndex == tp.partition).get
  }

  private def createTopicAndGetLog(topic: String, topicPartition: TopicPartition): UnifiedLog = {
    createTopic(topic, 1, 1)

    val logManager = server.getLogManager
    TestUtils.waitUntilTrue(() => logManager.getLog(topicPartition).isDefined,
      "Log for partition [topic,0] should be created")
    logManager.getLog(topicPartition).get
  }

}
