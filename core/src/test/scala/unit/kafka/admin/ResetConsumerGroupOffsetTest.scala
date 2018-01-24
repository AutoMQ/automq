/**
  * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
  * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
  * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
  * License. You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
  * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
  * specific language governing permissions and limitations under the License.
  */
package kafka.admin

import java.io.{BufferedWriter, File, FileWriter}
import java.text.{ParseException, SimpleDateFormat}
import java.util.{Calendar, Date, Properties}

import kafka.admin.ConsumerGroupCommand.{ConsumerGroupCommandOptions, ConsumerGroupService, KafkaConsumerGroupService}
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.junit.Assert._
import org.junit.Test

class TimeConversionTests {

  @Test
  def testDateTimeFormats() {
    //check valid formats
    invokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS"))
    invokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ"))
    invokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX"))
    invokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXX"))
    invokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))

    //check some invalid formats
    try {
      invokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss"))
      fail("Call to getDateTime should fail")
    } catch {
      case _: ParseException =>
    }

    try {
      invokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.X"))
      fail("Call to getDateTime should fail")
    } catch {
      case _: ParseException =>
    }
  }

  private def invokeGetDateTimeMethod(format: SimpleDateFormat) {
    val checkpoint = new Date()
    val timestampString = format.format(checkpoint)
    ConsumerGroupCommand.convertTimestamp(timestampString)
  }

}

/**
  * Test cases by:
  * - Non-existing consumer group
  * - One for each scenario, with scope=all-topics
  * - scope=one topic, scenario=to-earliest
  * - scope=one topic+partitions, scenario=to-earliest
  * - scope=topics, scenario=to-earliest
  * - scope=topics+partitions, scenario=to-earliest
  * - export/import
  */
class ResetConsumerGroupOffsetTest extends ConsumerGroupCommandTest {
  
  val overridingProps = new Properties()
  val topic1 = "foo1"
  val topic2 = "foo2"

  /**
    * Implementations must override this method to return a set of KafkaConfigs. This method will be invoked for every
    * test and should not reuse previous configurations unless they select their ports randomly when servers are started.
    */
  override def generateConfigs: Seq[KafkaConfig] = {
    TestUtils.createBrokerConfigs(1, zkConnect, enableControlledShutdown = false)
      .map(KafkaConfig.fromProps(_, overridingProps))  
  } 

  @Test
  def testResetOffsetsNotExistingGroup() {
    addConsumerGroupExecutor(numConsumers = 1, topic1)

    val cgcArgs = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", "missing.group", "--all-topics", "--to-current")
    val consumerGroupCommand = getConsumerGroupService(cgcArgs)

    TestUtils.waitUntilTrue(() => {
      val assignmentsToReset = consumerGroupCommand.resetOffsets()
      assignmentsToReset == Map.empty
    }, "Expected to have an empty assignations map.")

  }

  @Test
  def testResetOffsetsNewConsumerExistingTopic(): Unit = {
    val cgcArgs = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", "new.group", "--topic", topic1, "--to-offset", "50")
    val opts = new ConsumerGroupCommandOptions(cgcArgs)
    val consumerGroupCommand = new KafkaConsumerGroupService(opts)

    adminZkClient.createTopic(topic1, 1, 1)

    TestUtils.produceMessages(servers, topic1, 100, acks = 1, 100 * 1000)

    TestUtils.waitUntilTrue(() => {
      val assignmentsToReset = consumerGroupCommand.resetOffsets()
      assignmentsToReset.exists({ assignment => assignment._2.offset() == 50 })
    }, "Expected the consumer group to reset to offset 1 (specific offset).")

    printConsumerGroup("new.group")
    adminZkClient.deleteTopic(topic1)
    consumerGroupCommand.close()
  }

  @Test
  def testResetOffsetsToLocalDateTime() {
    adminZkClient.createTopic(topic1, 1, 1)

    val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.DATE, -1)

    TestUtils.produceMessages(servers, topic1, 100, acks = 1, 100 * 1000)

    val cgcArgs = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics", "--dry-run")
    val consumerGroupCommand = getConsumerGroupService(cgcArgs)

    val executor = addConsumerGroupExecutor(numConsumers = 1, topic1)

    TestUtils.waitUntilTrue(() => {
      val (_, assignmentsOption) = consumerGroupCommand.collectGroupOffsets()
      assignmentsOption match {
        case Some(assignments) =>
          val sumOffset = assignments.filter(_.topic.exists(_ == topic1))
            .filter(_.offset.isDefined)
            .map(assignment => assignment.offset.get)
            .foldLeft(0.toLong)(_ + _)
          sumOffset == 100
        case _ => false
      }
    }, "Expected that consumer group has consumed all messages from topic/partition.")

    executor.shutdown()

    val cgcArgs1 = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics", "--to-datetime", format.format(calendar.getTime))
    val consumerGroupCommand1 = getConsumerGroupService(cgcArgs1)

    TestUtils.waitUntilTrue(() => {
      val assignmentsToReset = consumerGroupCommand1.resetOffsets()
      assignmentsToReset.exists { assignment => assignment._2.offset() == 0 }
    }, "Expected the consumer group to reset to when offset was 50.")

    printConsumerGroup()

    adminZkClient.deleteTopic(topic1)
  }

  @Test
  def testResetOffsetsToZonedDateTime() {
    adminZkClient.createTopic(topic1, 1, 1)
    TestUtils.produceMessages(servers, topic1, 50, acks = 1, 100 * 1000)

    val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
    val checkpoint = new Date()

    TestUtils.produceMessages(servers, topic1, 50, acks = 1, 100 * 1000)

    val cgcArgs = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics", "--dry-run")
    val consumerGroupCommand = getConsumerGroupService(cgcArgs)

    val executor = addConsumerGroupExecutor(numConsumers = 1, topic1)

    TestUtils.waitUntilTrue(() => {
      val (_, assignmentsOption) = consumerGroupCommand.collectGroupOffsets()
      assignmentsOption match {
        case Some(assignments) =>
          val sumOffset = (assignments.filter(_.topic.exists(_ == topic1))
            .filter(_.offset.isDefined)
            .map(assignment => assignment.offset.get) foldLeft 0.toLong)(_ + _)
          sumOffset == 100
        case _ => false
      }
    }, "Expected that consumer group has consumed all messages from topic/partition.")

    executor.shutdown()

    val cgcArgs1 = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics", "--to-datetime", format.format(checkpoint))
    val consumerGroupCommand1 = getConsumerGroupService(cgcArgs1)

    TestUtils.waitUntilTrue(() => {
      val assignmentsToReset = consumerGroupCommand1.resetOffsets()
      assignmentsToReset.exists { assignment => assignment._2.offset() == 50 }
    }, "Expected the consumer group to reset to when offset was 50.")

    printConsumerGroup()

    adminZkClient.deleteTopic(topic1)
  }

  @Test
  def testResetOffsetsByDuration() {
    val cgcArgs = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics", "--by-duration", "PT1M")
    val consumerGroupCommand = getConsumerGroupService(cgcArgs)

    adminZkClient.createTopic(topic1, 1, 1)

    produceConsumeAndShutdown(consumerGroupCommand, 1, topic1, 100)

    TestUtils.waitUntilTrue(() => {
        val assignmentsToReset = consumerGroupCommand.resetOffsets()
        assignmentsToReset.exists { assignment => assignment._2.offset() == 0 }
    }, "Expected the consumer group to reset to offset 0 (earliest by duration).")

    printConsumerGroup()

    adminZkClient.deleteTopic(topic1)
  }

  @Test
  def testResetOffsetsByDurationToEarliest() {
    val cgcArgs = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics", "--by-duration", "PT0.1S")
    val consumerGroupCommand = getConsumerGroupService(cgcArgs)

    adminZkClient.createTopic(topic1, 1, 1)

    produceConsumeAndShutdown(consumerGroupCommand, 1, topic1, 100)

    TestUtils.waitUntilTrue(() => {
      val assignmentsToReset = consumerGroupCommand.resetOffsets()
      assignmentsToReset.exists { assignment => assignment._2.offset() == 100 }
    }, "Expected the consumer group to reset to offset 100 (latest by duration).")

    printConsumerGroup()
    adminZkClient.deleteTopic(topic1)
  }

  @Test
  def testResetOffsetsToEarliest() {
    val cgcArgs = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics", "--to-earliest")
    val consumerGroupCommand = getConsumerGroupService(cgcArgs)

    adminZkClient.createTopic(topic1, 1, 1)

    produceConsumeAndShutdown(consumerGroupCommand, 1, topic1, 100)

    TestUtils.waitUntilTrue(() => {
      val assignmentsToReset = consumerGroupCommand.resetOffsets()
      assignmentsToReset.exists { assignment => assignment._2.offset() == 0 }
    }, "Expected the consumer group to reset to offset 0 (earliest).")

    printConsumerGroup()
    adminZkClient.deleteTopic(topic1)
  }

  @Test
  def testResetOffsetsToLatest() {
    val cgcArgs = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics", "--to-latest")
    val consumerGroupCommand = getConsumerGroupService(cgcArgs)

    adminZkClient.createTopic(topic1, 1, 1)

    produceConsumeAndShutdown(consumerGroupCommand, 1, topic1, 100)

    TestUtils.produceMessages(servers, topic1, 100, acks = 1, 100 * 1000)


    TestUtils.waitUntilTrue(() => {
      val assignmentsToReset = consumerGroupCommand.resetOffsets()
      assignmentsToReset.exists({ assignment => assignment._2.offset() == 200 })
    }, "Expected the consumer group to reset to offset 200 (latest).")

    printConsumerGroup()
    adminZkClient.deleteTopic(topic1)
  }

  @Test
  def testResetOffsetsToCurrentOffset() {
    val cgcArgs = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics", "--to-current")
    val consumerGroupCommand = getConsumerGroupService(cgcArgs)

    adminZkClient.createTopic(topic1, 1, 1)

    produceConsumeAndShutdown(consumerGroupCommand, 1, topic1, 100)

    TestUtils.produceMessages(servers, topic1, 100, acks = 1, 100 * 1000)

    TestUtils.waitUntilTrue(() => {
      val assignmentsToReset = consumerGroupCommand.resetOffsets()
      assignmentsToReset.exists({ assignment => assignment._2.offset() == 100 })
    }, "Expected the consumer group to reset to offset 100 (current).")

    printConsumerGroup()
    adminZkClient.deleteTopic(topic1)
  }

  private def produceConsumeAndShutdown(consumerGroupCommand: ConsumerGroupService, numConsumers: Int, topic: String, totalMessages: Int) {
    TestUtils.produceMessages(servers, topic, totalMessages, acks = 1, 100 * 1000)
    val executor =  addConsumerGroupExecutor(numConsumers, topic)

    TestUtils.waitUntilTrue(() => {
      val (_, assignmentsOption) = consumerGroupCommand.collectGroupOffsets()
      assignmentsOption match {
        case Some(assignments) =>
          val sumOffset = assignments.filter(_.topic.exists(_ == topic))
            .filter(_.offset.isDefined)
            .map(assignment => assignment.offset.get)
            .foldLeft(0.toLong)(_ + _)
          sumOffset == totalMessages
        case _ => false
      }
    }, "Expected the consumer group to consume all messages from topic.")

    executor.shutdown()
  }

  @Test
  def testResetOffsetsToSpecificOffset() {
    val cgcArgs = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics", "--to-offset", "1")
    val consumerGroupCommand = getConsumerGroupService(cgcArgs)

    adminZkClient.createTopic(topic1, 1, 1)

    produceConsumeAndShutdown(consumerGroupCommand, 1, topic1, 100)


    TestUtils.waitUntilTrue(() => {
      val assignmentsToReset = consumerGroupCommand.resetOffsets()
      assignmentsToReset.exists({ assignment => assignment._2.offset() == 1 })
    }, "Expected the consumer group to reset to offset 1 (specific offset).")

    printConsumerGroup()
    adminZkClient.deleteTopic(topic1)
  }

  @Test
  def testResetOffsetsShiftPlus() {
    val cgcArgs = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics", "--shift-by", "50")
    val consumerGroupCommand = getConsumerGroupService(cgcArgs)

    adminZkClient.createTopic(topic1, 1, 1)

    produceConsumeAndShutdown(consumerGroupCommand, 1, topic1, 100)

    TestUtils.produceMessages(servers, topic1, 100, acks = 1, 100 * 1000)

    TestUtils.waitUntilTrue(() => {
      val assignmentsToReset = consumerGroupCommand.resetOffsets()
      assignmentsToReset.exists({ assignment => assignment._2.offset() == 150 })
    }, "Expected the consumer group to reset to offset 150 (current + 50).")

    printConsumerGroup()
    adminZkClient.deleteTopic(topic1)
  }

  @Test
  def testResetOffsetsShiftMinus() {
    val cgcArgs = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics", "--shift-by", "-50")
    val consumerGroupCommand = getConsumerGroupService(cgcArgs)

    adminZkClient.createTopic(topic1, 1, 1)

    produceConsumeAndShutdown(consumerGroupCommand, 1, topic1, 100)

    TestUtils.produceMessages(servers, topic1, 100, acks = 1, 100 * 1000)


    TestUtils.waitUntilTrue(() => {
      val assignmentsToReset = consumerGroupCommand.resetOffsets()
      assignmentsToReset.exists({ assignment => assignment._2.offset() == 50 })
    }, "Expected the consumer group to reset to offset 50 (current - 50).")

    printConsumerGroup()
    adminZkClient.deleteTopic(topic1)
  }

  @Test
  def testResetOffsetsShiftByLowerThanEarliest() {
    val cgcArgs = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics", "--shift-by", "-150")
    val consumerGroupCommand = getConsumerGroupService(cgcArgs)

    adminZkClient.createTopic(topic1, 1, 1)

    produceConsumeAndShutdown(consumerGroupCommand, 1, topic1, 100)

    TestUtils.produceMessages(servers, topic1, 100, acks = 1, 100 * 1000)

    TestUtils.waitUntilTrue(() => {
      val assignmentsToReset = consumerGroupCommand.resetOffsets()
      assignmentsToReset.exists({ assignment => assignment._2.offset() == 0 })
    }, "Expected the consumer group to reset to offset 0 (earliest by shift).")

    printConsumerGroup()
    adminZkClient.deleteTopic(topic1)
  }

  @Test
  def testResetOffsetsShiftByHigherThanLatest() {
    val cgcArgs = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics", "--shift-by", "150")
    val consumerGroupCommand = getConsumerGroupService(cgcArgs)

    adminZkClient.createTopic(topic1, 1, 1)

    produceConsumeAndShutdown(consumerGroupCommand, 1, topic1, 100)

    TestUtils.produceMessages(servers, topic1, 100, acks = 1, 100 * 1000)

    TestUtils.waitUntilTrue(() => {
      val assignmentsToReset = consumerGroupCommand.resetOffsets()
      assignmentsToReset.exists({ assignment => assignment._2.offset() == 200 })
    }, "Expected the consumer group to reset to offset 200 (latest by shift).")

    printConsumerGroup()
    adminZkClient.deleteTopic(topic1)
  }

  @Test
  def testResetOffsetsToEarliestOnOneTopic() {
    val cgcArgs = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--topic", topic1, "--to-earliest")
    val consumerGroupCommand = getConsumerGroupService(cgcArgs)

    adminZkClient.createTopic(topic1, 1, 1)

    produceConsumeAndShutdown(consumerGroupCommand, 1, topic1, 100)

    TestUtils.waitUntilTrue(() => {
      val assignmentsToReset = consumerGroupCommand.resetOffsets()
      assignmentsToReset.exists { assignment => assignment._2.offset() == 0 }
    }, "Expected the consumer group to reset to offset 0 (earliest).")

    printConsumerGroup()
    adminZkClient.deleteTopic(topic1)
  }

  @Test
  def testResetOffsetsToEarliestOnOneTopicAndPartition() {
    val cgcArgs = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--topic", String.format("%s:1", topic1), "--to-earliest")
    val consumerGroupCommand = getConsumerGroupService(cgcArgs)

    adminZkClient.createTopic(topic1, 2, 1)

    produceConsumeAndShutdown(consumerGroupCommand, 2, topic1, 100)

    TestUtils.waitUntilTrue(() => {
      val assignmentsToReset = consumerGroupCommand.resetOffsets()
      assignmentsToReset.exists { assignment => assignment._2.offset() == 0 && assignment._1.partition() == 1 }
    }, "Expected the consumer group to reset to offset 0 (earliest) in partition 1.")

    printConsumerGroup()
    adminZkClient.deleteTopic(topic1)
  }

  @Test
  def testResetOffsetsToEarliestOnTopics() {
    val cgcArgs = Array("--bootstrap-server", brokerList, "--reset-offsets",
      "--group", group,
      "--topic", topic1,
      "--topic", topic2,
      "--to-earliest")
    val consumerGroupCommand = getConsumerGroupService(cgcArgs)

    adminZkClient.createTopic(topic1, 1, 1)
    adminZkClient.createTopic(topic2, 1, 1)

    produceConsumeAndShutdown(consumerGroupCommand, 1, topic1, 100)
    produceConsumeAndShutdown(consumerGroupCommand, 1, topic2, 100)

    TestUtils.waitUntilTrue(() => {
      val assignmentsToReset = consumerGroupCommand.resetOffsets()
      assignmentsToReset.exists { assignment => assignment._2.offset() == 0 && assignment._1.topic() == topic1 } &&
        assignmentsToReset.exists { assignment => assignment._2.offset() == 0 && assignment._1.topic() == topic2 }
    }, "Expected the consumer group to reset to offset 0 (earliest).")

    printConsumerGroup()
    adminZkClient.deleteTopic(topic1)
    adminZkClient.deleteTopic(topic2)
  }

  @Test
  def testResetOffsetsToEarliestOnTopicsAndPartitions() {
    val cgcArgs = Array("--bootstrap-server", brokerList, "--reset-offsets",
      "--group", group,
      "--topic", String.format("%s:1", topic1),
      "--topic", String.format("%s:1", topic2),
      "--to-earliest")
    val consumerGroupCommand = getConsumerGroupService(cgcArgs)

    adminZkClient.createTopic(topic1, 2, 1)
    adminZkClient.createTopic(topic2, 2, 1)

    produceConsumeAndShutdown(consumerGroupCommand, 2, topic1, 100)
    produceConsumeAndShutdown(consumerGroupCommand, 2, topic2, 100)

    TestUtils.waitUntilTrue(() => {
      val assignmentsToReset = consumerGroupCommand.resetOffsets()
      assignmentsToReset.exists { assignment => assignment._2.offset() == 0 && assignment._1.partition() == 1 && assignment._1.topic() == topic1 }
      assignmentsToReset.exists { assignment => assignment._2.offset() == 0 && assignment._1.partition() == 1 && assignment._1.topic() == topic2 }
    }, "Expected the consumer group to reset to offset 0 (earliest) in partition 1.")

    printConsumerGroup()
    adminZkClient.deleteTopic(topic1)
    adminZkClient.deleteTopic(topic2)
  }

  @Test
  def testResetOffsetsExportImportPlan() {
    val cgcArgs = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics", "--to-offset","2", "--export")
    val consumerGroupCommand = getConsumerGroupService(cgcArgs)

    adminZkClient.createTopic(topic1, 2, 1)

    produceConsumeAndShutdown(consumerGroupCommand, 2, topic1, 100)

    val file = File.createTempFile("reset", ".csv")

    TestUtils.waitUntilTrue(() => {
      val assignmentsToReset = consumerGroupCommand.resetOffsets()
      val bw = new BufferedWriter(new FileWriter(file))
      bw.write(consumerGroupCommand.exportOffsetsToReset(assignmentsToReset))
      bw.close()
      assignmentsToReset.exists { assignment => assignment._2.offset() == 2 } && file.exists()
    }, "Expected the consume all messages and save reset offsets plan to file")


    val cgcArgsExec = Array("--bootstrap-server", brokerList, "--reset-offsets", "--group", group, "--all-topics", "--from-file", file.getCanonicalPath, "--dry-run")
    val consumerGroupCommandExec = getConsumerGroupService(cgcArgsExec)

    TestUtils.waitUntilTrue(() => {
        val assignmentsToReset = consumerGroupCommandExec.resetOffsets()
        assignmentsToReset.exists { assignment => assignment._2.offset() == 2 }
    }, "Expected the consumer group to reset to offset 2 according to the plan in the file.")

    file.deleteOnExit()

    printConsumerGroup()
    adminZkClient.deleteTopic(topic1)
  }

  private def printConsumerGroup() {
    val cgcArgs = Array("--bootstrap-server", brokerList, "--group", group, "--describe")
    ConsumerGroupCommand.main(cgcArgs)
  }

  private def printConsumerGroup(group: String) {
    val cgcArgs = Array("--bootstrap-server", brokerList, "--group", group, "--describe")
    ConsumerGroupCommand.main(cgcArgs)
  }

}
