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
package kafka.coordinator.transaction

import java.io.PrintStream
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import kafka.internals.generated.{TransactionLogKey, TransactionLogValue}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.protocol.{ByteBufferAccessor, MessageUtil}
import org.apache.kafka.common.record.{CompressionType, Record, RecordBatch}
import org.apache.kafka.common.{MessageFormatter, TopicPartition}

import scala.collection.mutable
import scala.jdk.CollectionConverters._

/**
 * Messages stored for the transaction topic represent the producer id and transactional status of the corresponding
 * transactional id, which have versions for both the key and value fields. Key and value
 * versions are used to evolve the message formats:
 *
 * key version 0:               [transactionalId]
 *    -> value version 0:       [producer_id, producer_epoch, expire_timestamp, status, [topic, [partition] ], timestamp]
 */
object TransactionLog {

  // log-level config default values and enforced values
  val DefaultNumPartitions: Int = 50
  val DefaultSegmentBytes: Int = 100 * 1024 * 1024
  val DefaultReplicationFactor: Short = 3.toShort
  val DefaultMinInSyncReplicas: Int = 2
  val DefaultLoadBufferSize: Int = 5 * 1024 * 1024

  // enforce always using
  //  1. cleanup policy = compact
  //  2. compression = none
  //  3. unclean leader election = disabled
  //  4. required acks = -1 when writing
  val EnforcedCompressionType: CompressionType = CompressionType.NONE
  val EnforcedRequiredAcks: Short = (-1).toShort

  /**
    * Generates the bytes for transaction log message key
    *
    * @return key bytes
    */
  private[transaction] def keyToBytes(transactionalId: String): Array[Byte] = {
    MessageUtil.toVersionPrefixedBytes(TransactionLogKey.HIGHEST_SUPPORTED_VERSION,
      new TransactionLogKey().setTransactionalId(transactionalId))
  }

  /**
    * Generates the payload bytes for transaction log message value
    *
    * @return value payload bytes
    */
  private[transaction] def valueToBytes(txnMetadata: TxnTransitMetadata): Array[Byte] = {
    if (txnMetadata.txnState == Empty && txnMetadata.topicPartitions.nonEmpty)
        throw new IllegalStateException(s"Transaction is not expected to have any partitions since its state is ${txnMetadata.txnState}: $txnMetadata")

      val transactionPartitions = if (txnMetadata.txnState == Empty) null
      else txnMetadata.topicPartitions
        .groupBy(_.topic)
        .map { case (topic, partitions) =>
          new TransactionLogValue.PartitionsSchema()
            .setTopic(topic)
            .setPartitionIds(partitions.map(tp => Integer.valueOf(tp.partition)).toList.asJava)
        }.toList.asJava

    MessageUtil.toVersionPrefixedBytes(TransactionLogValue.HIGHEST_SUPPORTED_VERSION,
      new TransactionLogValue()
        .setProducerId(txnMetadata.producerId)
        .setProducerEpoch(txnMetadata.producerEpoch)
        .setTransactionTimeoutMs(txnMetadata.txnTimeoutMs)
        .setTransactionStatus(txnMetadata.txnState.byte)
        .setTransactionLastUpdateTimestampMs(txnMetadata.txnLastUpdateTimestamp)
        .setTransactionStartTimestampMs(txnMetadata.txnStartTimestamp)
        .setTransactionPartitions(transactionPartitions))
  }

  /**
    * Decodes the transaction log messages' key
    *
    * @return the key
    */
  def readTxnRecordKey(buffer: ByteBuffer): TxnKey = {
    val version = buffer.getShort
    if (version >= TransactionLogKey.LOWEST_SUPPORTED_VERSION && version <= TransactionLogKey.HIGHEST_SUPPORTED_VERSION) {
      val value = new TransactionLogKey(new ByteBufferAccessor(buffer), version)
      TxnKey(
        version = version,
        transactionalId = value.transactionalId
      )
    } else throw new IllegalStateException(s"Unknown version $version from the transaction log message")
  }

  /**
    * Decodes the transaction log messages' payload and retrieves the transaction metadata from it
    *
    * @return a transaction metadata object from the message
    */
  def readTxnRecordValue(transactionalId: String, buffer: ByteBuffer): Option[TransactionMetadata] = {
    // tombstone
    if (buffer == null) None
    else {
      val version = buffer.getShort
      if (version >= TransactionLogValue.LOWEST_SUPPORTED_VERSION && version <= TransactionLogValue.HIGHEST_SUPPORTED_VERSION) {
        val value = new TransactionLogValue(new ByteBufferAccessor(buffer), version)
        val transactionMetadata = new TransactionMetadata(
          transactionalId = transactionalId,
          producerId = value.producerId,
          lastProducerId = RecordBatch.NO_PRODUCER_ID,
          producerEpoch = value.producerEpoch,
          lastProducerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
          txnTimeoutMs = value.transactionTimeoutMs,
          state = TransactionMetadata.byteToState(value.transactionStatus),
          topicPartitions = mutable.Set.empty[TopicPartition],
          txnStartTimestamp = value.transactionStartTimestampMs,
          txnLastUpdateTimestamp = value.transactionLastUpdateTimestampMs)

        if (!transactionMetadata.state.equals(Empty))
          value.transactionPartitions.forEach(partitionsSchema =>
            transactionMetadata.addPartitions(partitionsSchema.partitionIds
              .asScala
              .map(partitionId => new TopicPartition(partitionsSchema.topic, partitionId))
              .toSet)
          )
        Some(transactionMetadata)
      } else throw new IllegalStateException(s"Unknown version $version from the transaction log message value")
    }
  }

  // Formatter for use with tools to read transaction log messages
  class TransactionLogMessageFormatter extends MessageFormatter {
    def writeTo(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]], output: PrintStream): Unit = {
      Option(consumerRecord.key).map(key => readTxnRecordKey(ByteBuffer.wrap(key))).foreach { txnKey =>
        val transactionalId = txnKey.transactionalId
        val value = consumerRecord.value
        val producerIdMetadata = if (value == null)
          None
        else
          readTxnRecordValue(transactionalId, ByteBuffer.wrap(value))
        output.write(transactionalId.getBytes(StandardCharsets.UTF_8))
        output.write("::".getBytes(StandardCharsets.UTF_8))
        output.write(producerIdMetadata.getOrElse("NULL").toString.getBytes(StandardCharsets.UTF_8))
        output.write("\n".getBytes(StandardCharsets.UTF_8))
      }
    }
  }

  /**
   * Exposed for printing records using [[kafka.tools.DumpLogSegments]]
   */
  def formatRecordKeyAndValue(record: Record): (Option[String], Option[String]) = {
    val txnKey = TransactionLog.readTxnRecordKey(record.key)
    val keyString = s"transaction_metadata::transactionalId=${txnKey.transactionalId}"

    val valueString = TransactionLog.readTxnRecordValue(txnKey.transactionalId, record.value) match {
      case None => "<DELETE>"

      case Some(txnMetadata) => s"producerId:${txnMetadata.producerId}," +
        s"producerEpoch:${txnMetadata.producerEpoch}," +
        s"state=${txnMetadata.state}," +
        s"partitions=${txnMetadata.topicPartitions.mkString("[", ",", "]")}," +
        s"txnLastUpdateTimestamp=${txnMetadata.txnLastUpdateTimestamp}," +
        s"txnTimeoutMs=${txnMetadata.txnTimeoutMs}"
    }

    (Some(keyString), Some(valueString))
  }

}

case class TxnKey(version: Short, transactionalId: String) {
  override def toString: String = transactionalId
}
