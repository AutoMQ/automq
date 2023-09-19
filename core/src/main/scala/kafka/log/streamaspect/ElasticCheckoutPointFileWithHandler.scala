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

package kafka.log.streamaspect

import kafka.log.stream.utils.ExceptionUtil
import kafka.utils.Logging
import org.apache.kafka.common.TopicPartition

class ElasticCheckoutPointFileWithHandler(val rawKafkaMeta: RawKafkaMeta) extends Logging {
  this.logIdent = s"[ElasticCheckoutPointFileWithHandler $rawKafkaMeta] "

  def write(entries: Iterable[(TopicPartition, Long)]): Unit = {
    rawKafkaMeta match {
      case LogStartOffsetCheckpoint =>
        write0(entries, (elasticLog, _) => elasticLog.persistLogStartOffset())
      case CleanerOffsetCheckpoint =>
        write0(entries, (elasticLog, checkpoint) => elasticLog.persistCleanerOffsetCheckpoint(checkpoint))
      case RecoveryPointCheckpoint =>
        write0(entries, (elasticLog, _) => elasticLog.persistRecoverOffsetCheckpoint())
      // do nothing when writing ReplicationOffsetCheckpoint
      case ReplicationOffsetCheckpoint =>
      case _ =>
        throw new IllegalArgumentException(s"Unsupported raw kafka meta $rawKafkaMeta for writing")
    }
  }

  private def write0(entries: Iterable[(TopicPartition, Long)], f: (ElasticLog, Long) => Unit): Unit = {
    ExceptionUtil.maybeRecordThrowableAndRethrow(new Runnable {
      override def run(): Unit = {
        for (elem <- entries) {
          val (topicPartition, checkpoint) = elem
          val elasticLog = ElasticLogManager.getElasticLog(topicPartition)
          if (elasticLog == null) {
            warn(s"Cannot find elastic log for $topicPartition and skip persistence. Is this broker shutting down?")
          } else {
            f(elasticLog, checkpoint)
          }
        }
      }
    }, "error when writing", this)
  }

  def read(): Seq[(TopicPartition, Long)] = {
    rawKafkaMeta match {
      case LogStartOffsetCheckpoint =>
        read0(elasticLog => (elasticLog.topicPartition, elasticLog.logStartOffset))
      case CleanerOffsetCheckpoint =>
        read0(elasticLog => (elasticLog.topicPartition, elasticLog.getCleanerOffsetCheckpointFromMeta))
      case RecoveryPointCheckpoint =>
        read0(elasticLog => (elasticLog.topicPartition, elasticLog.recoveryPoint))
      case ReplicationOffsetCheckpoint =>
        read0(elasticLog => (elasticLog.topicPartition, elasticLog.nextOffsetMetadata.messageOffset))
      case _ =>
        throw new IllegalArgumentException(s"Unsupported raw kafka meta $rawKafkaMeta for reading")
    }
  }

  private def read0(f: ElasticLog => (TopicPartition, Long)): Seq[(TopicPartition, Long)] = {
    var result: Seq[(TopicPartition, Long)] = Seq.empty
    ExceptionUtil.maybeRecordThrowableAndRethrow(new Runnable {
      override def run(): Unit = {
        result = ElasticLogManager.getAllElasticLogs.map(f).toSeq
      }
    }, "error when reading", this)
    result
  }
}
