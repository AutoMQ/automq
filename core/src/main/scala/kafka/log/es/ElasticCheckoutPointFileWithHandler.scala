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

package kafka.log.es

import org.apache.kafka.common.TopicPartition

class ElasticCheckoutPointFileWithHandler(val rawKafkaMeta: RawKafkaMeta) {

  def write(entries: Iterable[(TopicPartition, Long)]): Unit = {
    rawKafkaMeta match {
      case LogStartOffsetCheckpoint =>
        write0(entries, (elasticLog, checkpoint) => elasticLog.setLogStartOffset(checkpoint))
      case CleanerOffsetCheckpoint =>
        write0(entries, (elasticLog, checkpoint) => elasticLog.setCleanerOffsetCheckpoint(checkpoint))
      case RecoveryPointCheckpoint =>
        write0(entries, (elasticLog, checkpoint) => elasticLog.setReCoverOffsetCheckpoint(checkpoint))
      // do nothing when writing ReplicationOffsetCheckpoint
      case ReplicationOffsetCheckpoint =>
      case _ =>
        throw new IllegalArgumentException(s"Unsupported raw kafka meta $rawKafkaMeta for writing")
    }
  }

  private def write0(entries: Iterable[(TopicPartition, Long)], f: (ElasticLog, Long) => Unit): Unit = {
    for (elem <- entries) {
      val (topicPartition, checkpoint) = elem
      val elasticLog = ElasticLogManager.getElasticLog(topicPartition)
      f(elasticLog, checkpoint)
    }
  }

  def read(): Seq[(TopicPartition, Long)] = {
    rawKafkaMeta match {
      case LogStartOffsetCheckpoint =>
        read0(elasticLog => (elasticLog.topicPartition, elasticLog.logStartOffset))
      case CleanerOffsetCheckpoint =>
        read0(elasticLog => (elasticLog.topicPartition, elasticLog.cleanerOffsetCheckpoint))
      case RecoveryPointCheckpoint =>
        read0(elasticLog => (elasticLog.topicPartition, elasticLog.recoveryPointCheckpoint))
      case ReplicationOffsetCheckpoint =>
        read0(elasticLog => (elasticLog.topicPartition, elasticLog.nextOffsetMetadata.messageOffset))
      case _ =>
        throw new IllegalArgumentException(s"Unsupported raw kafka meta $rawKafkaMeta for reading")
    }
  }

  private def read0(f: ElasticLog => (TopicPartition, Long)): Seq[(TopicPartition, Long)] = {
    ElasticLogManager.getAllElasticLogs.map(f).toSeq
  }
}
