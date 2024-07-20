/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.log.streamaspect

import kafka.log.streamaspect.utils.ExceptionUtil
import kafka.utils.Logging
import org.apache.kafka.common.TopicPartition

// TODO: better implementation, limit the partition meta logic in partition
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
                        f(elasticLog.getLocalLog(), checkpoint)
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
                read0(elasticLog => (elasticLog.topicPartition, elasticLog.logEndOffsetMetadata.messageOffset))
            case _ =>
                throw new IllegalArgumentException(s"Unsupported raw kafka meta $rawKafkaMeta for reading")
        }
    }

    private def read0(f: ElasticLog => (TopicPartition, Long)): Seq[(TopicPartition, Long)] = {
        var result: Seq[(TopicPartition, Long)] = Seq.empty
        ExceptionUtil.maybeRecordThrowableAndRethrow(new Runnable {
            override def run(): Unit = {
                result = ElasticLogManager.getAllElasticLogs.map(log => log.getLocalLog()).map(f).toSeq
            }
        }, "error when reading", this)
        result
    }
}
