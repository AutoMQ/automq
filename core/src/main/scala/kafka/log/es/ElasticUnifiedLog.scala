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

import kafka.log._
import kafka.server.epoch.LeaderEpochFileCache
import kafka.server.{BrokerTopicStats, LogOffsetMetadata}
import kafka.utils.{CoreUtils, Logging}
import org.apache.kafka.common.record.{RecordBatch, RecordVersion, Records}
import org.apache.kafka.common.utils.{Time, Utils}
import org.apache.kafka.common.{TopicPartition, Uuid}

import java.util.concurrent.CompletableFuture
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class ElasticUnifiedLog(_logStartOffset: Long,
                        elasticLog: ElasticLog,
                        brokerTopicStats: BrokerTopicStats,
                        producerIdExpirationCheckIntervalMs: Int,
                        _leaderEpochCache: Option[LeaderEpochFileCache],
                        producerStateManager: ProducerStateManager,
                        _topicId: Option[Uuid])
  extends UnifiedLog(_logStartOffset, elasticLog, brokerTopicStats, producerIdExpirationCheckIntervalMs,
    _leaderEpochCache, producerStateManager, _topicId, false) {

  var confirmOffsetChangeListener: Option[() => Unit] = None

  elasticLog.confirmOffsetChangeListener = Some(() => confirmOffsetChangeListener.map(_.apply()))

  def confirmOffset(): LogOffsetMetadata = {
    elasticLog.confirmOffset.get()
  }

  override private[log] def replaceSegments(newSegments: collection.Seq[LogSegment], oldSegments: collection.Seq[LogSegment]): Unit = {
    val deletedSegments = elasticLog.replaceSegments(newSegments, oldSegments)
    deleteProducerSnapshots(deletedSegments, asyncDelete = true)
  }

  override protected def maybeHandleIOException[T](msg: => String)(fun: => T): T = {
    LocalLog.maybeHandleIOException(logDirFailureChannel, dir.getPath, msg) {
      fun
    }
  }

  override private[log] def splitOverflowedSegment(segment: LogSegment) = {
    // normally, there should be no overflowed segment
    throw new UnsupportedOperationException()
  }

  override def initializeTopicId(): Unit = {
    // topic id is passed by constructor arguments every time, there is no need load from partition meta file.
  }

  // only for testing
  private[log] def listProducerSnapshots(): mutable.Map[Long, ElasticPartitionProducerSnapshotMeta] = {
    val producerSnapshots: mutable.Map[Long, ElasticPartitionProducerSnapshotMeta] = mutable.Map()
    elasticLog.metaStream.getAllProducerSnapshots.forEach((offset, snapshot) => {
      producerSnapshots.put(offset.longValue(), snapshot)
    })
    producerSnapshots
  }

  // only for testing
  private[log] def removeAndDeleteSegments(segmentsToDelete: Iterable[LogSegment],
                                           asyncDelete: Boolean): Unit = {
    elasticLog.removeAndDeleteSegments(segmentsToDelete, asyncDelete, LogDeletion(elasticLog))
  }

  override def close(): Unit = {
    val future = closeWithFuture()
    CoreUtils.swallow(future.get(), this)
  }

  override def closeWithFuture(): CompletableFuture[Void] = {
    info("Closing log")
    val closeFuture = lock synchronized {
      maybeFlushMetadataFile()
      elasticLog.checkIfMemoryMappedBufferClosed()
      producerExpireCheck.cancel(true)
      maybeHandleIOException(s"Error while closing $topicPartition") {
        // We take a snapshot at the last written offset to hopefully avoid the need to scan the log
        // after restarting and to ensure that we cannot inadvertently hit the upgrade optimization
        // (the clean shutdown file is written after the logs are all closed).
        producerStateManager.takeSnapshot()
      }
      // flush all inflight data/index
      flush(true)
      elasticLog.closeWithFuture()
    }
    elasticLog.segments.clear()
    elasticLog.isMemoryMappedBufferClosed = true
    elasticLog.deleteEmptyDir()
    closeFuture
  }

  /**
   * Only close streams.
   */
  def closeStreams(): CompletableFuture[Void] = {
    elasticLog.closeStreams()
  }

  override private[log] def delete(): Unit = {
    throw new UnsupportedOperationException("delete() is not supported for ElasticUnifiedLog")
  }
}

object ElasticUnifiedLog extends Logging {
  def rebuildProducerState(producerStateManager: ProducerStateManager,
                           segments: LogSegments,
                           logStartOffset: Long,
                           lastOffset: Long,
                           time: Time,
                           reloadFromCleanShutdown: Boolean,
                           logPrefix: String): Unit = {
    val offsetsToSnapshot = {
      if (segments.nonEmpty) {
        val lastSegmentBaseOffset = segments.lastSegment.get.baseOffset
        val nextLatestSegmentBaseOffset = segments.lowerSegment(lastSegmentBaseOffset).map(_.baseOffset)
        Seq(nextLatestSegmentBaseOffset, Some(lastSegmentBaseOffset), Some(lastOffset))
      } else {
        Seq(Some(lastOffset))
      }
    }

    info(s"Reloading from producer snapshot and rebuilding producer state from offset $lastOffset")
    val isEmptyBeforeTruncation = producerStateManager.isEmpty && producerStateManager.mapEndOffset >= lastOffset
    val producerStateLoadStart = time.milliseconds()
    producerStateManager.truncateAndReload(logStartOffset, lastOffset, time.milliseconds())
    val segmentRecoveryStart = time.milliseconds()

    if (lastOffset > producerStateManager.mapEndOffset && !isEmptyBeforeTruncation) {
      segments.values(producerStateManager.mapEndOffset, lastOffset).foreach { segment =>
        val startOffset = Utils.max(segment.baseOffset, producerStateManager.mapEndOffset, logStartOffset)
        producerStateManager.updateMapEndOffset(startOffset)

        if (offsetsToSnapshot.contains(Some(segment.baseOffset)))
          producerStateManager.takeSnapshot()

        val maxPosition = segment.size

        val fetchDataInfo = segment.read(startOffset,
          maxSize = Int.MaxValue,
          maxPosition = maxPosition)
        if (fetchDataInfo != null)
          loadProducersFromRecords(producerStateManager, fetchDataInfo.records)
      }
    }
    producerStateManager.updateMapEndOffset(lastOffset)
    producerStateManager.takeSnapshot()
    info(s"${logPrefix}Producer state recovery took ${segmentRecoveryStart - producerStateLoadStart}ms for snapshot load " +
      s"and ${time.milliseconds() - segmentRecoveryStart}ms for segment recovery from offset $lastOffset")
  }

  /**
   * If the recordVersion is >= RecordVersion.V2, then create and return a LeaderEpochFileCache.
   * Otherwise, the message format is considered incompatible and return None.
   *
   * @param topicPartition        The topic partition
   * @param recordVersion         The record version
   * @param leaderEpochCheckpoint The leader epoch checkpoint
   * @return The new LeaderEpochFileCache instance (if created), none otherwise
   */
  private[log] def maybeCreateLeaderEpochCache(topicPartition: TopicPartition,
                                               recordVersion: RecordVersion,
                                               leaderEpochCheckpoint: ElasticLeaderEpochCheckpoint): Option[LeaderEpochFileCache] = {

    def newLeaderEpochFileCache(): LeaderEpochFileCache = new LeaderEpochFileCache(topicPartition, leaderEpochCheckpoint)

    if (recordVersion.precedes(RecordVersion.V2)) {
      None
    } else {
      Some(newLeaderEpochFileCache())
    }
  }

  private def loadProducersFromRecords(producerStateManager: ProducerStateManager, records: Records): Unit = {
    val loadedProducers = mutable.Map.empty[Long, ProducerAppendInfo]
    val completedTxns = ListBuffer.empty[CompletedTxn]
    records.batches.forEach { batch =>
      if (batch.hasProducerId) {
        val maybeCompletedTxn = updateProducers(
          producerStateManager,
          batch,
          loadedProducers,
          firstOffsetMetadata = None,
          origin = AppendOrigin.Replication)
        maybeCompletedTxn.foreach(completedTxns += _)
      }
    }
    loadedProducers.values.foreach(producerStateManager.update)
    completedTxns.foreach(producerStateManager.completeTxn)
  }

  private def updateProducers(producerStateManager: ProducerStateManager,
                              batch: RecordBatch,
                              producers: mutable.Map[Long, ProducerAppendInfo],
                              firstOffsetMetadata: Option[LogOffsetMetadata],
                              origin: AppendOrigin): Option[CompletedTxn] = {
    val producerId = batch.producerId
    val appendInfo = producers.getOrElseUpdate(producerId, producerStateManager.prepareUpdate(producerId, origin))
    appendInfo.append(batch, firstOffsetMetadata)
  }
}
