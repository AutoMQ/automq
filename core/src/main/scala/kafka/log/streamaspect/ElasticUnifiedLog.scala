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

import kafka.log._
import kafka.log.streamaspect.ElasticUnifiedLog.updateProducers
import kafka.server.epoch.LeaderEpochFileCache
import kafka.server.{BrokerTopicStats, FetchDataInfo, FetchHighWatermark, FetchIsolation, FetchLogEnd, FetchTxnCommitted, LogOffsetMetadata}
import kafka.utils.Logging
import org.apache.kafka.common.errors.OffsetOutOfRangeException
import org.apache.kafka.common.record.{MemoryRecords, RecordBatch, RecordVersion, Records}
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
                        __topicId: Option[Uuid])
  extends UnifiedLog(_logStartOffset, elasticLog, brokerTopicStats, producerIdExpirationCheckIntervalMs,
    _leaderEpochCache, producerStateManager, __topicId, false) {

  var confirmOffsetChangeListener: Option[() => Unit] = None

  /**
   * The time stamp when the log is opened. It is used to handle the case that the first batch comes with partition not
   * opened yet. See https://github.com/AutoMQ/automq-for-kafka/issues/584.
   */
  val openedTimeStamp = System.currentTimeMillis()

  elasticLog.confirmOffsetChangeListener = Some(() => confirmOffsetChangeListener.map(_.apply()))

  def confirmOffset(): LogOffsetMetadata = {
    elasticLog.confirmOffset
  }

  override private[log] def replaceSegments(newSegments: collection.Seq[LogSegment], oldSegments: collection.Seq[LogSegment]): Unit = {
    val deletedSegments = elasticLog.replaceSegments(newSegments, oldSegments)
    deleteProducerSnapshots(deletedSegments, asyncDelete = true)
  }

  // We only add the partition's path into failureLogDirs instead of the whole logDir.
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

  override protected def analyzeAndValidateProducerState(appendOffsetMetadata: LogOffsetMetadata,
                                                         records: MemoryRecords,
                                                         origin: AppendOrigin):
  (mutable.Map[Long, ProducerAppendInfo], List[CompletedTxn], Option[BatchMetadata]) = {
    val updatedProducers = mutable.Map.empty[Long, ProducerAppendInfo]
    val completedTxns = ListBuffer.empty[CompletedTxn]
    var relativePositionInSegment = appendOffsetMetadata.relativePositionInSegment

    records.batches.forEach { batch =>
      if (batch.hasProducerId) {
        // if this is a client produce request, there will be up to 5 batches which could have been duplicated.
        // If we find a duplicate, we return the metadata of the appended batch to the client.
        if (origin == AppendOrigin.Client) {
          val maybeLastEntry = producerStateManager.lastEntry(batch.producerId)

          maybeLastEntry.flatMap(_.findDuplicateBatch(batch)).foreach { duplicate =>
            return (updatedProducers, completedTxns.toList, Some(duplicate))
          }
        }

        // We cache offset metadata for the start of each transaction. This allows us to
        // compute the last stable offset without relying on additional index lookups.
        val firstOffsetMetadata = if (batch.isTransactional)
          Some(LogOffsetMetadata(batch.baseOffset, appendOffsetMetadata.segmentBaseOffset, relativePositionInSegment))
        else
          None

        val maybeCompletedTxn = updateProducers(producerStateManager, batch, updatedProducers, firstOffsetMetadata, origin, Some(openedTimeStamp))
        maybeCompletedTxn.foreach(completedTxns += _)
      }

      relativePositionInSegment += batch.sizeInBytes
    }
    (updatedProducers, completedTxns.toList, None)
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


  /**
   * Asynchronously read messages from the log.
   *
   * @param startOffset   The offset to begin reading at
   * @param maxLength     The maximum number of bytes to read
   * @param isolation     The fetch isolation, which controls the maximum offset we are allowed to read
   * @param minOneMessage If this is true, the first message will be returned even if it exceeds `maxLength` (if one exists)
   * @return The fetch data information including fetch starting offset metadata and messages read.
   */
  override def readAsync(startOffset: Long,
                         maxLength: Int,
                         isolation: FetchIsolation,
                         minOneMessage: Boolean): CompletableFuture[FetchDataInfo] = {
    try {
      checkLogStartOffset(startOffset)
    } catch {
      case e: OffsetOutOfRangeException =>
        return CompletableFuture.failedFuture(e);
    }
    val maxOffsetMetadata = isolation match {
      case FetchLogEnd => elasticLog.logEndOffsetMetadata
      case FetchHighWatermark => fetchHighWatermarkMetadata
      case FetchTxnCommitted => fetchLastStableOffsetMetadata
    }
    elasticLog.readAsync(startOffset, maxLength, minOneMessage, maxOffsetMetadata, isolation == FetchTxnCommitted)
  }

  override def close(): CompletableFuture[Void] = {
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
      elasticLog.close()
    }
    elasticLog.segments.clear()
    closeFuture.whenComplete((_, _) => {
      elasticLog.isMemoryMappedBufferClosed = true
      elasticLog.deleteEmptyDir()
    })
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
                              origin: AppendOrigin,
                              logOpenedTimestamp: Option[Long] = None): Option[CompletedTxn] = {
    val producerId = batch.producerId
    val appendInfo = producers.getOrElseUpdate(producerId, producerStateManager.prepareUpdate(producerId, origin))
    appendInfo.append(batch, firstOffsetMetadata, logOpenedTimestamp)
  }
}
