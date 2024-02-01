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
import kafka.log.streamaspect.ElasticUnifiedLog.{CheckpointExecutor, MaxCheckpointIntervalBytes, MinCheckpointIntervalMs}
import kafka.server.epoch.LeaderEpochFileCache
import kafka.server.{BrokerTopicStats, FetchDataInfo, FetchHighWatermark, FetchIsolation, FetchLogEnd, FetchTxnCommitted, LogOffsetMetadata, RequestLocal}
import kafka.utils.Logging
import org.apache.kafka.common.errors.OffsetOutOfRangeException
import org.apache.kafka.common.record.{MemoryRecords, RecordVersion}
import org.apache.kafka.common.utils.ThreadUtils
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.server.common.MetadataVersion

import java.nio.ByteBuffer
import java.util
import java.util.concurrent.{CompletableFuture, Executors}
import scala.util.{Failure, Success, Try}

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

  elasticLog.confirmOffsetChangeListener = Some(() => confirmOffsetChangeListener.map(_.apply()))

  var checkpointIntervalBytes = 0
  var lastCheckpointTimestamp = time.milliseconds()

  def confirmOffset(): LogOffsetMetadata = {
    elasticLog.confirmOffset
  }


  override def appendAsLeader(records: MemoryRecords, leaderEpoch: Int, origin: AppendOrigin, interBrokerProtocolVersion: MetadataVersion, requestLocal: RequestLocal): LogAppendInfo = {
    checkpointIntervalBytes += records.sizeInBytes()
    val rst = super.appendAsLeader(records, leaderEpoch, origin, interBrokerProtocolVersion, requestLocal)
    if (checkpointIntervalBytes > MaxCheckpointIntervalBytes && time.milliseconds() - lastCheckpointTimestamp > MinCheckpointIntervalMs) {
      checkpointIntervalBytes = 0
      lastCheckpointTimestamp = time.milliseconds()
      CheckpointExecutor.execute(() => checkpoint())
    }
    rst
  }

  private def checkpoint(): Unit = {
    producerStateManager.asInstanceOf[ElasticProducerStateManager].takeSnapshotAndRemoveExpired(elasticLog.recoveryPoint)
    flush(true)
    elasticLog.persistRecoverOffsetCheckpoint()
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
    Try(checkLogStartOffset(startOffset)) match {
      case Success(_) => elasticLog.readAsync(startOffset, maxLength, minOneMessage, maxOffsetMetadata(isolation), isolation == FetchTxnCommitted)
      case Failure(e: OffsetOutOfRangeException) => CompletableFuture.failedFuture(e)
      case Failure(e) => throw e
    }
  }

  /**
   * Get the max offset metadata of the log based on the isolation level
   */
  def maxOffsetMetadata(isolation: FetchIsolation): LogOffsetMetadata = {
    isolation match {
      case FetchLogEnd => elasticLog.logEndOffsetMetadata
      case FetchHighWatermark => fetchHighWatermarkMetadata
      case FetchTxnCommitted => fetchLastStableOffsetMetadata
    }
  }

  /**
   * Create a fetch data info with no messages
   */
  def emptyFetchDataInfo(maxOffsetMetadata: LogOffsetMetadata, isolation: FetchIsolation): FetchDataInfo = {
    LocalLog.emptyFetchDataInfo(maxOffsetMetadata, isolation == FetchTxnCommitted)
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

  // only used for test
  def listProducerSnapshots(): util.NavigableMap[java.lang.Long, ByteBuffer] = {
    producerStateManager.asInstanceOf[ElasticProducerStateManager].snapshotsMap
  }
}

object ElasticUnifiedLog extends Logging {
  private val CheckpointExecutor = Executors.newSingleThreadExecutor(ThreadUtils.createThreadFactory("checkpoint-executor", true))
  private val MaxCheckpointIntervalBytes = 50 * 1024 * 1024
  private val MinCheckpointIntervalMs = 10 * 1000

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
}
