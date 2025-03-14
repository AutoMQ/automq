/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.log.streamaspect

import com.automq.stream.api.Client
import com.automq.stream.utils.FutureUtil
import kafka.cluster.PartitionSnapshot
import kafka.log._
import kafka.log.streamaspect.ElasticUnifiedLog.{CheckpointExecutor, MaxCheckpointIntervalBytes, MinCheckpointIntervalMs}
import kafka.server._
import kafka.utils.Logging
import org.apache.kafka.common.errors.OffsetOutOfRangeException
import org.apache.kafka.common.errors.s3.StreamFencedException
import org.apache.kafka.common.record.{MemoryRecords, RecordVersion}
import org.apache.kafka.common.utils.{ThreadUtils, Time}
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.server.common.{MetadataVersion, OffsetAndEpoch}
import org.apache.kafka.server.util.Scheduler
import org.apache.kafka.storage.internals.epoch.LeaderEpochFileCache
import org.apache.kafka.storage.internals.log._

import java.io.File
import java.nio.ByteBuffer
import java.nio.file.Path
import java.util
import java.util.concurrent.atomic.LongAdder
import java.util.concurrent.{CompletableFuture, ConcurrentHashMap, CopyOnWriteArrayList, Executors}
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.util.{Failure, Success, Try}

class ElasticUnifiedLog(_logStartOffset: Long,
    elasticLog: ElasticLog,
    brokerTopicStats: BrokerTopicStats,
    producerIdExpirationCheckIntervalMs: Int,
    _leaderEpochCache: Option[LeaderEpochFileCache],
    producerStateManager: ProducerStateManager,
    __topicId: Option[Uuid],
    logOffsetsListener: LogOffsetsListener,
    var snapshotRead: Boolean,
)
    extends UnifiedLog(_logStartOffset, elasticLog, brokerTopicStats, producerIdExpirationCheckIntervalMs,
        _leaderEpochCache, producerStateManager, __topicId, false, false, logOffsetsListener) {

    var confirmOffsetChangeListener: Option[() => Unit] = None

    elasticLog.confirmOffsetChangeListener = Some(() => confirmOffsetChangeListener.map(_.apply()))

    // fuzzy interval bytes for checkpoint, it's ok not thread safe
    var checkpointIntervalBytes = 0
    var lastCheckpointTimestamp = time.milliseconds()
    var configChangeListeners = new CopyOnWriteArrayList[LogConfigChangeListener]()

    def getLocalLog(): ElasticLog = elasticLog

    def confirmOffset(): LogOffsetMetadata = {
        elasticLog.confirmOffset
    }

    override def appendAsLeader(
        records: MemoryRecords,
        leaderEpoch: Int,
        origin: AppendOrigin,
        interBrokerProtocolVersion: MetadataVersion,
        requestLocal: RequestLocal,
        verificationGuard: VerificationGuard = VerificationGuard.SENTINEL
    ): LogAppendInfo = {
        val size = records.sizeInBytes()
        checkpointIntervalBytes += size
        ElasticUnifiedLog.DirtyBytes.add(size)
        val rst = super.appendAsLeader(records, leaderEpoch, origin, interBrokerProtocolVersion, requestLocal, verificationGuard)
        if (checkpointIntervalBytes > MaxCheckpointIntervalBytes && time.milliseconds() - lastCheckpointTimestamp > MinCheckpointIntervalMs) {
            checkpointIntervalBytes = 0
            lastCheckpointTimestamp = time.milliseconds()
            CheckpointExecutor.execute(() => checkpoint())
        }
        rst
    }

    def tryCheckpoint(): Boolean = {
        if (checkpointIntervalBytes > 0) {
            checkpointIntervalBytes = 0
            lastCheckpointTimestamp = time.milliseconds()
            checkpoint()
            true
        } else {
            false
        }
    }

    private def checkpoint(): Unit = {
        val snapshotCf = lock.synchronized {
            // https://github.com/AutoMQ/automq-for-kafka/issues/798
            // guard snapshot with log lock
            val confirmOffset = elasticLog.confirmOffset
            val cf = producerStateManager.asInstanceOf[ElasticProducerStateManager].takeSnapshotAndRemoveExpired(elasticLog.recoveryPoint)
            if (confirmOffset != null) {
                elasticLog.updateRecoveryPoint(confirmOffset.messageOffset)
            }
            cf
        }
        snapshotCf.get()
        elasticLog.persistRecoverOffsetCheckpoint()
    }

    override private[log] def replaceSegments(newSegments: collection.Seq[LogSegment],
        oldSegments: collection.Seq[LogSegment]): Unit = {
        val deletedSegments = elasticLog.replaceSegments(newSegments, oldSegments)
        deleteProducerSnapshots(deletedSegments, asyncDelete = true)
    }

    override private[log] def splitOverflowedSegment(segment: LogSegment) = {
        // normally, there should be no overflowed segment
        throw new UnsupportedOperationException()
    }

    override protected def initializeTopicId(): Unit = {
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
            case Success(_) => elasticLog.readAsync(startOffset, maxLength, minOneMessage, maxOffsetMetadata(isolation), isolation == FetchIsolation.TXN_COMMITTED)
            case Failure(e: OffsetOutOfRangeException) => CompletableFuture.failedFuture(e)
            case Failure(e) => throw e
        }
    }

    /**
     * Get the max offset metadata of the log based on the isolation level
     */
    def maxOffsetMetadata(isolation: FetchIsolation): LogOffsetMetadata = {
        isolation match {
            case FetchIsolation.LOG_END => elasticLog.logEndOffsetMetadata
            case FetchIsolation.HIGH_WATERMARK => fetchHighWatermarkMetadata
            case FetchIsolation.TXN_COMMITTED => fetchLastStableOffsetMetadata
        }
    }

    /**
     * Create a fetch data info with no messages
     */
    def emptyFetchDataInfo(maxOffsetMetadata: LogOffsetMetadata, isolation: FetchIsolation): FetchDataInfo = {
        LocalLog.emptyFetchDataInfo(maxOffsetMetadata, isolation == FetchIsolation.TXN_COMMITTED)
    }

    override protected def updateLogStartOffset(offset: Long): Unit = {
        logStartOffset = offset
        // AutoMQ inject start
        localLog.asInstanceOf[ElasticLog].updateLogStartOffset(offset)
        // AutoMQ inject end

        if (highWatermark < offset) {
            updateHighWatermark(offset)
        }

        if (localLog.recoveryPoint < offset) {
            localLog.updateRecoveryPoint(offset)
        }
    }

    override def close(): Unit = {
        ElasticUnifiedLog.Logs.remove(elasticLog.topicPartition, this)
        lock synchronized {
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
        // graceful await append ack
        elasticLog.lastAppendAckFuture.get()
        elasticLog.isMemoryMappedBufferClosed = true
        elasticLog.deleteEmptyDir()
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

    override def createNewCleanedSegment(dir: File,
        logConfig: LogConfig,
        baseOffset: Long): LogSegment = {
        localLog.createNewCleanedSegment(dir, logConfig, baseOffset)
    }

    override def flushProducerStateSnapshot(
        snapshot: Path): Unit = {
        // noop implementation, producer snapshot and recover point will be appended to MetaStream, so they have order relation.
    }

    override def updateConfig(
      newConfig: LogConfig): LogConfig = {
        val config = super.updateConfig(newConfig)
        for (listener <- configChangeListeners.asScala) {
          try {
            listener.onLogConfigChange(this, newConfig)
          } catch {
            case e: Throwable =>
              error(s"Error while invoking config change listener $listener", e)
          }
        }
        config
    }

    override def endOffsetForEpoch(leaderEpoch: Int): Option[OffsetAndEpoch] = {
        if (snapshotRead) {
            Option(new OffsetAndEpoch(logEndOffset, leaderEpoch))
        } else {
            super.endOffsetForEpoch(leaderEpoch)
        }
    }

    // only used for test
    def listProducerSnapshots(): util.NavigableMap[java.lang.Long, ByteBuffer] = {
        producerStateManager.asInstanceOf[ElasticProducerStateManager].snapshotsMap
    }

    def addConfigChangeListener(listener: LogConfigChangeListener): Unit = {
        configChangeListeners.add(listener)
    }

    def snapshot(snapshot: PartitionSnapshot.Builder): Unit = {
        snapshot.firstUnstableOffset(firstUnstableOffsetMetadata.orNull)
        val localLog = getLocalLog()
        localLog.snapshot(snapshot)
    }

    def snapshot(snapshot: PartitionSnapshot): Unit = {
        val localLog = getLocalLog()
        localLog.snapshot(snapshot)
        if (snapshot.firstUnstableOffset() == null) {
            firstUnstableOffsetMetadata = None
        } else {
            var offset = snapshot.firstUnstableOffset()
            val segmentBaseOffset = localLog.segments.floorSegment(offset.messageOffset).get().baseOffset()
            offset = new LogOffsetMetadata(offset.messageOffset, segmentBaseOffset, offset.relativePositionInSegment)
            firstUnstableOffsetMetadata = Some(offset)
        }
        if (snapshot.logMeta() != null) {
            val opt = localLog.segments.firstSegmentBaseOffset()
            opt.ifPresent(baseOffset => {
                updateLogStartOffset(baseOffset)
            })
        }
        highWatermarkMetadata = localLog.logEndOffsetMetadata

    }

}

object ElasticUnifiedLog extends Logging {
    private val CheckpointExecutor = Executors.newSingleThreadScheduledExecutor(ThreadUtils.createThreadFactory("checkpoint-executor", true))
    private val MaxCheckpointIntervalBytes = 50 * 1024 * 1024
    private val MinCheckpointIntervalMs = 10 * 1000
    private val Logs = new ConcurrentHashMap[TopicPartition, ElasticUnifiedLog]()
    // fuzzy dirty bytes for checkpoint, it's ok not thread safe
    private val DirtyBytes = new LongAdder()
    private val MaxDirtyBytes = 5L * 1024 * 1024 * 1024 // 5GiB, when the object size is 500MiB, the log recover only need to read at most 10 objects

    CheckpointExecutor.scheduleWithFixedDelay(() => fullCheckpoint(), 1, 1, java.util.concurrent.TimeUnit.MINUTES)

    def apply(
        dir: File,
        config: LogConfig,
        scheduler: Scheduler,
        time: Time,
        maxTransactionTimeoutMs: Int,
        producerStateManagerConfig: ProducerStateManagerConfig,
        brokerTopicStats: BrokerTopicStats,
        producerIdExpirationCheckIntervalMs: Int,
        logDirFailureChannel: LogDirFailureChannel,
        topicId: Option[Uuid],
        leaderEpoch: Long = 0,
        logOffsetsListener: LogOffsetsListener,
        client: Client,
        namespace: String,
        openStreamChecker: OpenStreamChecker,
        snapshotRead: Boolean = false
    ): ElasticUnifiedLog = {
        val topicPartition = UnifiedLog.parseTopicPartitionName(dir)
        val partitionLogDirFailureChannel = new PartitionLogDirFailureChannel(logDirFailureChannel, dir.getPath);
        LocalLog.maybeHandleIOException(partitionLogDirFailureChannel, dir.getPath, s"failed to open ElasticUnifiedLog $topicPartition in dir $dir") {
            val start = System.currentTimeMillis()
            var localLog: ElasticLog = null
            while(localLog == null) {
                try {
                    localLog = ElasticLog(client, namespace, dir, config, scheduler, time, topicPartition,
                        partitionLogDirFailureChannel, new ConcurrentHashMap[String, Int](), maxTransactionTimeoutMs,
                        producerStateManagerConfig, topicId, leaderEpoch, openStreamChecker, snapshotRead)
                } catch {
                    case e: Throwable =>
                        val cause = FutureUtil.cause(e)
                        cause match {
                            case e1: StreamFencedException => throw e1
                            case e1: Throwable =>
                                error(s"open $topicPartition failed, retry open after 1s", e)
                                Thread.sleep(1000)
                        }
                }
            }
            val leaderEpochFileCache = ElasticUnifiedLog.maybeCreateLeaderEpochCache(topicPartition, config.recordVersion, new ElasticLeaderEpochCheckpoint(localLog.leaderEpochCheckpointMeta, localLog.saveLeaderEpochCheckpoint), scheduler)
            // The real logStartOffset should be set by loaded offsets from ElasticLogLoader.
            // Since the real value has been passed to localLog, we just pass it to ElasticUnifiedLog.
            val elasticUnifiedLog = new ElasticUnifiedLog(localLog.logStartOffset,
                localLog,
                brokerTopicStats,
                producerIdExpirationCheckIntervalMs,
                _leaderEpochCache = leaderEpochFileCache,
                localLog.producerStateManager,
                topicId,
                logOffsetsListener,
                snapshotRead
            )
            val timeCost = System.currentTimeMillis() - start
            info(s"ElasticUnifiedLog $topicPartition opened time cost: $timeCost ms")

            if (!snapshotRead) {
                ElasticUnifiedLog.Logs.put(elasticUnifiedLog.getLocalLog().topicPartition, elasticUnifiedLog)
            }

            elasticUnifiedLog
        }
    }

    private def fullCheckpoint(): Unit = {
        if (DirtyBytes.sum() < MaxDirtyBytes) {
            return
        }
        DirtyBytes.reset()
        for (log <- Logs.values().asScala) {
            try {
                if (log.tryCheckpoint()) {
                    // sleep a while to avoid too many checkpoint at the same time, which may cause high append latency
                    Thread.sleep(10)
                }
            } catch {
                case e: Throwable => error("Error while checkpoint", e)
            }
        }
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
        leaderEpochCheckpoint: ElasticLeaderEpochCheckpoint,
        scheduler: Scheduler
    ): Option[LeaderEpochFileCache] = {

        def newLeaderEpochFileCache(): LeaderEpochFileCache = new LeaderEpochFileCache(topicPartition, leaderEpochCheckpoint, scheduler)

        if (recordVersion.precedes(RecordVersion.V2)) {
            None
        } else {
            Some(newLeaderEpochFileCache())
        }
    }
}
