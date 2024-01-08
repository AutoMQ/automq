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

import com.automq.stream.api.{Client, CreateStreamOptions, KeyValue, OpenStreamOptions}
import io.netty.buffer.Unpooled
import kafka.log._
import kafka.log.streamaspect.ElasticLogFileRecords.{BatchIteratorRecordsAdaptor, PooledMemoryRecords}
import kafka.metrics.{KafkaMetricsGroup, KafkaMetricsUtil}
import kafka.server.checkpoints.LeaderEpochCheckpointFile
import kafka.server.epoch.EpochEntry
import kafka.server.{FetchDataInfo, LogDirFailureChannel, LogOffsetMetadata}
import kafka.utils.{CoreUtils, Logging, Scheduler}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.utils.{ThreadUtils, Time}
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.errors.OffsetOutOfRangeException

import java.io.File
import java.nio.ByteBuffer
import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
import scala.jdk.CollectionConverters._
import scala.util.control.Breaks.{break, breakable}

/**
 * An append-only log for storing messages in elastic stream. The log is a sequence of LogSegments, each with a base offset.
 * New log segments are created according to a configurable policy that controls the size in bytes or time interval
 * for a given segment.
 *
 * NOTE: this class is not thread-safe, and it relies on the thread safety provided by the Log class.
 *
 * NOTE2: If you just want to pass an initial value to super class's variable field,
 * it is better to use another variable to hold the initial value.
 * See https://stackoverflow.com/questions/6656868/why-cant-i-assign-to-var-in-scala-subclass?rq=3.
 *
 * @param metaStream                The meta stream
 * @param streamManager             The stream manager
 * @param streamSliceManager        The stream slice manager
 * @param producerStateManager      The producer state manager
 * @param logSegmentManager         The log segment manager.
 * @param partitionMeta             The partition meta
 * @param leaderEpochCheckpointMeta The leader epoch checkpoint meta
 * @param __dir                     The directory in which log segments are created.
 * @param _config                   The log configuration settings.
 * @param segments                  The non-empty log segments recovered from disk
 * @param _nextOffsetMetadata       The offset where the next message could be appended.
 * @param scheduler                 The thread pool scheduler used for background actions
 * @param time                      The time instance used for checking the clock
 * @param topicPartition            The topic partition associated with this log
 * @param logDirFailureChannel      The LogDirFailureChannel instance to asynchronously handle Log dir failure
 * @param _initStartOffset          The start offset of the log. Only used for log-startOffset initialization
 */
class ElasticLog(val metaStream: MetaStream,
                 val streamManager: ElasticLogStreamManager,
                 val streamSliceManager: ElasticStreamSliceManager,
                 val producerStateManager: ProducerStateManager,
                 val logSegmentManager: ElasticLogSegmentManager,
                 val partitionMeta: ElasticPartitionMeta,
                 val leaderEpochCheckpointMeta: ElasticLeaderEpochCheckpointMeta,
                 __dir: File,
                 _config: LogConfig,
                 segments: LogSegments,
                 _nextOffsetMetadata: LogOffsetMetadata,
                 scheduler: Scheduler,
                 time: Time,
                 topicPartition: TopicPartition,
                 logDirFailureChannel: LogDirFailureChannel,
                 val _initStartOffset: Long = 0,
                 leaderEpoch: Long
                ) extends LocalLog(__dir, _config, segments, partitionMeta.getRecoverOffset, _nextOffsetMetadata, scheduler, time, topicPartition, logDirFailureChannel, _initStartOffset) {

  import ElasticLog._

  this.logIdent = s"[ElasticLog partition=$topicPartition epoch=$leaderEpoch] "
  /**
   * The next valid offset. The records with offset smaller than $confirmOffset has been confirmed by ElasticStream.
   */
  private val _confirmOffset: AtomicReference[LogOffsetMetadata] = new AtomicReference(_nextOffsetMetadata)
  var confirmOffsetChangeListener: Option[() => Unit] = None

  private val appendAckQueue = new LinkedBlockingQueue[Long]()
  private val appendAckThread = APPEND_CALLBACK_EXECUTOR(math.abs(logIdent.hashCode % APPEND_CALLBACK_EXECUTOR.length))

  // persist log meta when lazy stream real create
  streamManager.setListener((_, event) => {
    if (event == ElasticStreamMetaEvent.STREAM_DO_CREATE) {
      logSegmentManager.asyncPersistLogMeta()
    }
  })

  // We only add the partition's path into failureLogDirs instead of the whole logDir.
  override protected def maybeHandleIOException[T](msg: => String)(fun: => T): T = {
    LocalLog.maybeHandleIOException(logDirFailureChannel, _dir.getPath, msg) {
      fun
    }
  }

  // We only add the partition's path into failureLogDirs instead of the whole logDir.
  override protected def maybeHandleIOExceptionAsync[T](msg: => String)(fun: => CompletableFuture[T]): CompletableFuture[T] = {
    LocalLog.maybeHandleIOExceptionAsync(logDirFailureChannel, _dir.getPath, msg) {
      fun
    }
  }

  private def getLogStartOffsetFromMeta: Long = partitionMeta.getStartOffset

  def persistLogStartOffset(): Unit = {
    if (getLogStartOffsetFromMeta == logStartOffset) {
      return
    }
    partitionMeta.setStartOffset(logStartOffset)
    persistPartitionMeta()
    info(s"saved logStartOffset: $logStartOffset")
  }

  // support reading from offsetCheckpointFile
  def getCleanerOffsetCheckpointFromMeta: Long = partitionMeta.getCleanerOffset

  def persistCleanerOffsetCheckpoint(offsetCheckpoint: Long): Unit = {
    if (getCleanerOffsetCheckpointFromMeta == offsetCheckpoint) {
      return
    }
    partitionMeta.setCleanerOffset(offsetCheckpoint)
    persistPartitionMeta()
    if (isDebugEnabled) {
      debug(s"saved cleanerOffsetCheckpoint: $offsetCheckpoint")
    }
  }

  def persistRecoverOffsetCheckpoint(): Unit = {
    if (partitionMeta.getRecoverOffset == recoveryPoint) {
      return
    }
    partitionMeta.setRecoverOffset(recoveryPoint)
    persistPartitionMeta()
    if (isDebugEnabled) {
      debug(s"saved recoverOffsetCheckpoint: $recoveryPoint")
    }
  }

  def saveLeaderEpochCheckpoint(meta: ElasticLeaderEpochCheckpointMeta): Unit = {
    persistMeta(metaStream, MetaKeyValue.of(MetaStream.LEADER_EPOCH_CHECKPOINT_KEY, ByteBuffer.wrap(meta.encode())))
  }

  def newSegment(baseOffset: Long, time: Time, suffix: String = ""): ElasticLogSegment = {
    // In roll, before new segment, last segment will be inactive by #onBecomeInactiveSegment
    val rst = createAndSaveSegment(logSegmentManager, suffix, logIdent = logIdent)(baseOffset, _dir, config, streamSliceManager, time)
    // sync await segment meta persist, cause of if not, when append and node crash, the data will be treated as the previous segment data.
    rst._2.get()
    rst._1
  }

  private def persistLogMeta(): Unit = {
    logSegmentManager.persistLogMeta()
  }

  private def persistPartitionMeta(): Unit = {
    persistMeta(metaStream, MetaKeyValue.of(MetaStream.PARTITION_META_KEY, ElasticPartitionMeta.encode(partitionMeta)))
    if (isDebugEnabled) {
      debug(s"${logIdent}save partition meta $partitionMeta")
    }
  }

  override private[log] def append(lastOffset: Long, largestTimestamp: Long, shallowOffsetOfMaxTimestamp: Long, records: MemoryRecords): Unit = {
    val activeSegment = segments.activeSegment
    val startTimestamp = time.nanoseconds()

    val permit = records.sizeInBytes()
    if (!APPEND_PERMIT_SEMAPHORE.tryAcquire(permit)) {
      while (!APPEND_PERMIT_SEMAPHORE.tryAcquire(permit, 1, TimeUnit.SECONDS)) {
        tryAppendStatistics()
      }
      APPEND_PERMIT_ACQUIRE_FAIL_TIME_HIST.update(System.nanoTime() - startTimestamp)
    }

    activeSegment.append(largestOffset = lastOffset, largestTimestamp = largestTimestamp,
      shallowOffsetOfMaxTimestamp = shallowOffsetOfMaxTimestamp, records = records)

    APPEND_TIME_HIST.update(System.nanoTime() - startTimestamp)
    val endOffset = lastOffset + 1
    updateLogEndOffset(endOffset)
    val cf = activeSegment.asInstanceOf[ElasticLogSegment].asyncLogFlush()
    cf.whenComplete((_, _) => {
      APPEND_PERMIT_SEMAPHORE.release(permit)
    })
    cf.thenAccept(_ => {
      APPEND_CALLBACK_TIME_HIST.update(System.nanoTime() - startTimestamp)
      // run callback async by executors to avoid deadlock when asyncLogFlush is called by append thread.
      // append callback executor is single thread executor, so the callback will be executed in order.
      val startNanos = System.nanoTime()
      var notify = false
      breakable {
        while (true) {
          val offset = _confirmOffset.get()
          if (offset.messageOffset < endOffset) {
            _confirmOffset.compareAndSet(offset, LogOffsetMetadata(endOffset, activeSegment.baseOffset, activeSegment.size))
            notify = true
          } else {
            break()
          }
        }
      }
      if (notify) {
        appendAckQueue.offer(endOffset)
        appendAckThread.submit(new Runnable {
          override def run(): Unit = {
            try {
              appendCallback(startNanos)
            } catch {
              case e: Throwable =>
                error(s"append callback error", e)
            }
          }
        })
      }
    })
  }

  private def appendCallback(startNanos: Long): Unit = {
    // group notify
    if (appendAckQueue.isEmpty) {
      return
    }
    appendAckQueue.clear()
    confirmOffsetChangeListener.foreach(_.apply())
    APPEND_ACK_TIME_HIST.update(System.nanoTime() - startNanos)

    tryAppendStatistics()
  }

  private def tryAppendStatistics(): Unit = {
    val lastRecordTimestamp = LAST_RECORD_TIMESTAMP.get()
    val now = System.currentTimeMillis()
    if (now - lastRecordTimestamp > 60000 && LAST_RECORD_TIMESTAMP.compareAndSet(lastRecordTimestamp, now)) {
      val remainingPermits = APPEND_PERMIT_SEMAPHORE.availablePermits()
      logger.info(s"log append cost, permitAcquireFail=${KafkaMetricsUtil.histToString(APPEND_PERMIT_ACQUIRE_FAIL_TIME_HIST)}, " +
        s"remainingPermit=$remainingPermits/$APPEND_PERMIT, " +
        s"append=${KafkaMetricsUtil.histToString(APPEND_TIME_HIST)}, " +
        s"callback=${KafkaMetricsUtil.histToString(APPEND_CALLBACK_TIME_HIST)}, " +
        s"ack=${KafkaMetricsUtil.histToString(APPEND_ACK_TIME_HIST)}")
      APPEND_PERMIT_ACQUIRE_FAIL_TIME_HIST.clear()
      APPEND_TIME_HIST.clear()
      APPEND_CALLBACK_TIME_HIST.clear()
      APPEND_ACK_TIME_HIST.clear()
    }
  }

  private[log] def confirmOffset: LogOffsetMetadata = {
    val confirmOffset = _confirmOffset.get()
    val offsetUpperBound = logSegmentManager.offsetUpperBound.get()
    if (offsetUpperBound != null && offsetUpperBound.messageOffset < confirmOffset.messageOffset) {
      offsetUpperBound
    } else {
      confirmOffset
    }
  }


  override private[log] def flush(offset: Long): Unit = {
    val currentRecoveryPoint = recoveryPoint
    if (currentRecoveryPoint <= offset) {
      val segmentsToFlush = segments.values(currentRecoveryPoint, offset)
      segmentsToFlush.foreach(_.flush())
    }
  }

  /**
   * Asynchronously read messages from the log.
   *
   * @param startOffset        The offset to begin reading at
   * @param maxLength          The maximum number of bytes to read
   * @param minOneMessage      If this is true, the first message will be returned even if it exceeds `maxLength` (if one exists)
   * @param maxOffsetMetadata  The metadata of the maximum offset to be fetched
   * @param includeAbortedTxns If true, aborted transactions are included
   * @return The fetch data information including fetch starting offset metadata and messages read.
   */
  def readAsync(startOffset: Long,
                maxLength: Int,
                minOneMessage: Boolean,
                maxOffsetMetadata: LogOffsetMetadata,
                includeAbortedTxns: Boolean): CompletableFuture[FetchDataInfo] = {
    maybeHandleIOExceptionAsync(s"Exception while reading from $topicPartition in dir ${dir.getParent}") {
      trace(s"Reading maximum $maxLength bytes at offset $startOffset from log with " +
        s"total length ${segments.sizeInBytes} bytes")
      // get LEO from super class
      val endOffsetMetadata = nextOffsetMetadata
      val endOffset = endOffsetMetadata.messageOffset
      val segmentOpt = segments.floorSegment(startOffset)

      var finalSegmentOpt: Option[LogSegment] = None

      def readFromSegment(segOpt: Option[LogSegment]): CompletableFuture[FetchDataInfo] = {
        if (segOpt.isEmpty) {
          CompletableFuture.completedFuture(null)
        } else {
          val segment = segOpt.get
          val baseOffset = segment.baseOffset

          val maxPosition =
          // Use the max offset position if it is on this segment; otherwise, the segment size is the limit.
            if (maxOffsetMetadata.segmentBaseOffset == segment.baseOffset) maxOffsetMetadata.relativePositionInSegment
            else segment.size

          segment.readAsync(startOffset, maxLength, maxPosition, maxOffsetMetadata.messageOffset, minOneMessage)
            .thenCompose(dataInfo => {
              if (dataInfo != null) {
                finalSegmentOpt = segOpt
                CompletableFuture.completedFuture(dataInfo)
              } else {
                readFromSegment(segments.higherSegment(baseOffset))
              }
            })
        }
      }

      // return error on attempt to read beyond the log end offset
      if (startOffset > endOffset || segmentOpt.isEmpty)
        CompletableFuture.failedFuture(new OffsetOutOfRangeException(s"Received request for offset $startOffset for partition $topicPartition, " +
          s"but we only have log segments upto $endOffset."))
      else if (startOffset == maxOffsetMetadata.messageOffset)
        CompletableFuture.completedFuture(LocalLog.emptyFetchDataInfo(maxOffsetMetadata, includeAbortedTxns))
      else if (startOffset > maxOffsetMetadata.messageOffset)
        CompletableFuture.completedFuture(LocalLog.emptyFetchDataInfo(convertToOffsetMetadataOrThrow(startOffset), includeAbortedTxns))
      else {
        // Do the read on the segment with a base offset less than the target offset
        // but if that segment doesn't contain any messages with an offset greater than that
        // continue to read from successive segments until we get some messages or we reach the end of the log
        readFromSegment(segmentOpt).thenApply(fetchDataInfo => {
          if (fetchDataInfo == null) {
            // okay we are beyond the end of the last segment with no data fetched although the start offset is in range,
            // this can happen when all messages with offset larger than start offsets have been deleted.
            // In this case, we will return the empty set with log end offset metadata
            FetchDataInfo(nextOffsetMetadata, MemoryRecords.EMPTY)
          } else {
            if (includeAbortedTxns) {
              val upperBoundOpt = fetchDataInfo.records match {
                case records: PooledMemoryRecords =>
                  Some(records.lastOffset())
                case adapter: BatchIteratorRecordsAdaptor =>
                  Some(adapter.lastOffset())
                case _ =>
                  None
              }
              addAbortedTransactions(startOffset, finalSegmentOpt.get, fetchDataInfo, upperBoundOpt)
            } else {
              fetchDataInfo
            }
          }
        })
      }
    }
  }

  /**
   * ref. LocalLog#replcaseSegments
   */
  private[log] def replaceSegments(newSegments: collection.Seq[LogSegment], oldSegments: collection.Seq[LogSegment]): Iterable[LogSegment] = {
    val existingSegments = segments
    val sortedNewSegments = newSegments.sortBy(_.baseOffset)
    // Some old segments may have been removed from index and scheduled for async deletion after the caller reads segments
    // but before this method is executed. We want to filter out those segments to avoid calling deleteSegmentFiles()
    // multiple times for the same segment.
    val sortedOldSegments = oldSegments.filter(seg => existingSegments.contains(seg.baseOffset)).sortBy(_.baseOffset)

    // add new segments
    sortedNewSegments.reverse.foreach(segment => {
      existingSegments.add(segment)
      logSegmentManager.put(segment.baseOffset, segment.asInstanceOf[ElasticLogSegment])
    })
    val newSegmentBaseOffsets = sortedNewSegments.map(_.baseOffset).toSet

    // deleted but not replaced segments
    val deletedNotReplaced = sortedOldSegments.map(seg => {
      // Do not remove the segment if (seg.baseOffset == sortedNewSegments.head.baseOffset). It is actually the newly replaced segment.
      if (seg.baseOffset != sortedNewSegments.head.baseOffset) {
        existingSegments.remove(seg.baseOffset)
        logSegmentManager.remove(seg.baseOffset)
      }
      seg.close()
      if (newSegmentBaseOffsets.contains(seg.baseOffset)) Option.empty else Some(seg)
    }).filter(item => item.isDefined).map(item => item.get)

    persistLogMeta()
    deletedNotReplaced
  }

  override private[log] def close(): CompletableFuture[Void] = {
    // already flush in UnifiedLog#close, so it's safe to set cleaned shutdown.
    partitionMeta.setCleanedShutdown(true)
    partitionMeta.setStartOffset(logStartOffset)
    partitionMeta.setRecoverOffset(recoveryPoint)

    maybeHandleIOException(s"Error while closing $topicPartition in dir ${dir.getParent}") {
      CoreUtils.swallow(persistLogMeta(), this)
      CoreUtils.swallow(checkIfMemoryMappedBufferClosed(), this)
      CoreUtils.swallow(segments.close(), this)
      CoreUtils.swallow(persistPartitionMeta(), this)
    }
    info("log(except for streams) closed")
    closeStreams()
  }

  /**
   * Directly close all streams of the log.
   */
  def closeStreams(): CompletableFuture[Void] = {
    CompletableFuture.allOf(streamManager.close(), metaStream.close())
  }
}

object ElasticLog extends Logging {
  private val APPEND_PERMIT = 100 * 1024 * 1024
  private val APPEND_PERMIT_SEMAPHORE = new Semaphore(APPEND_PERMIT)

  private val LAST_RECORD_TIMESTAMP = new AtomicLong()
  private val APPEND_PERMIT_ACQUIRE_FAIL_TIME_HIST = KafkaMetricsGroup.newHistogram("AppendPermitAcquireFailTimeNanos")
  private val APPEND_TIME_HIST = KafkaMetricsGroup.newHistogram("AppendTimeNanos")
  private val APPEND_CALLBACK_TIME_HIST = KafkaMetricsGroup.newHistogram("AppendCallbackTimeNanos")
  private val APPEND_ACK_TIME_HIST = KafkaMetricsGroup.newHistogram("AppendAckTimeNanos")
  private val APPEND_CALLBACK_EXECUTOR: Array[ExecutorService] = new Array[ExecutorService](8)

  for (i <- APPEND_CALLBACK_EXECUTOR.indices) {
    APPEND_CALLBACK_EXECUTOR(i) = Executors.newSingleThreadExecutor(ThreadUtils.createThreadFactory("log-append-callback-executor-" + i, true))
  }

  private val META_SCHEDULE_EXECUTOR = Executors.newScheduledThreadPool(1, ThreadUtils.createThreadFactory("log-meta-schedule-executor", true))

  private def formatStreamKey(namespace: String, topicPartition: TopicPartition, topicId: Uuid): String = namespace + "/" + topicId.toString + "/" + topicPartition.partition()

  def apply(client: Client, namespace: String, dir: File,
            config: LogConfig,
            scheduler: Scheduler,
            time: Time,
            topicPartition: TopicPartition,
            logDirFailureChannel: LogDirFailureChannel,
            numRemainingSegments: ConcurrentMap[String, Int] = new ConcurrentHashMap[String, Int],
            maxTransactionTimeoutMs: Int,
            producerStateManagerConfig: ProducerStateManagerConfig,
            topicId: Uuid,
            leaderEpoch: Long,
            executorService: ExecutorService): ElasticLog = {
    val logIdent = s"[ElasticLog partition=$topicPartition epoch=$leaderEpoch] "

    val key = formatStreamKey(namespace, topicPartition, topicId)
    val value = client.kvClient().getKV(KeyValue.Key.of(key)).get()

    var partitionMeta: ElasticPartitionMeta = null

    // open meta stream
    val metaNotExists = value.isNull

    var metaStream: MetaStream = null
    var logStreamManager: ElasticLogStreamManager = null

    try {
      metaStream = if (metaNotExists) {
        val stream = createMetaStream(client, key, config.replicationFactor, leaderEpoch, logIdent = logIdent)
        info(s"${logIdent}created a new meta stream: stream_id=${stream.streamId()}")
        stream
      } else {
        val metaStreamId = Unpooled.wrappedBuffer(value.get()).readLong()
        // open partition meta stream
        val stream = client.streamClient().openStream(metaStreamId, OpenStreamOptions.builder().epoch(leaderEpoch).build())
          .thenApply(stream => new MetaStream(stream, META_SCHEDULE_EXECUTOR, logIdent))
          .get()
        info(s"${logIdent}opened existing meta stream: stream_id=$metaStreamId")
        stream
      }
      // fetch metas(log meta, producer snapshot, partition meta, ...) from meta stream
      val metaMap = metaStream.replay().asScala

      // load meta info for this partition
      val partitionMetaOpt = metaMap.get(MetaStream.PARTITION_META_KEY).map(m => m.asInstanceOf[ElasticPartitionMeta])
      if (partitionMetaOpt.isEmpty) {
        partitionMeta = new ElasticPartitionMeta(0, 0, 0)
        persistMeta(metaStream, MetaKeyValue.of(MetaStream.PARTITION_META_KEY, ElasticPartitionMeta.encode(partitionMeta)))
      } else {
        partitionMeta = partitionMetaOpt.get
      }
      info(s"${logIdent}loaded partition meta: $partitionMeta")

      //load producer snapshots for this partition
      val producerSnapshotsMeta = metaMap.get(MetaStream.PRODUCER_SNAPSHOTS_META_KEY).map(m => m.asInstanceOf[ElasticPartitionProducerSnapshotsMeta]).getOrElse(new ElasticPartitionProducerSnapshotsMeta())
      val snapshotsMap = producerSnapshotsMeta.getSnapshots
      if (!snapshotsMap.isEmpty) {
        info(s"${logIdent}loaded ${snapshotsMap.size} producer snapshots, offsets(filenames) are ${snapshotsMap.keySet()} ")
      } else {
        info(s"${logIdent}loaded no producer snapshots")
      }

      val producerStateManager = ElasticProducerStateManager(topicPartition, dir,
        maxTransactionTimeoutMs, producerStateManagerConfig, time, snapshotsMap, kv => metaStream.append(kv).thenApply(_ => null))

      val logMeta: ElasticLogMeta = metaMap.get(MetaStream.LOG_META_KEY).map(m => m.asInstanceOf[ElasticLogMeta]).getOrElse(new ElasticLogMeta())
      logStreamManager = new ElasticLogStreamManager(logMeta.getStreamMap, client.streamClient(), config.replicationFactor, leaderEpoch)
      val streamSliceManager = new ElasticStreamSliceManager(logStreamManager)

      val logSegmentManager = new ElasticLogSegmentManager(metaStream, logStreamManager, logIdent = logIdent)

      // load LogSegments and recover log
      val segments = new LogSegments(topicPartition)
      val offsets = new ElasticLogLoader(
        logMeta,
        segments,
        logSegmentManager,
        streamSliceManager,
        dir,
        topicPartition,
        config,
        time,
        hadCleanShutdown = partitionMeta.getCleanedShutdown,
        logStartOffsetCheckpoint = partitionMeta.getStartOffset,
        partitionMeta.getRecoverOffset,
        leaderEpochCache = None,
        producerStateManager = producerStateManager,
        numRemainingSegments = numRemainingSegments,
        createAndSaveSegmentFunc = createAndSaveSegment(logSegmentManager, logIdent = logIdent)).load()
      info(s"${logIdent}loaded log meta: $logMeta")

      // load leader epoch checkpoint
      val leaderEpochCheckpointMetaOpt = metaMap.get(MetaStream.LEADER_EPOCH_CHECKPOINT_KEY).map(m => m.asInstanceOf[ElasticLeaderEpochCheckpointMeta])
      val leaderEpochCheckpointMeta = if (leaderEpochCheckpointMetaOpt.isEmpty) {
        val newMeta = new ElasticLeaderEpochCheckpointMeta(LeaderEpochCheckpointFile.CurrentVersion, List.empty[EpochEntry].asJava)
        // save right now.
        persistMeta(metaStream, MetaKeyValue.of(MetaStream.LEADER_EPOCH_CHECKPOINT_KEY, ByteBuffer.wrap(newMeta.encode())))
        newMeta
      } else {
        leaderEpochCheckpointMetaOpt.get
      }
      info(s"${logIdent}loaded leader epoch checkpoint with ${leaderEpochCheckpointMeta.entries.size} entries")
      if (!leaderEpochCheckpointMeta.entries.isEmpty) {
        val lastEntry = leaderEpochCheckpointMeta.entries.get(leaderEpochCheckpointMeta.entries.size - 1)
        info(s"${logIdent}last leaderEpoch entry is: $lastEntry")
      }

      val elasticLog = new ElasticLog(metaStream, logStreamManager, streamSliceManager, producerStateManager, logSegmentManager, partitionMeta, leaderEpochCheckpointMeta, dir, config,
        segments, offsets.nextOffsetMetadata, scheduler, time, topicPartition, logDirFailureChannel, offsets.logStartOffset, leaderEpoch)
      if (partitionMeta.getCleanedShutdown) {
        // set cleanedShutdown=false before append, the mark will be set to true when gracefully close.
        partitionMeta.setCleanedShutdown(false)
        elasticLog.persistPartitionMeta()
      }
      elasticLog
    } catch {
      case e: Throwable =>
        // We have to close streams here since this log has not been added to currentLogs yet. It will not be handled
        // by LogDirFailureChannel.
        CoreUtils.swallow({
          if (metaStream != null) {
            metaStream.close().get
          }
          if (logStreamManager != null) {
            logStreamManager.close().get()
          }
        }, this)
        error(s"${logIdent}failed to open elastic log, trying to close streams. Error msg: ${e.getMessage}")
        throw e
    }


  }

  /**
   * Destroy related streams of the targeted partition.
   *
   * @param client         elastic stream client
   * @param namespace      namespace
   * @param topicPartition topic partition
   * @param currentEpoch   current epoch of the partition
   * @return Unit
   */
  def destroy(client: Client, namespace: String, topicPartition: TopicPartition, topicId: Uuid, currentEpoch: Long): Unit = {
    val logIdent = s"[ElasticLog partition=$topicPartition topicId=$topicId] "

    val key = formatStreamKey(namespace, topicPartition, topicId)
    var metaStreamIdOpt:Option[Long] = None

    try {
      val value = client.kvClient().getKV(KeyValue.Key.of(key)).get()
      val metaStreamId = Unpooled.wrappedBuffer(value.get()).readLong()
      metaStreamIdOpt = Some(metaStreamId)
    } finally {
      // remove kv info
      client.kvClient().delKV(KeyValue.Key.of(key)).get()
    }

    if (metaStreamIdOpt.isEmpty) {
      warn(s"$logIdent meta stream not exists for topicPartition $topicPartition, skip destroy")
      return
    }

    // First, open partition meta stream with higher epoch.
    val metaStream = openStreamWithRetry(client, metaStreamIdOpt.get, currentEpoch + 1, logIdent)
    info(s"$logIdent opened meta stream: stream_id=${metaStreamIdOpt.get}, epoch=${currentEpoch + 1}")
    // fetch metas(log meta, producer snapshot, partition meta, ...) from meta stream
    val metaMap = metaStream.replay().asScala

    metaMap.get(MetaStream.LOG_META_KEY).map(m => m.asInstanceOf[ElasticLogMeta]).foreach(logMeta => {
      // Then, destroy log stream, time index stream, txn stream, ...
      // streamId <0 means the stream is not actually created.
      logMeta.getStreamMap.values().forEach(streamId => if (streamId >= 0) {
        openStreamWithRetry(client, streamId, currentEpoch + 1, logIdent).destroy()
        info(s"$logIdent destroyed stream: stream_id=$streamId, epoch=${currentEpoch + 1}")
      })
    })

    // Finally, destroy meta stream.
    metaStream.destroy()
    info(s"$logIdent Destroyed with epoch ${currentEpoch + 1}")
  }

  private def openStreamWithRetry(client: Client, streamId: Long, epoch: Long, logIdent: String): MetaStream = {
    client.streamClient()
      .openStream(streamId, OpenStreamOptions.builder().epoch(epoch).build())
      .exceptionally(_ => client.streamClient()
        .openStream(streamId, OpenStreamOptions.builder().build()).join()
      ).thenApply(stream => new MetaStream(stream, META_SCHEDULE_EXECUTOR, logIdent))
      .join()
  }

  private[streamaspect] def createMetaStream(client: Client, key: String, replicaCount: Int, leaderEpoch: Long, logIdent: String): MetaStream = {
    val metaStream = client.streamClient().createAndOpenStream(CreateStreamOptions.builder()
        .replicaCount(replicaCount)
        .epoch(leaderEpoch).build()
      ).thenApply(stream => new MetaStream(stream, META_SCHEDULE_EXECUTOR, logIdent))
      .get()
    // save partition meta stream id relation to PM
    val streamId = metaStream.streamId()
    info(s"${logIdent}created meta stream for $key, streamId: $streamId")
    val valueBuf = ByteBuffer.allocate(8)
    valueBuf.putLong(streamId)
    valueBuf.flip()
    client.kvClient().putKVIfAbsent(KeyValue.of(key, valueBuf)).get()
    metaStream
  }

  private def persistMeta(metaStream: MetaStream, metaKeyValue: MetaKeyValue): Unit = {
    metaStream.appendSync(metaKeyValue)
  }

  /**
   * Create a new segment and save the meta in metaStream if needed. This method can be used to create a new normal segment or a new cleaned segment.
   * For the newly created segment, the meta will immediately be saved in metaStream.
   * For the newly created cleaned segment, the meta should not be saved here. It will be saved if the replacement happens.
   */
  private def createAndSaveSegment(logSegmentManager: ElasticLogSegmentManager, suffix: String = "", logIdent: String)
                                  (baseOffset: Long, dir: File, config: LogConfig, streamSliceManager: ElasticStreamSliceManager, time: Time)
  : (ElasticLogSegment, CompletableFuture[Void]) = {
    if (!suffix.equals("") && !suffix.equals(LocalLog.CleanedFileSuffix)) {
      throw new IllegalArgumentException("suffix must be empty or " + LocalLog.CleanedFileSuffix)
    }
    val meta = new ElasticStreamSegmentMeta()
    meta.baseOffset(baseOffset)
    meta.streamSuffix(suffix)
    meta.createTimestamp(time.milliseconds())
    val segment: ElasticLogSegment = ElasticLogSegment(dir, meta, streamSliceManager, config, time, logSegmentManager.logSegmentEventListener(), logIdent = logIdent)
    var metaSaveCf: CompletableFuture[Void] = CompletableFuture.completedFuture(null)
    if (suffix.equals("")) {
      metaSaveCf = logSegmentManager.create(baseOffset, segment)
    } else if (suffix.equals(UnifiedLog.CleanedFileSuffix)) {
      logSegmentManager.putInflightCleaned(baseOffset, segment)
    }

    info(s"${logIdent}Created a new log segment with baseOffset = $baseOffset, suffix = $suffix")
    (segment, metaSaveCf)
  }
}
