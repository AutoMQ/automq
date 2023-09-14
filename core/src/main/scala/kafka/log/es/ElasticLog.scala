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

import io.netty.buffer.Unpooled
import kafka.log._
import kafka.log.es.api.{Client, CreateStreamOptions, KeyValue, OpenStreamOptions}
import kafka.log.es.metrics.Timer
import kafka.server.checkpoints.LeaderEpochCheckpointFile
import kafka.server.epoch.EpochEntry
import kafka.server.{LogDirFailureChannel, LogOffsetMetadata}
import kafka.utils.{CoreUtils, Logging, Scheduler}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.utils.{ThreadUtils, Time}

import java.io.File
import java.nio.ByteBuffer
import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.control.Breaks.{break, breakable}

/**
 * An append-only log for storing messages in elastic stream. The log is a sequence of LogSegments, each with a base offset.
 * New log segments are created according to a configurable policy that controls the size in bytes or time interval
 * for a given segment.
 *
 * NOTE: this class is not thread-safe, and it relies on the thread safety provided by the Log class.
 *
 * @param metaStream                The meta stream
 * @param streamManager             The stream manager
 * @param streamSliceManager        The stream slice manager
 * @param producerStateManager      The producer state manager
 * @param logSegmentManager         The log segment manager.
 * @param partitionMeta             The partition meta
 * @param leaderEpochCheckpointMeta The leader epoch checkpoint meta
 * @param _dir                      The directory in which log segments are created.
 * @param _config                   The log configuration settings.
 * @param segments                  The non-empty log segments recovered from disk
 * @param nextOffsetMetadata        The offset where the next message could be appended
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
                 _dir: File,
                 _config: LogConfig,
                 segments: LogSegments,
                 @volatile private[log] var nextOffsetMetadata: LogOffsetMetadata,
                 scheduler: Scheduler,
                 time: Time,
                 topicPartition: TopicPartition,
                 logDirFailureChannel: LogDirFailureChannel,
                 val _initStartOffset: Long = 0,
                 leaderEpoch: Long
                ) extends LocalLog(_dir, _config, segments, partitionMeta.getRecoverOffset, nextOffsetMetadata, scheduler, time, topicPartition, logDirFailureChannel, _initStartOffset) {

  import kafka.log.es.ElasticLog._

  this.logIdent = s"[ElasticLog partition=$topicPartition epoch=$leaderEpoch] "
  /**
   * The next valid offset. The records with offset smaller than $confirmOffset has been confirmed by ElasticStream.
   */
  var confirmOffset: AtomicReference[LogOffsetMetadata] = new AtomicReference(nextOffsetMetadata)
  var confirmOffsetChangeListener: Option[() => Unit] = None

  private val appendAckQueue = new LinkedBlockingQueue[Long]()
  private val appendAckThread = APPEND_CALLBACK_EXECUTOR(math.abs(logIdent.hashCode % APPEND_CALLBACK_EXECUTOR.length))

  private var modified = false


  // persist log meta when lazy stream real create
  streamManager.setListener((_, event) => {
    if (event == ElasticStreamMetaEvent.STREAM_DO_CREATE) {
      persistLogMeta()
    }
  })

  override protected def maybeHandleIOException[T](msg: => String)(fun: => T): T = {
    LocalLog.maybeHandleIOException(logDirFailureChannel, _dir.getPath, msg) {
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
    info(s"saved cleanerOffsetCheckpoint: $offsetCheckpoint")
  }

  def persistRecoverOffsetCheckpoint(): Unit = {
    if (partitionMeta.getRecoverOffset == recoveryPoint) {
      return
    }
    partitionMeta.setRecoverOffset(recoveryPoint)
    persistPartitionMeta()
    info(s"saved recoverOffsetCheckpoint: $recoveryPoint")
  }

  def saveLeaderEpochCheckpoint(meta: ElasticLeaderEpochCheckpointMeta): Unit = {
    persistMeta(metaStream, MetaKeyValue.of(MetaStream.LEADER_EPOCH_CHECKPOINT_KEY, ByteBuffer.wrap(meta.encode())))
  }

  def newSegment(baseOffset: Long, time: Time, suffix: String = ""): ElasticLogSegment = {
    // In roll, before new segment, last segment will be inactive by #onBecomeInactiveSegment
    createAndSaveSegment(logSegmentManager, suffix, logIdent = logIdent)(baseOffset, _dir, config, streamSliceManager, time)
  }

  private def persistLogMeta(): Unit = {
    logSegmentManager.persistLogMeta()
  }

  private def persistPartitionMeta(): Unit = {
    persistMeta(metaStream, MetaKeyValue.of(MetaStream.PARTITION_META_KEY, ElasticPartitionMeta.encode(partitionMeta)))
    info(s"${logIdent}save partition meta $partitionMeta")
  }

  override private[log] def append(lastOffset: Long, largestTimestamp: Long, shallowOffsetOfMaxTimestamp: Long, records: MemoryRecords): Unit = {
    val activeSegment = segments.activeSegment
    val startTimestamp = time.nanoseconds()

    val permit = records.sizeInBytes()
    if (!APPEND_PERMIT_SEMAPHORE.tryAcquire(permit)) {
      while (!APPEND_PERMIT_SEMAPHORE.tryAcquire(permit, 1, TimeUnit.SECONDS)) {
        tryAppendStatistics()
      }
      APPEND_PERMIT_ACQUIRE_FAIL_TIMER.update(System.nanoTime() - startTimestamp)
    }

    activeSegment.append(largestOffset = lastOffset, largestTimestamp = largestTimestamp,
      shallowOffsetOfMaxTimestamp = shallowOffsetOfMaxTimestamp, records = records)
    APPEND_TIMER.update(System.nanoTime() - startTimestamp)
    val endOffset = lastOffset + 1
    updateLogEndOffset(endOffset)
    val cf = activeSegment.asInstanceOf[ElasticLogSegment].asyncLogFlush()
    cf.whenComplete((_, _) => {
      APPEND_PERMIT_SEMAPHORE.release(permit)
    })
    cf.thenAccept(_ => {
      APPEND_CALLBACK_TIMER.update(System.nanoTime() - startTimestamp)
      // run callback async by executors to avoid deadlock when asyncLogFlush is called by append thread.
      // append callback executor is single thread executor, so the callback will be executed in order.
      val startNanos = System.nanoTime()
      var notify = false
      breakable {
        while (true) {
          val offset = confirmOffset.get()
          if (offset.messageOffset < endOffset) {
            confirmOffset.compareAndSet(offset, LogOffsetMetadata(endOffset, activeSegment.baseOffset, activeSegment.size))
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

    modified = true
  }

  private def appendCallback(startNanos: Long): Unit = {
    // group notify
    if (appendAckQueue.isEmpty) {
      return
    }
    appendAckQueue.clear()
    confirmOffsetChangeListener.foreach(_.apply())
    APPEND_ACK_TIMER.update(System.nanoTime() - startNanos)

    tryAppendStatistics()
  }

  private def tryAppendStatistics(): Unit = {
    val lastRecordTimestamp = LAST_RECORD_TIMESTAMP.get()
    val now = System.currentTimeMillis()
    if (now - lastRecordTimestamp > 10000 && LAST_RECORD_TIMESTAMP.compareAndSet(lastRecordTimestamp, now)) {
      val permitAcquireFailStatistics = APPEND_PERMIT_ACQUIRE_FAIL_TIMER.getAndReset()
      val remainingPermits = APPEND_PERMIT_SEMAPHORE.availablePermits()
      val appendStatistics = APPEND_TIMER.getAndReset()
      val callbackStatistics = APPEND_CALLBACK_TIMER.getAndReset()
      val ackStatistics = APPEND_ACK_TIMER.getAndReset()
      logger.info(s"log append cost, permitAcquireFail=$permitAcquireFailStatistics, remainingPermit=$remainingPermits/$APPEND_PERMIT ,append=$appendStatistics, callback=$callbackStatistics, ack=$ackStatistics")
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


  /**
   * Closes the segments of the log.
   */
  override private[log] def close(): Unit = {
    CoreUtils.swallow(closeWithFuture().get(), this)
    info("log closed")
  }

  override private[log] def closeWithFuture(): CompletableFuture[Void] = {
    // already flush in UnifiedLog#close, so it's safe to set cleaned shutdown.
    partitionMeta.setCleanedShutdown(true)
    partitionMeta.setStartOffset(logStartOffset)
    partitionMeta.setRecoverOffset(recoveryPoint)

    maybeHandleIOException(s"Error while closing $topicPartition in dir ${dir.getParent}") {
      CoreUtils.swallow(persistPartitionMeta(), this)
      if (modified) {
        // update log size
        CoreUtils.swallow(persistLogMeta(), this)
      }
      CoreUtils.swallow(checkIfMemoryMappedBufferClosed(), this)
      CoreUtils.swallow(segments.close(), this)
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
  private val APPEND_PERMIT = 1024 * 1024 * 1024
  private val APPEND_PERMIT_SEMAPHORE = new Semaphore(APPEND_PERMIT)

  private val LAST_RECORD_TIMESTAMP = new AtomicLong()
  private val APPEND_PERMIT_ACQUIRE_FAIL_TIMER = new Timer()
  private val APPEND_TIMER = new Timer()
  private val APPEND_CALLBACK_TIMER = new Timer()
  private val APPEND_ACK_TIMER = new Timer()
  private val APPEND_CALLBACK_EXECUTOR: Array[ExecutorService] = new Array[ExecutorService](8)

  for (i <- APPEND_CALLBACK_EXECUTOR.indices) {
    APPEND_CALLBACK_EXECUTOR(i) = Executors.newSingleThreadExecutor(ThreadUtils.createThreadFactory("log-append-callback-executor-" + i, true))
  }

  private val META_SCHEDULE_EXECUTOR = Executors.newScheduledThreadPool(1, ThreadUtils.createThreadFactory("log-meta-schedule-executor", true))

  def formatStreamKey(namespace: String, topicPartition: TopicPartition): String = namespace + "/" + topicPartition.topic() + "/" + topicPartition.partition()

  def apply(client: Client, namespace: String, dir: File,
            config: LogConfig,
            scheduler: Scheduler,
            time: Time,
            topicPartition: TopicPartition,
            logDirFailureChannel: LogDirFailureChannel,
            numRemainingSegments: ConcurrentMap[String, Int] = new ConcurrentHashMap[String, Int],
            maxTransactionTimeoutMs: Int,
            producerStateManagerConfig: ProducerStateManagerConfig,
            leaderEpoch: Long): ElasticLog = {
    val logIdent = s"[ElasticLog partition=$topicPartition epoch=$leaderEpoch] "

    val key = formatStreamKey(namespace, topicPartition)
    val kvList = client.kvClient().getKV(java.util.Arrays.asList(key)).get()

    var partitionMeta: ElasticPartitionMeta = null

    // open meta stream
    val metaNotExists = kvList.get(0).value() == null

    var metaStream: MetaStream = null
    var logStreamManager: ElasticLogStreamManager = null

    try {
      metaStream = if (metaNotExists) {
        val stream = createMetaStream(client, key, config.replicationFactor, leaderEpoch, logIdent = logIdent)
        info(s"${logIdent}created a new meta stream: stream_id=${stream.streamId()}")
        stream
      } else {
        val keyValue = kvList.get(0)
        val metaStreamId = Unpooled.wrappedBuffer(keyValue.value()).readLong()
        // open partition meta stream
        val stream = client.streamClient().openStream(metaStreamId, OpenStreamOptions.newBuilder().epoch(leaderEpoch).build())
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

      def loadAllValidSnapshots(): mutable.Map[Long, ElasticPartitionProducerSnapshotMeta] = {
        metaMap.filter(kv => kv._1.startsWith(MetaStream.PRODUCER_SNAPSHOT_KEY_PREFIX))
          .map(kv => (kv._1.stripPrefix(MetaStream.PRODUCER_SNAPSHOT_KEY_PREFIX).toLong, kv._2.asInstanceOf[ElasticPartitionProducerSnapshotMeta]))
      }

      //load producer snapshots for this partition
      val producerSnapshotsMetaOpt = metaMap.get(MetaStream.PRODUCER_SNAPSHOTS_META_KEY).map(m => m.asInstanceOf[ElasticPartitionProducerSnapshotsMeta])
      val (producerSnapshotsMeta, snapshotsMap) = if (producerSnapshotsMetaOpt.isEmpty) {
        // No need to persist if not exists
        (new ElasticPartitionProducerSnapshotsMeta(), new mutable.HashMap[Long, ElasticPartitionProducerSnapshotMeta]())
      } else {
        (producerSnapshotsMetaOpt.get, loadAllValidSnapshots())
      }
      if (snapshotsMap.nonEmpty) {
        info(s"${logIdent}loaded ${snapshotsMap.size} producer snapshots, offsets(filenames) are ${snapshotsMap.keys} ")
      } else {
        info(s"${logIdent}loaded no producer snapshots")
      }

      def persistProducerSnapshotMeta(meta: ElasticPartitionProducerSnapshotMeta): Unit = {
        val key = MetaStream.PRODUCER_SNAPSHOT_KEY_PREFIX + meta.getOffset
        if (meta.isEmpty) {
          // The real deletion of this snapshot is done in metaStream's compaction.
          producerSnapshotsMeta.remove(meta.getOffset)
        } else {
          producerSnapshotsMeta.add(meta.getOffset)
          persistMeta(metaStream, MetaKeyValue.of(key, meta.encode()))
        }
        persistMeta(metaStream, MetaKeyValue.of(MetaStream.PRODUCER_SNAPSHOTS_META_KEY, producerSnapshotsMeta.encode()))
      }

      val producerStateManager = ElasticProducerStateManager(topicPartition, dir,
        maxTransactionTimeoutMs, producerStateManagerConfig, time, snapshotsMap, persistProducerSnapshotMeta)

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
          client.kvClient().delKV(java.util.Arrays.asList(key)).get()
        }, this)
        error(s"${logIdent}failed to open elastic log, trying to close streams and delete key. Error msg: ${e.getMessage}")
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
  def destroy(client: Client, namespace: String, topicPartition: TopicPartition, currentEpoch: Long): Unit = {
    val logIdent = s"[ElasticLog partition=$topicPartition] "

    val key = formatStreamKey(namespace, topicPartition)

    try {
      val kvList = client.kvClient().getKV(java.util.Arrays.asList(key)).get()
      val keyValue = kvList.get(0)
      val metaStreamId = Unpooled.wrappedBuffer(keyValue.value()).readLong()
      // First, open partition meta stream with higher epoch.
      val metaStream = openStreamWithRetry(client, metaStreamId, currentEpoch + 1, logIdent)
      info(s"$logIdent opened meta stream: stream_id=$metaStreamId, epoch=${currentEpoch + 1}")
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
    } finally {
      // remove kv info
      client.kvClient().delKV(java.util.Arrays.asList(key)).get()
    }

    info(s"$logIdent Destroyed with epoch ${currentEpoch + 1}")
  }

  private def openStreamWithRetry(client: Client, streamId: Long, epoch: Long, logIdent: String): MetaStream = {
    client.streamClient()
      .openStream(streamId, OpenStreamOptions.newBuilder().epoch(epoch).build())
      .exceptionally(_ => client.streamClient()
        .openStream(streamId, OpenStreamOptions.newBuilder().build()).join()
      ).thenApply(stream => new MetaStream(stream, META_SCHEDULE_EXECUTOR, logIdent))
      .join()
  }

  private[es] def createMetaStream(client: Client, key: String, replicaCount: Int, leaderEpoch: Long, logIdent: String): MetaStream = {
    val metaStream = client.streamClient().createAndOpenStream(CreateStreamOptions.newBuilder()
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
    client.kvClient().putKV(java.util.Arrays.asList(KeyValue.of(key, valueBuf))).get()
    metaStream
  }

  private def persistMeta(metaStream: MetaStream, metaKeyValue: MetaKeyValue): Unit = {
    metaStream.appendSync(metaKeyValue)
  }

  /**
   * Create a new segment and save the meta in metaStream if needed. This method can be used to create a new normal segment or a new cleaned segment.
   * For the newly created segment, the meta will immediately be saved in metaStream.
   * For the newly created cleaned segment, the meta should not be saved here. It will be saved iff the replacement happens.
   */
  private def createAndSaveSegment(logSegmentManager: ElasticLogSegmentManager, suffix: String = "", logIdent: String)(baseOffset: Long, dir: File,
                                                                                                                       config: LogConfig, streamSliceManager: ElasticStreamSliceManager, time: Time): ElasticLogSegment = {
    if (!suffix.equals("") && !suffix.equals(LocalLog.CleanedFileSuffix)) {
      throw new IllegalArgumentException("suffix must be empty or " + LocalLog.CleanedFileSuffix)
    }
    val meta = new ElasticStreamSegmentMeta()
    meta.baseOffset(baseOffset)
    meta.streamSuffix(suffix)
    val segment: ElasticLogSegment = ElasticLogSegment(dir, meta, streamSliceManager, config, time, logSegmentManager.logSegmentEventListener())
    if (suffix.equals("")) {
      logSegmentManager.put(baseOffset, segment)
      logSegmentManager.persistLogMeta()
    }

    info(s"${logIdent}Created a new log segment with baseOffset = $baseOffset, suffix = $suffix")
    segment
  }
}