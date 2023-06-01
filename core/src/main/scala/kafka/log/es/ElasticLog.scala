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

import com.automq.elasticstream.client.api
import com.automq.elasticstream.client.api.{Client, CreateStreamOptions, KeyValue, OpenStreamOptions}
import io.netty.buffer.Unpooled
import kafka.log._
import kafka.server.checkpoints.LeaderEpochCheckpointFile
import kafka.server.epoch.EpochEntry
import kafka.server.{LogDirFailureChannel, LogOffsetMetadata}
import kafka.utils.{Logging, Scheduler}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.utils.Time

import java.io.File
import java.nio.ByteBuffer
import java.util
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}
import scala.collection.mutable
import scala.jdk.CollectionConverters._

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
 * @param segmentMap                Map of baseOffset to segment. It is generally consistent with the 'segments'. We use it rather than 'segments' to persist segments' info to avoid errors when 1. a new segment is created but has not been added to 'segments'; 2. an existing segment is deleted. 3. the new segment is added to 'segments'.
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
class ElasticLog(val metaStream: api.Stream,
                 val streamManager: ElasticLogStreamManager,
                 val streamSliceManager: ElasticStreamSliceManager,
                 val producerStateManager: ProducerStateManager,
                 val segmentMap: collection.concurrent.Map[Long, ElasticLogSegment],
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
                 val segmentEventListener: ElasticStreamEventListener
                ) extends LocalLog(_dir, _config, segments, partitionMeta.getRecoverOffset, nextOffsetMetadata, scheduler, time, topicPartition, logDirFailureChannel, _initStartOffset) {

  import kafka.log.es.ElasticLog._

  var confirmOffset: AtomicReference[LogOffsetMetadata] = new AtomicReference(nextOffsetMetadata)
  var confirmOffsetChangeListener: Option[() => Unit] = None

  this.logIdent = s"[ElasticLog partition=$topicPartition] "

  // persist log meta when lazy stream real create
  streamManager.setListener((_, event) => {
    if (event == ElasticStreamMetaEvent.STREAM_DO_CREATE) {
      persistLogMeta()
    }
  })

  def logMeta: ElasticLogMeta = {
    elasticLogMeta(streamManager.streams(), segmentMap.toMap)
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
    persistMeta(metaStream, MetaKeyValue.of(LEADER_EPOCH_CHECKPOINT_KEY, ByteBuffer.wrap(meta.encode())))
  }

  def newSegment(baseOffset: Long, time: Time, suffix: String = ""): ElasticLogSegment = {
    // In roll, before new segment, last segment will be inactive by #onBecomeInactiveSegment
    createAndSaveSegment(metaStream, segmentMap, segmentEventListener, suffix, logIdent = logIdent)(baseOffset, _dir, config, streamSliceManager, time)
  }

  private def persistLogMeta(): Unit = {
    persistLogMetaInStream(metaStream, logMeta, logIdent = logIdent)
  }

  private def persistPartitionMeta(): Unit = {
    persistMeta(metaStream, MetaKeyValue.of(PARTITION_META_KEY, ElasticPartitionMeta.encode(partitionMeta)))
    info(s"${logIdent}save partition meta $partitionMeta")
  }


  override private[log] def append(lastOffset: Long, largestTimestamp: Long, shallowOffsetOfMaxTimestamp: Long, records: MemoryRecords): Unit = {
    val activeSegment = segments.activeSegment
    activeSegment.append(largestOffset = lastOffset, largestTimestamp = largestTimestamp,
      shallowOffsetOfMaxTimestamp = shallowOffsetOfMaxTimestamp, records = records)
    updateLogEndOffset(lastOffset + 1)
    activeSegment.asInstanceOf[ElasticLogSegment].asyncLogFlush().thenAccept(_ => {
      confirmOffset.synchronized {
        val offset = confirmOffset.get()
        if (offset.messageOffset < lastOffset + 1) {
          confirmOffset.set(LogOffsetMetadata(lastOffset + 1, activeSegment.baseOffset, activeSegment.size))
          confirmOffsetChangeListener.map(_.apply())
        }
      }
    })
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
      segmentMap.put(segment.baseOffset, segment.asInstanceOf[ElasticLogSegment])
    })
    val newSegmentBaseOffsets = sortedNewSegments.map(_.baseOffset).toSet

    // deleted but not replaced segments
    val deletedNotReplaced = sortedOldSegments.map(seg => {
      // Do not remove the segment if (seg.baseOffset == sortedNewSegments.head.baseOffset). It is actually the newly replaced segment.
      if (seg.baseOffset != sortedNewSegments.head.baseOffset) {
        existingSegments.remove(seg.baseOffset)
        segmentMap.remove(seg.baseOffset)
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
    // already flush in UnifiedLog#close, so it's safe to set cleaned shutdown.
    partitionMeta.setCleanedShutdown(true)
    partitionMeta.setRecoverOffset(recoveryPoint)
    persistPartitionMeta()

    maybeHandleIOException(s"Error while renaming dir for $topicPartition in dir ${dir.getParent}") {
      checkIfMemoryMappedBufferClosed()
      segments.close()
      streamManager.close()
    }
  }
}

object ElasticLog extends Logging {
  private val LOG_META_KEY: String = "LOG"
  private val PRODUCER_SNAPSHOTS_META_KEY: String = "PRODUCER_SNAPSHOTS"
  private val PRODUCER_SNAPSHOT_KEY_PREFIX: String = "PRODUCER_SNAPSHOT_"
  private val PARTITION_META_KEY: String = "PARTITION"
  private val LEADER_EPOCH_CHECKPOINT_KEY: String = "LEADER_EPOCH_CHECKPOINT"

  def apply(client: Client, namespace: String, dir: File,
            config: LogConfig,
            scheduler: Scheduler,
            time: Time,
            topicPartition: TopicPartition,
            logDirFailureChannel: LogDirFailureChannel,
            numRemainingSegments: ConcurrentMap[String, Int] = new ConcurrentHashMap[String, Int],
            maxTransactionTimeoutMs: Int,
            producerStateManagerConfig: ProducerStateManagerConfig): ElasticLog = {
    val logIdent = s"[ElasticLog partition=$topicPartition] "

    val key = namespace + "/" + topicPartition.topic() + "-" + topicPartition.partition()
    val kvList = client.kvClient().getKV(java.util.Arrays.asList(key)).get()

    var partitionMeta: ElasticPartitionMeta = null

    // open meta stream
    val metaNotExists = kvList.get(0).value() == null
    val metaStream = if (metaNotExists) {
      createMetaStream(client, key, config.replicationFactor, logIdent = logIdent)
    } else {
      val keyValue = kvList.get(0)
      val metaStreamId = Unpooled.wrappedBuffer(keyValue.value()).readLong()
      // open partition meta stream
      client.streamClient().openStream(metaStreamId, OpenStreamOptions.newBuilder().build()).get()
    }
    info(s"${logIdent}opened meta stream: ${metaStream.streamId()}")

    // fetch metas(log meta, producer snapshot, partition meta, ...) from meta stream
    val metaMap = getMetas(metaStream, logIdent = logIdent)


    // load meta info for this partition
    val partitionMetaOpt = metaMap.get(PARTITION_META_KEY).map(m => m.asInstanceOf[ElasticPartitionMeta])
    if (partitionMetaOpt.isEmpty) {
      partitionMeta = new ElasticPartitionMeta(0, 0, 0)
      persistMeta(metaStream, MetaKeyValue.of(PARTITION_META_KEY, ElasticPartitionMeta.encode(partitionMeta)))
    } else {
      partitionMeta = partitionMetaOpt.get
    }
    info(s"${logIdent}loaded partition meta: $partitionMeta")

    def loadAllValidSnapshots(): mutable.Map[Long, ElasticPartitionProducerSnapshotMeta] = {
      metaMap.filter(kv => kv._1.startsWith(PRODUCER_SNAPSHOT_KEY_PREFIX))
        .map(kv => (kv._1.stripPrefix(PRODUCER_SNAPSHOT_KEY_PREFIX).toLong, kv._2.asInstanceOf[ElasticPartitionProducerSnapshotMeta]))
    }

    //load producer snapshots for this partition
    val producerSnapshotsMetaOpt = metaMap.get(PRODUCER_SNAPSHOTS_META_KEY).map(m => m.asInstanceOf[ElasticPartitionProducerSnapshotsMeta])
    val (producerSnapshotMeta, snapshotsMap) = if (producerSnapshotsMetaOpt.isEmpty) {
      // No need to persist if not exists
      (ElasticPartitionProducerSnapshotsMeta.EMPTY, new mutable.HashMap[Long, ElasticPartitionProducerSnapshotMeta]())
    } else {
      (producerSnapshotsMetaOpt.get, loadAllValidSnapshots())
    }
    if (snapshotsMap.nonEmpty) {
      info(s"${logIdent}loaded ${snapshotsMap.size} producer snapshots, offsets(filenames) are ${snapshotsMap.keys} ")
    } else {
      info(s"${logIdent}loaded no producer snapshots")
    }

    def persistProducerSnapshotMeta(meta: ElasticPartitionProducerSnapshotMeta): Unit = {
      val key = PRODUCER_SNAPSHOT_KEY_PREFIX + meta.getOffset
      if (meta.isEmpty) {
        // TODO: delete the snapshot
        producerSnapshotMeta.remove(meta.getOffset)
      } else {
        producerSnapshotMeta.add(meta.getOffset)
        persistMeta(metaStream, MetaKeyValue.of(key, meta.encode()))
      }
      persistMeta(metaStream, MetaKeyValue.of(PRODUCER_SNAPSHOTS_META_KEY, producerSnapshotMeta.encode()))
    }

    val producerStateManager = ElasticProducerStateManager(topicPartition, dir,
      maxTransactionTimeoutMs, producerStateManagerConfig, time, snapshotsMap, persistProducerSnapshotMeta)

    val logMeta: ElasticLogMeta = metaMap.get(LOG_META_KEY).map(m => m.asInstanceOf[ElasticLogMeta]).getOrElse(new ElasticLogMeta())
    val logStreamManager = new ElasticLogStreamManager(logMeta.getStreamMap, client.streamClient(), config.replicationFactor)
    val streamSliceManager = new ElasticStreamSliceManager(logStreamManager)
    val segmentMap = new ConcurrentHashMap[Long, ElasticLogSegment]().asScala
    val segmentEventListener = new ElasticStreamEventListener() {
      override def onEvent(baseOffset: Long, event: ElasticStreamMetaEvent): Unit = {
        event match {
          case ElasticStreamMetaEvent.SEGMENT_DELETE =>
            val deleted = segmentMap.remove(baseOffset) != null
            if (deleted) {
              persistLogMetaInStream(metaStream, elasticLogMeta(logStreamManager.streams(), segmentMap.toMap), logIdent = logIdent)
            }
          case ElasticStreamMetaEvent.SEGMENT_UPDATE =>
            persistLogMetaInStream(metaStream, elasticLogMeta(logStreamManager.streams(), segmentMap.toMap), logIdent = logIdent)
          case _ =>
            throw new IllegalStateException(s"Unsupported event $event")
        }
      }
    }

    // load LogSegments and recover log
    val segments = new LogSegments(topicPartition)
    val offsets = new ElasticLogLoader(
      logMeta,
      segments,
      segmentMap,
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
      segmentEventListener = segmentEventListener,
      createAndSaveSegmentFunc = createAndSaveSegment(metaStream, segmentMap, segmentEventListener, logIdent = logIdent)).load()
    info(s"${logIdent}loaded log meta: $logMeta")

    // load leader epoch checkpoint
    val leaderEpochCheckpointMetaOpt = metaMap.get(LEADER_EPOCH_CHECKPOINT_KEY).map(m => m.asInstanceOf[ElasticLeaderEpochCheckpointMeta])
    val leaderEpochCheckpointMeta = if (leaderEpochCheckpointMetaOpt.isEmpty) {
      val newMeta = new ElasticLeaderEpochCheckpointMeta(LeaderEpochCheckpointFile.CurrentVersion, List.empty[EpochEntry].asJava)
      // save right now.
      persistMeta(metaStream, MetaKeyValue.of(LEADER_EPOCH_CHECKPOINT_KEY, ByteBuffer.wrap(newMeta.encode())))
      newMeta
    } else {
      leaderEpochCheckpointMetaOpt.get
    }
    info(s"${logIdent}loaded leader epoch checkpoint with ${leaderEpochCheckpointMeta.entries.size} entries")
    if (!leaderEpochCheckpointMeta.entries.isEmpty) {
      val lastEntry = leaderEpochCheckpointMeta.entries.get(leaderEpochCheckpointMeta.entries.size - 1)
      info(s"${logIdent}last leaderEpoch entry is: $lastEntry")
    }

    val elasticLog = new ElasticLog(metaStream, logStreamManager, streamSliceManager, producerStateManager, segmentMap, partitionMeta, leaderEpochCheckpointMeta, dir, config,
      segments, offsets.nextOffsetMetadata, scheduler, time, topicPartition, logDirFailureChannel, offsets.logStartOffset, segmentEventListener)
    if (partitionMeta.getCleanedShutdown) {
      // set cleanedShutdown=false before append, the mark will be set to true when gracefully close.
      partitionMeta.setCleanedShutdown(false)
      elasticLog.persistPartitionMeta()
    }
    elasticLog
  }

  /**
   * generate the ElasticLog meta dynamically.
   */
  private def elasticLogMeta(streams: util.Map[String, api.Stream], segmentMap: Map[Long, ElasticLogSegment]): ElasticLogMeta = {
    val elasticLogMeta = new ElasticLogMeta()
    val streamMap = new util.HashMap[String, java.lang.Long]()
    streams.entrySet().forEach(entry => {
      streamMap.put(entry.getKey, entry.getValue.streamId())
    })
    elasticLogMeta.setStreamMap(streamMap)
    val segmentList: util.List[ElasticStreamSegmentMeta] = segmentMap.values.map(segment => segment.meta).toList.asJava
    elasticLogMeta.setSegmentMetas(segmentList)
    elasticLogMeta
  }

  private def createMetaStream(client: Client, key: String, replicaCount: Int, logIdent: String): api.Stream = {
    val metaStream = client.streamClient().createAndOpenStream(CreateStreamOptions.newBuilder().replicaCount(replicaCount).build()).get()
    // save partition meta stream id relation to PM
    val streamId = metaStream.streamId()
    info(s"${logIdent}created meta stream for $key, streamId: $streamId")
    val valueBuf = ByteBuffer.allocate(8)
    valueBuf.putLong(streamId)
    valueBuf.flip()
    client.kvClient().putKV(java.util.Arrays.asList(KeyValue.of(key, valueBuf))).get()
    metaStream
  }

  // Note that we should always use the latest log meta to persist.
  private def persistLogMetaInStream(metaStream: api.Stream, logMeta: ElasticLogMeta, logIdent: String): Unit = {
    persistMeta(metaStream, MetaKeyValue.of(LOG_META_KEY, ElasticLogMeta.encode(logMeta)))
    info(s"${logIdent}save log meta $logMeta")
  }

  private def persistMeta(metaStream: api.Stream, metaKeyValue: MetaKeyValue): Unit = {
    metaStream.append(RawPayloadRecordBatch.of(MetaKeyValue.encode(metaKeyValue))).get()
  }

  /**
   * Create a new segment and save the meta in metaStream if needed. This method can be used to create a new normal segment or a new cleaned segment.
   * For the newly created segment, the meta will immediately be saved in metaStream.
   * For the newly created cleaned segment, the meta should not be saved here. It will be saved iff the replacement happens.
   */
  private def createAndSaveSegment(metaStream: api.Stream, segmentMap: collection.concurrent.Map[Long, ElasticLogSegment], segmentEventListener: ElasticStreamEventListener, suffix: String = "", logIdent: String)(baseOffset: Long, dir: File,
                                                                                                                                                                                                                         config: LogConfig, streamSliceManager: ElasticStreamSliceManager, time: Time): ElasticLogSegment = {
    if (!suffix.equals("") && !suffix.equals(LocalLog.CleanedFileSuffix)) {
      throw new IllegalArgumentException("suffix must be empty or " + LocalLog.CleanedFileSuffix)
    }
    val meta = new ElasticStreamSegmentMeta()
    meta.baseOffset(baseOffset)
    meta.streamSuffix(suffix)
    val segment: ElasticLogSegment = ElasticLogSegment(dir, meta, streamSliceManager, config, time, segmentEventListener)
    if (suffix.equals("")) {
      segmentMap.put(baseOffset, segment)
      persistLogMetaInStream(metaStream, elasticLogMeta(streamSliceManager.getStreamManager.streams(), segmentMap.toMap), logIdent = logIdent)
    }

    info(s"${logIdent}Created a new log segment with baseOffset = $baseOffset, suffix = $suffix")
    segment
  }

  private def getMetas(metaStream: api.Stream, logIdent: String): mutable.Map[String, Any] = {
    val startOffset = metaStream.startOffset()
    val endOffset = metaStream.nextOffset()
    val kvMap: mutable.Map[String, ByteBuffer] = mutable.Map()
    // TODO: stream fetch API support fetch by startOffset and endOffset
    // TODO: reverse scan meta stream
    var pos = startOffset
    var done = false
    while (!done) {
      val fetchRst = metaStream.fetch(pos, endOffset, 64 * 1024).get()
      for (recordBatch <- fetchRst.recordBatchList().asScala) {
        // TODO: catch illegal decode
        val kv = MetaKeyValue.decode(recordBatch.rawPayload())
        kvMap.put(kv.getKey, kv.getValue)
        // TODO: stream fetch result add next offset suggest
        pos = recordBatch.lastOffset()
      }
      if (pos >= endOffset) {
        done = true
      }
    }

    val metaMap = mutable.Map[String, Any]()
    kvMap.foreach(kv => {
      kv._1 match {
        case ElasticLog.LOG_META_KEY =>
          metaMap.put(kv._1, ElasticLogMeta.decode(kv._2))
        case ElasticLog.PARTITION_META_KEY =>
          metaMap.put(kv._1, ElasticPartitionMeta.decode(kv._2))
        case snapshot if snapshot.startsWith(ElasticLog.PRODUCER_SNAPSHOT_KEY_PREFIX) =>
          metaMap.put(kv._1, ElasticPartitionProducerSnapshotMeta.decode(kv._2))
        case ElasticLog.PRODUCER_SNAPSHOTS_META_KEY =>
          metaMap.put(kv._1, ElasticPartitionProducerSnapshotsMeta.decode(kv._2))
        case ElasticLog.LEADER_EPOCH_CHECKPOINT_KEY =>
          metaMap.put(kv._1, ElasticLeaderEpochCheckpointMeta.decode(kv._2))
        case _ =>
          error(s"${logIdent}unknown meta key: ${kv._1}")
      }
    })
    metaMap
  }
}