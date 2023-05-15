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
import kafka.server.checkpoints.LeaderEpochCheckpointFile
import kafka.server.epoch.EpochEntry
import kafka.server.{LogDirFailureChannel, LogOffsetMetadata}
import kafka.utils.{Logging, Scheduler}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Time
import sdk.elastic.stream.api
import sdk.elastic.stream.api.{Client, CreateStreamOptions, KeyValue, OpenStreamOptions}

import java.io.File
import java.nio.ByteBuffer
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}
import scala.collection.mutable
import scala.jdk.CollectionConverters._

class ElasticLog(val metaStream: api.Stream,
                 val streamManager: ElasticLogStreamManager,
                 val streamSliceManager: ElasticStreamSliceManager,
                 val producerStateManager: ProducerStateManager,
                 val partitionMeta: ElasticPartitionMeta,
                 val leaderEpochCheckpointMeta: ElasticLeaderEpochCheckpointMeta,
                 _dir: File,
                 c: LogConfig,
                 segments: LogSegments,
                 @volatile private[log] var nextOffsetMetadata: LogOffsetMetadata,
                 scheduler: Scheduler,
                 time: Time,
                 topicPartition: TopicPartition,
                 logDirFailureChannel: LogDirFailureChannel)
  extends LocalLog(_dir, c, segments, partitionMeta.getRecoverOffset, nextOffsetMetadata, scheduler, time, topicPartition, logDirFailureChannel, partitionMeta.getStartOffset) {

  import kafka.log.es.ElasticLog._

  this.logIdent = s"[ElasticLog partition=$topicPartition] "

  private var cleanedSegments: List[ElasticLogSegment] = List()

  // persist log meta when lazy stream real create
  streamManager.setListener((_, event) => {
    if (event == LazyStream.Event.CREATE) {
      persistLogMeta()
    }
  })

  def getLogStartOffsetFromMeta: Long = partitionMeta.getStartOffset

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
    if (!suffix.equals("") && !suffix.equals(LocalLog.CleanedFileSuffix)) {
      throw new IllegalArgumentException("suffix must be empty or " + LocalLog.CleanedFileSuffix)
    }
    // In roll, before new segment, last segment will be inactive by #onBecomeInactiveSegment
    val meta = new ElasticStreamSegmentMeta()
    meta.baseOffset(baseOffset)
    meta.streamSuffix(suffix)
    val segment: ElasticLogSegment = ElasticLogSegment(_dir, meta, streamSliceManager, config, time)
    if (suffix.equals(LocalLog.CleanedFileSuffix)) {
      // remove cleanedSegments when replace
      cleanedSegments = cleanedSegments :+ segment
    }
    persistLogMeta()
    info(s"Created a new log segment $baseOffset")
    segment
  }

  private def logMeta(): ElasticLogMeta = {
    val elasticLogMeta = new ElasticLogMeta()
    streamManager.streams().forEach((name, stream) => {
      elasticLogMeta.putStream(name, stream.streamId())
    })
    elasticLogMeta.setSegments(segments.values.map(s => (s.asInstanceOf[ElasticLogSegment]).meta).toList.asJava)
    elasticLogMeta.setCleanedSegments(cleanedSegments.map(s => s.meta).asJava)
    elasticLogMeta
  }

  private def persistLogMeta(): Unit = {
    val elasticLogMeta = logMeta()
    persistMeta(metaStream, MetaKeyValue.of(LOG_META_KEY, ElasticLogMeta.encode(elasticLogMeta)))
    debug(s"saved log meta: $elasticLogMeta")
  }

  private def persistPartitionMeta(): Unit = {
    info(s"save partition meta $partitionMeta")
    persistMeta(metaStream, MetaKeyValue.of(PARTITION_META_KEY, ElasticPartitionMeta.encode(partitionMeta)))
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
  private[log] def replaceSegments(newSegments: collection.Seq[LogSegment], oldSegments: collection.Seq[LogSegment]): Unit = {
    val existingSegments = segments
    val sortedNewSegments = newSegments.sortBy(_.baseOffset)
    // Some old segments may have been removed from index and scheduled for async deletion after the caller reads segments
    // but before this method is executed. We want to filter out those segments to avoid calling deleteSegmentFiles()
    // multiple times for the same segment.
    val sortedOldSegments = oldSegments.filter(seg => existingSegments.contains(seg.baseOffset)).sortBy(_.baseOffset)

    // add new segments
    sortedNewSegments.reverse.foreach(existingSegments.add)
    // delete old segments
    sortedOldSegments.foreach(seg => {
      if (seg.baseOffset != sortedNewSegments.head.baseOffset)
        existingSegments.remove(seg.baseOffset)
      seg.close()
    })
    persistLogMeta()
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
      streamManager.close();
    }
  }
}

object ElasticLog extends Logging {
  val LOG_META_KEY: String = "LOG"
  val PRODUCER_SNAPSHOTS_META_KEY: String = "PRODUCER_SNAPSHOTS"
  val PRODUCER_SNAPSHOT_KEY_PREFIX: String = "PRODUCER_SNAPSHOT_"
  val PARTITION_META_KEY: String = "PARTITION"
  val LEADER_EPOCH_CHECKPOINT_KEY: String = "LEADER_EPOCH_CHECKPOINT"

  def apply(client: Client, dir: File,
            config: LogConfig,
            scheduler: Scheduler,
            time: Time,
            topicPartition: TopicPartition,
            logDirFailureChannel: LogDirFailureChannel,
            numRemainingSegments: ConcurrentMap[String, Int] = new ConcurrentHashMap[String, Int],
            maxTransactionTimeoutMs: Int,
            producerStateManagerConfig: ProducerStateManagerConfig): ElasticLog = {
    this.logIdent = s"[ElasticLog partition=$topicPartition] "

    val key = "/kafka/pm/" + topicPartition.topic() + "-" + topicPartition.partition();
    val kvList = client.kvClient().getKV(java.util.Arrays.asList(key)).get();

    var partitionMeta: ElasticPartitionMeta = null

    // open meta stream
    val metaNotExists = kvList.get(0).value() == null
    val metaStream = if (metaNotExists) {
      createMetaStream(client, key)
    } else {
      val keyValue = kvList.get(0)
      val metaStreamId = Unpooled.wrappedBuffer(keyValue.value()).readLong();
      // open partition meta stream
      client.streamClient().openStream(metaStreamId, OpenStreamOptions.newBuilder().build()).get()
    }
    info(s"opened meta stream: ${metaStream.streamId()}")

    // fetch metas(log meta, producer snapshot, partition meta, ...) from meta stream
    val metaMap = getMetas(metaStream)


    // load meta info for this partition
    val partitionMetaOpt = metaMap.get(PARTITION_META_KEY).map(m => m.asInstanceOf[ElasticPartitionMeta])
    if (partitionMetaOpt.isEmpty) {
      partitionMeta = new ElasticPartitionMeta(0, 0, 0)
      persistMeta(metaStream, MetaKeyValue.of(PARTITION_META_KEY, ElasticPartitionMeta.encode(partitionMeta)))
    } else {
      partitionMeta = partitionMetaOpt.get
    }
    info(s"loaded partition meta: $partitionMeta")

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
        info(s"loaded ${snapshotsMap.size} producer snapshots, offsets(filenames) are ${snapshotsMap.keys} ")
    } else {
      info(s"loaded no producer snapshots")
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

    // load LogSegments and recover log
    val logMeta: ElasticLogMeta = metaMap.get(LOG_META_KEY).map(m => m.asInstanceOf[ElasticLogMeta]).getOrElse(new ElasticLogMeta())
    val logStreamManager = new ElasticLogStreamManager(logMeta.getStreams, client.streamClient())
    val streamSliceManager = new ElasticStreamSliceManager(logStreamManager)
    val segments = new LogSegments(topicPartition)
    val offsets = new ElasticLogLoader(
      logMeta,
      segments,
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
      numRemainingSegments = numRemainingSegments).load()
    info(s"loaded log meta: $logMeta")

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
    info(s"loaded leader epoch checkpoint with ${leaderEpochCheckpointMeta.entries.size} entries")
    if (!leaderEpochCheckpointMeta.entries.isEmpty) {
      val lastEntry = leaderEpochCheckpointMeta.entries.get(leaderEpochCheckpointMeta.entries.size - 1)
      info(s"last leaderEpoch entry is: $lastEntry")
    }

    val elasticLog = new ElasticLog(metaStream, logStreamManager, streamSliceManager, producerStateManager, partitionMeta, leaderEpochCheckpointMeta, dir, config,
      segments, offsets.nextOffsetMetadata, scheduler, time, topicPartition, logDirFailureChannel)
    if (partitionMeta.getCleanedShutdown) {
      // set cleanedShutdown=false before append, the mark will be set to true when gracefully close.
      partitionMeta.setCleanedShutdown(false)
      elasticLog.persistPartitionMeta()
    }
    elasticLog
  }

  def createMetaStream(client: Client, key: String): api.Stream = {
    //TODO: replica count
    val metaStream = client.streamClient().createAndOpenStream(CreateStreamOptions.newBuilder().replicaCount(1).build()).get()

    // save partition meta stream id relation to PM
    val streamId = metaStream.streamId()
    info(s"created meta stream for $key, streamId: $streamId")
    val valueBuf = ByteBuffer.allocate(8);
    valueBuf.putLong(streamId)
    valueBuf.flip()
    client.kvClient().putKV(java.util.Arrays.asList(KeyValue.of(key, valueBuf))).get()
    metaStream
  }

  def persistMeta(metaStream: api.Stream, metaKeyValue: MetaKeyValue): Unit = {
    metaStream.append(RawPayloadRecordBatch.of(MetaKeyValue.encode(metaKeyValue))).get()
  }

  def getMetas(metaStream: api.Stream): mutable.Map[String, Any] = {
    val startOffset = metaStream.startOffset()
    val endOffset = metaStream.nextOffset()
    val kvMap: mutable.Map[String, ByteBuffer] = mutable.Map()
    // TODO: stream fetch API support fetch by startOffset and endOffset
    // TODO: reverse scan meta stream
    var pos = startOffset
    var done = false
    while (!done) {
      val fetchRst = metaStream.fetch(pos, 128 * 1024).get()
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
          error(s"unknown meta key: ${kv._1}")
      }
    })
    metaMap
  }
}