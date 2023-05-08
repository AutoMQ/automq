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
import kafka.log.es.ElasticLog.{LOG_META_KEY, persistMeta}
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
                 val streamSegmentManager: ElasticStreamSegmentManager,
                 val partitionMeta: ElasticPartitionMeta,
                 _dir: File,
                 c: LogConfig,
                 segments: LogSegments,
                 recoveryPoint: Long,
                 @volatile private[log] var nextOffsetMetadata: LogOffsetMetadata,
                 scheduler: Scheduler,
                 time: Time,
                 topicPartition: TopicPartition,
                 logDirFailureChannel: LogDirFailureChannel)
  extends LocalLog(_dir, c, segments, recoveryPoint, nextOffsetMetadata, scheduler, time, topicPartition, logDirFailureChannel) {

  private var cleanedSegments: List[ElasticLogSegment] = List()

  // persist log meta when lazy stream real create
  streamManager.setListener((_, event) => {
    if (event == LazyStream.Event.CREATE) {
      persistLogMeta()
    }
  })

  def logStartOffset: Long = partitionMeta.getStartOffset

  def setLogStartOffset(startOffset: Long): Unit = partitionMeta.setStartOffset(startOffset)

  def cleanerOffsetCheckpoint: Long = partitionMeta.getCleanerOffset

  def setCleanerOffsetCheckpoint(offsetCheckpoint: Long): Unit = partitionMeta.setCleanerOffset(offsetCheckpoint)

  def recoveryPointCheckpoint: Long = partitionMeta.getRecoverOffset

  def setReCoverOffsetCheckpoint(offsetCheckpoint: Long): Unit = partitionMeta.setRecoverOffset(offsetCheckpoint)

  def newSegment(baseOffset: Long, time: Time, suffix: String = ""): ElasticLogSegment = {
    if (!suffix.equals("") && !suffix.equals(LocalLog.CleanedFileSuffix)) {
      throw new IllegalArgumentException("suffix must be empty or " + LocalLog.CleanedFileSuffix)
    }
    // In roll, before new segment, last segment will be inactive by #onBecomeInactiveSegment
    val meta = new ElasticStreamSegmentMeta()
    meta.setSegmentBaseOffset(baseOffset)
    meta.setStreamSuffix(suffix)
    val segment: ElasticLogSegment = ElasticLogSegment(meta, streamSegmentManager, config, time)
    if (suffix.equals(LocalLog.CleanedFileSuffix)) {
      // remove cleanedSegments when replace
      cleanedSegments = cleanedSegments :+ segment
    }
    persistLogMeta()
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
    info(s"save log meta: $elasticLogMeta")
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
    super.close()
  }
}

object ElasticLog extends Logging {
  val LOG_META_KEY: Short = 0
  val PRODUCER_SNAPSHOT_META_KEY: Short = 1
  val PARTITION_META_KEY: Short = 2
  val META_KEYS = Set(LOG_META_KEY, PRODUCER_SNAPSHOT_META_KEY, PARTITION_META_KEY)

  def apply(client: Client, dir: File,
            config: LogConfig,
            scheduler: Scheduler,
            time: Time,
            topicPartition: TopicPartition,
            logDirFailureChannel: LogDirFailureChannel,
            producerStateManager: ProducerStateManager,
            numRemainingSegments: ConcurrentMap[String, Int] = new ConcurrentHashMap[String, Int]): ElasticLog = {
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

    // fetch metas(log meta, producer snapshot, partition meta, ...) from meta stream
    val metaMap = getMetas(metaStream)


    val partitionMetaOpt = metaMap.get(PARTITION_META_KEY).map(m => m.asInstanceOf[ElasticPartitionMeta])
    if (partitionMetaOpt.isEmpty) {
      partitionMeta = new ElasticPartitionMeta(0, 0, 0)
      persistMeta(metaStream, MetaKeyValue.of(PARTITION_META_KEY, ElasticPartitionMeta.encode(partitionMeta)))
    } else {
      partitionMeta = partitionMetaOpt.get
    }

    // load LogSegments and recover log
    val logMeta: ElasticLogMeta = metaMap.get(LOG_META_KEY).map(m => m.asInstanceOf[ElasticLogMeta]).getOrElse(new ElasticLogMeta())
    val logStreamManager = new ElasticLogStreamManager(logMeta.getStreams, client.streamClient())
    val streamSegmentManager = new ElasticStreamSegmentManager(logStreamManager)
    val segments = new LogSegments(topicPartition)
    val offsets = new ElasticLogLoader(
      logMeta,
      segments,
      streamSegmentManager,
      dir,
      topicPartition,
      config,
      time,
      hadCleanShutdown = false, // TODO: fetch from partition meta?
      logStartOffsetCheckpoint = partitionMeta.getStartOffset,
      partitionMeta.getRecoverOffset,
      leaderEpochCache = None,
      producerStateManager = producerStateManager,
      numRemainingSegments = numRemainingSegments).load()

    val elasticLog = new ElasticLog(metaStream, logStreamManager, streamSegmentManager, partitionMeta, dir, config,
      segments, offsets.recoveryPoint, offsets.nextOffsetMetadata, scheduler, time, topicPartition, logDirFailureChannel)
    elasticLog
  }

  def createMetaStream(client: Client, key: String): api.Stream = {
    //TODO: replica count
    val metaStream = client.streamClient().createAndOpenStream(CreateStreamOptions.newBuilder().replicaCount(1).build()).get()

    // save partition meta stream id relation to PM
    val streamId = metaStream.streamId()
    val valueBuf = ByteBuffer.allocate(8);
    valueBuf.putLong(streamId)
    client.kvClient().putKV(java.util.Arrays.asList(KeyValue.of(key, valueBuf))).get()
    metaStream
  }

  def persistMeta(metaStream: api.Stream, metaKeyValue: MetaKeyValue): Unit = {
    metaStream.append(new SingleRecordBatch(MetaKeyValue.encode(metaKeyValue))).get()
  }

  def getMetas(metaStream: api.Stream): mutable.Map[Short, Any] = {
    val startOffset = metaStream.startOffset()
    val endOffset = metaStream.nextOffset()
    val kvMap: mutable.Map[Short, ByteBuffer] = mutable.Map()
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

    val metaMap = mutable.Map[Short, Any]()
    kvMap.foreach(kv => {
      kv._1 match {
        case ElasticLog.LOG_META_KEY =>
          metaMap.put(kv._1, ElasticLogMeta.decode(kv._2))
        case ElasticLog.PARTITION_META_KEY =>
          metaMap.put(kv._1, ElasticPartitionMeta.decode(kv._2))
        case _ =>
          warn(s"unknown meta key: ${kv._1}")
      }
    })
    metaMap
  }
}