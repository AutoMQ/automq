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
import kafka.log.es.ElasticLog.saveElasticLogMeta
import kafka.server.{LogDirFailureChannel, LogOffsetMetadata}
import kafka.utils.{Logging, Scheduler}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Time
import sdk.elastic.stream.api
import sdk.elastic.stream.api.{Client, CreateStreamOptions, KeyValue, OpenStreamOptions}

import java.io.File
import java.nio.ByteBuffer
import scala.collection.mutable
import scala.jdk.CollectionConverters._

class ElasticLog(val metaStream: api.Stream,
                 val logStream: api.Stream,
                 val offsetStream: api.Stream,
                 val timeStream: api.Stream,
                 val txnStream: api.Stream,
                 val logMeta: ElasticLogMeta,
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

  def logStartOffset: Long = partitionMeta.getStartOffset

  def setLogStartOffset(startOffset: Long): Unit = partitionMeta.setStartOffset(startOffset)

  def cleanerOffsetCheckpoint: Long = partitionMeta.getCleanerOffset

  def setCleanerOffsetCheckpoint(offsetCheckpoint: Long): Unit = partitionMeta.setCleanerOffset(offsetCheckpoint)

  def newSegment(baseOffset: Long, time: Time): ElasticLogSegment = {
    val log = new ElasticLogFileRecords(new DefaultElasticStreamSegment(logStream, logStream.nextOffset()))
    val offsetIndex = new ElasticOffsetIndex(new DefaultElasticStreamSegment(offsetStream, offsetStream.nextOffset()), baseOffset, config.maxIndexSize)
    val timeIndex = new ElasticTimeIndex(new DefaultElasticStreamSegment(timeStream, timeStream.nextOffset()), baseOffset, config.maxIndexSize)
    val txnStreamSegment = new DefaultElasticStreamSegment(txnStream, txnStream.nextOffset())
    val segment: ElasticLogSegment = new ElasticLogSegment(log, offsetIndex, timeIndex, new ElasticTransactionIndex(baseOffset, txnStreamSegment),
      baseOffset, config.indexInterval, config.segmentJitterMs, time);
    //TODO: modify log meta and save, set last active segment end offset and add new segment
    saveElasticLogMeta(metaStream, MetaKeyValue.of(ElasticLog.LOG_META_KEY, ElasticLogMeta.encode(logMeta)))
    segment
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
            logDirFailureChannel: LogDirFailureChannel): ElasticLog = {
    val key = "/kafka/pm/" + topicPartition.topic() + "-" + topicPartition.partition();
    val kvList = client.kvClient().getKV(java.util.Arrays.asList(key)).get();

    var metaStream: api.Stream = null
    // partition dimension meta
    var logMeta: ElasticLogMeta = null
    var partitionMeta: ElasticPartitionMeta = null
    // TODO: load other partition dimension meta
    //    var producerSnaphot = null

    var logStream: api.Stream = null
    var offsetStream: api.Stream = null
    var timeStream: api.Stream = null
    var txnStream: api.Stream = null

    if (kvList.get(0).value() == null) {
      // create stream
      //TODO: replica count
      metaStream = client.streamClient().createAndOpenStream(CreateStreamOptions.newBuilder().replicaCount(1).build()).get()

      // save partition meta stream id relation to PM
      val streamId = metaStream.streamId()
      val valueBuf = ByteBuffer.allocate(8);
      valueBuf.putLong(streamId)
      client.kvClient().putKV(java.util.Arrays.asList(KeyValue.of(key, valueBuf))).get()

      // TODO: concurrent create
      logStream = client.streamClient().createAndOpenStream(CreateStreamOptions.newBuilder().replicaCount(1).build()).get()
      offsetStream = client.streamClient().createAndOpenStream(CreateStreamOptions.newBuilder().replicaCount(1).build()).get()
      timeStream = client.streamClient().createAndOpenStream(CreateStreamOptions.newBuilder().replicaCount(1).build()).get()
      txnStream = client.streamClient().createAndOpenStream(CreateStreamOptions.newBuilder().replicaCount(1).build()).get()

      logMeta = ElasticLogMeta.of(logStream.streamId(), offsetStream.streamId(), timeStream.streamId(), txnStream.streamId())
      partitionMeta = new ElasticPartitionMeta(0, 0)
      saveElasticLogMeta(metaStream, MetaKeyValue.of(LOG_META_KEY, ElasticLogMeta.encode(logMeta)))
      saveElasticLogMeta(metaStream, MetaKeyValue.of(PARTITION_META_KEY, ElasticPartitionMeta.encode(partitionMeta)))
    } else {
      // get partition meta stream id from PM
      val keyValue = kvList.get(0)
      val metaStreamId = Unpooled.wrappedBuffer(keyValue.value()).readLong();
      // open partition meta stream
      metaStream = client.streamClient().openStream(metaStreamId, OpenStreamOptions.newBuilder().build()).get()

      val metaMap = getMetas(metaStream)

      val logMetaOpt = metaMap.get(LOG_META_KEY).map(m => m.asInstanceOf[ElasticLogMeta])

      // TODO: refactor log apply
      if (logMetaOpt.isEmpty) {
        logStream = client.streamClient().createAndOpenStream(CreateStreamOptions.newBuilder().replicaCount(1).build()).get()
        offsetStream = client.streamClient().createAndOpenStream(CreateStreamOptions.newBuilder().replicaCount(1).build()).get()
        timeStream = client.streamClient().createAndOpenStream(CreateStreamOptions.newBuilder().replicaCount(1).build()).get()
        txnStream = client.streamClient().createAndOpenStream(CreateStreamOptions.newBuilder().replicaCount(1).build()).get()

        logMeta = ElasticLogMeta.of(logStream.streamId(), offsetStream.streamId(), timeStream.streamId(), txnStream.streamId())
        saveElasticLogMeta(metaStream, MetaKeyValue.of(LOG_META_KEY, ElasticLogMeta.encode(logMeta)))
      } else {
        logMeta = logMetaOpt.get
        logStream = client.streamClient().openStream(logMeta.getLogStreamId, OpenStreamOptions.newBuilder().build()).get()
        offsetStream = client.streamClient().openStream(logMeta.getOffsetStreamId, OpenStreamOptions.newBuilder().build()).get()
        timeStream = client.streamClient().openStream(logMeta.getTimeStreamId, OpenStreamOptions.newBuilder().build()).get()
        txnStream = client.streamClient().openStream(logMeta.getTxnStreamId, OpenStreamOptions.newBuilder().build()).get()
      }

      val partitionMetaOpt = metaMap.get(PARTITION_META_KEY).map(m => m.asInstanceOf[ElasticPartitionMeta])
        if (partitionMetaOpt.isEmpty) {
            partitionMeta = new ElasticPartitionMeta(0, 0)
            saveElasticLogMeta(metaStream, MetaKeyValue.of(PARTITION_META_KEY, ElasticPartitionMeta.encode(partitionMeta)))
        } else {
            partitionMeta = partitionMetaOpt.get
        }
    }

    val segments = new LogSegments(topicPartition)
    for (segmentMeta <- logMeta.getSegments.asScala) {
      val baseOffset = segmentMeta.getSegmentBaseOffset

      val log = new ElasticLogFileRecords(new DefaultElasticStreamSegment(logStream, segmentMeta.getLogStreamStartOffset))
      val offsetIndex = new ElasticOffsetIndex(new DefaultElasticStreamSegment(offsetStream, segmentMeta.getOffsetStreamStartOffset), baseOffset, config.maxIndexSize)
      val timeIndex = new ElasticTimeIndex(new DefaultElasticStreamSegment(timeStream, segmentMeta.getTimeStreamStartOffset), baseOffset, config.maxIndexSize)
      val txnStreamSegment = new DefaultElasticStreamSegment(txnStream, segmentMeta.getTxnStreamStartOffset)
      //      val endOffset = if (segmentMeta.getSegmentEndOffset == -1L) {
      //        segmentMeta.getSegmentBaseOffset + logStream.nextOffset() - segmentMeta.getLogStreamStartOffset
      //      } else {
      //        segmentMeta.getSegmentEndOffset
      //      }
      // TODO: set endOffset, to avoid segment get overflow
      val elasticLogSegment = new ElasticLogSegment(log, offsetIndex, timeIndex,
        new ElasticTransactionIndex(baseOffset, txnStreamSegment), segmentMeta.getSegmentBaseOffset, config.indexInterval, config.segmentJitterMs, time)
      segments.add(elasticLogSegment)
    }

    val nextOffsetMetadata = segments.lastSegment match {
      case Some(lastSegment) =>
        //FIXME: get the last writable offset with the client
        val lastOffset = 0L
        //FIXME: get the physical position
        LogOffsetMetadata(lastOffset, lastSegment.baseOffset, 0)
      case None =>
        LogOffsetMetadata(0L, 0L, 0)
    }


    val initRecoveryPoint = nextOffsetMetadata.messageOffset
    val elasticLog = new ElasticLog(metaStream, logStream, offsetStream, timeStream, txnStream, logMeta, partitionMeta, dir, config, segments, initRecoveryPoint, nextOffsetMetadata,
      scheduler, time, topicPartition, logDirFailureChannel)
    if (segments.isEmpty) {
      val elasticStreamSegment = elasticLog.newSegment(0L, time)
      segments.add(elasticStreamSegment)
    }
    elasticLog
  }

  def saveElasticLogMeta(metaStream: api.Stream, metaKeyValue: MetaKeyValue): Unit = {
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
