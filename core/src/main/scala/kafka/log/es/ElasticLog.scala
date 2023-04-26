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
import kafka.utils.Scheduler
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Time
import sdk.elastic.stream.api
import sdk.elastic.stream.api.{Client, CreateStreamOptions, KeyValue, OpenStreamOptions}

import java.io.File
import java.nio.ByteBuffer
import scala.jdk.CollectionConverters._

class ElasticLog(val metaStream: api.Stream,
                 val dataStream: api.Stream,
                 val timeStream: api.Stream,
                 val txnStream: api.Stream,
                 val logMeta: ElasticLogMeta,
                 _dir: File,
                 c: LogConfig,
                 segments: LogSegments,
                 recoveryPoint: Long,
                 nextOffsetMetadata: LogOffsetMetadata,
                 scheduler: Scheduler,
                 time: Time,
                 topicPartition: TopicPartition,
                 logDirFailureChannel: LogDirFailureChannel)
  extends LocalLog(_dir, c, segments, recoveryPoint, nextOffsetMetadata, scheduler, time, topicPartition, logDirFailureChannel) {

  def logStartOffset(): Long = {
    // FIXME
    0L
  }

  def newSegment(baseOffset: Long, time: Time): ElasticLogSegment = {
    val dataStreamSegment = new DefaultElasticStreamSegment(baseOffset, dataStream, dataStream.nextOffset())
    val timeStreamSegment = new DefaultElasticStreamSegment(baseOffset, timeStream, timeStream.nextOffset())
    val txnStreamSegment = new DefaultElasticStreamSegment(baseOffset, txnStream, txnStream.nextOffset())
    val segment: ElasticLogSegment = new ElasticLogSegment(topicPartition, dataStreamSegment, timeStreamSegment, new ElasticTransactionIndex(baseOffset, txnStreamSegment), endOffset = baseOffset,
      baseOffset, config.indexInterval, config.segmentJitterMs, time);
    //TODO: modify log meta and save, set last active segment end offset and add new segment
    saveElasticLogMeta(metaStream, logMeta)
    segment
  }

  /**
   * Closes the segments of the log.
   */
  override private[log] def close(): Unit = {
    super.close()
  }
}

object ElasticLog {
  private val logMetaKey = "logMeta"

  def apply(client: Client, dir: File,
            config: LogConfig,
            recoveryPoint: Long,
            scheduler: Scheduler,
            time: Time,
            topicPartition: TopicPartition,
            logDirFailureChannel: LogDirFailureChannel): ElasticLog = {
    val key = "/kafka/pm/" + topicPartition.topic() + "-" + topicPartition.partition();
    val kvList = client.kvClient().getKV(java.util.Arrays.asList(key)).get();

    var metaStream: api.Stream = null
    // partition dimension meta
    var logMeta: ElasticLogMeta = null
    // TODO: load other partition dimension meta
    //    var producerSnaphot = null
    //    var partitionMeta = null

    var dataStream: api.Stream = null
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
      dataStream = client.streamClient().createAndOpenStream(CreateStreamOptions.newBuilder().replicaCount(1).build()).get()
      timeStream = client.streamClient().createAndOpenStream(CreateStreamOptions.newBuilder().replicaCount(1).build()).get()
      txnStream = client.streamClient().createAndOpenStream(CreateStreamOptions.newBuilder().replicaCount(1).build()).get()

      logMeta = ElasticLogMeta.of(dataStream.streamId(), timeStream.streamId(), txnStream.streamId())
      saveElasticLogMeta(metaStream, logMeta)
    } else {
      // get partition meta stream id from PM
      val keyValue = kvList.get(0)
      val metaStreamId = Unpooled.wrappedBuffer(keyValue.value()).readLong();
      // open partition meta stream
      metaStream = client.streamClient().openStream(metaStreamId, OpenStreamOptions.newBuilder().build()).get()

      //TODO: string literal
      val metaMap = StreamUtils.fetchKV(metaStream, Set(logMetaKey).asJava).asScala
      logMeta = metaMap.get(logMetaKey).map(buf => ElasticLogMeta.decode(buf)).getOrElse(new ElasticLogMeta())
      dataStream = client.streamClient().openStream(logMeta.getDataStreamId, OpenStreamOptions.newBuilder().build()).get()
      timeStream = client.streamClient().openStream(logMeta.getTimeStreamId, OpenStreamOptions.newBuilder().build()).get()
      txnStream = client.streamClient().openStream(logMeta.getTxnStreamId, OpenStreamOptions.newBuilder().build()).get()
    }


    val segments = new LogSegments(topicPartition)
    for (segmentMeta <- logMeta.getSegments.asScala) {
      val baseOffset = segmentMeta.getSegmentBaseOffset
      val dataStreamSegment = new DefaultElasticStreamSegment(baseOffset, dataStream, segmentMeta.getDataStreamStartOffset)
      val timeStreamSegment = new DefaultElasticStreamSegment(baseOffset, timeStream, segmentMeta.getTimeStreamStartOffset)
      val txnStreamSegment = new DefaultElasticStreamSegment(baseOffset, txnStream, segmentMeta.getTxnStreamStartOffset)
      val endOffset = if (segmentMeta.getSegmentEndOffset == -1L) {
        segmentMeta.getSegmentBaseOffset + dataStream.nextOffset() - segmentMeta.getDataStreamStartOffset
      } else {
        segmentMeta.getSegmentEndOffset
      }
      val elasticLogSegment = new ElasticLogSegment(topicPartition,
        dataStreamSegment, timeStreamSegment, new ElasticTransactionIndex(baseOffset, txnStreamSegment), endOffset, segmentMeta.getSegmentBaseOffset, config.indexInterval, config.segmentJitterMs, time)
      segments.add(elasticLogSegment)
    }
    //FIXME: offset
    val nextOffsetMetadata = LogOffsetMetadata(0L, 0, 0)
    val elasticLog = new ElasticLog(metaStream, dataStream, timeStream, txnStream, logMeta, dir, config, segments, recoveryPoint, nextOffsetMetadata,
      scheduler, time, topicPartition, logDirFailureChannel)
    if (segments.isEmpty) {
      val elasticStreamSegment = elasticLog.newSegment(0L, time)
      segments.add(elasticStreamSegment)
    }
    elasticLog
  }

  def saveElasticLogMeta(metaStream: api.Stream, logMeta: ElasticLogMeta): Unit = {
    println(s"saveElasticLogMeta: $logMeta")
    //TODO: save meta to meta stream
    //    val metaBuf = ElasticLogMeta.encode(logMeta)
    //    StreamUtils.putKV(metaStream, "logMeta", metaBuf)
  }
}
