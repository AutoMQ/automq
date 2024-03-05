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

import ElasticLog.{debug, error, info}
import kafka.server.LogOffsetMetadata

import java.util
import java.util.Optional
import java.util.concurrent.{CompletableFuture, ConcurrentHashMap}
import java.util.concurrent.atomic.AtomicReference
import java.util.stream.Collectors
import scala.jdk.CollectionConverters.{ListHasAsScala, SetHasAsScala}

class ElasticLogSegmentManager(val metaStream: MetaStream, val streamManager: ElasticLogStreamManager, logIdent: String) {
  val segments = new ConcurrentHashMap[Long, ElasticLogSegment]()
  private val inflightCleanedSegments = new ConcurrentHashMap[Long, ElasticLogSegment]()
  val segmentEventListener = new EventListener()
  val offsetUpperBound = new AtomicReference[LogOffsetMetadata]()

  def put(baseOffset: Long, segment: ElasticLogSegment): Unit = {
    segments.put(baseOffset, segment)
    inflightCleanedSegments.remove(baseOffset, segment)
  }

  def putInflightCleaned(baseOffset: Long, segment: ElasticLogSegment): Unit = {
    inflightCleanedSegments.put(baseOffset, segment)
  }

  def create(baseOffset: Long, segment: ElasticLogSegment): CompletableFuture[Void] = {
    val offset = LogOffsetMetadata(baseOffset, baseOffset, 0)
    while (!offsetUpperBound.compareAndSet(null, offset)) {
      info(s"$logIdent try create new segment with offset $baseOffset, wait last segment meta persisted.")
      Thread.sleep(1L)
    }
    segments.put(baseOffset, segment)
    asyncPersistLogMeta().thenAccept(_ => {
      offsetUpperBound.set(null)
    })
  }

  def remove(baseOffset: Long): ElasticLogSegment = {
    segments.remove(baseOffset)
  }

  def persistLogMeta(): ElasticLogMeta = {
    asyncPersistLogMeta().get()
  }
  
  def asyncPersistLogMeta(): CompletableFuture[ElasticLogMeta] = {
    val meta = logMeta()
    val kv = MetaKeyValue.of(MetaStream.LOG_META_KEY, ElasticLogMeta.encode(meta))
    metaStream.append(kv).thenApply(_ => {
      info(s"${logIdent}save log meta $meta")
      trimStream(meta)
      meta
    }).whenComplete((_, ex) => {
      if (ex != null) {
        error(s"$logIdent persist log meta $meta fail", ex)
      }
    })
  }

  private def trimStream(meta: ElasticLogMeta): Unit = {
    try {
      trimStream0(meta)
    } catch {
      case e: Throwable => error(s"$logIdent trim stream failed", e)
    }
  }

  private def trimStream0(meta: ElasticLogMeta): Unit = {
    val streamMinOffsets = new util.HashMap[String, java.lang.Long]()
    inflightCleanedSegments.forEach((_, segment) => {
      val segMeta = segment.meta
      calStreamsMinOffset(streamMinOffsets, segMeta)
    })
    for (segMeta <- meta.getSegmentMetas.asScala) {
      calStreamsMinOffset(streamMinOffsets, segMeta)
    }

    for (entry <- streamManager.streams().entrySet().asScala) {
      val streamName = entry.getKey
      val stream = entry.getValue
      var minOffset = streamMinOffsets.get(streamName)
      // if minOffset == null, then stream is not used by any segment, should trim it to end.
      minOffset = Optional.ofNullable(minOffset).orElse(stream.nextOffset())
      if (minOffset > stream.startOffset()) {
        stream.trim(minOffset)
      }
    }
  }

  private def calStreamsMinOffset(streamMinOffsets: util.HashMap[String, java.lang.Long], segMeta: ElasticStreamSegmentMeta): Unit = {
    streamMinOffsets.compute("log" + segMeta.streamSuffix(), (_, v) => {
      math.min(segMeta.log().start(), Optional.ofNullable(v).orElse(Long.MaxValue))
    })
    streamMinOffsets.compute("tim" + segMeta.streamSuffix(), (_, v) => {
      Math.min(segMeta.time().start(), Optional.ofNullable(v).orElse(Long.MaxValue))
    })
    streamMinOffsets.compute("txn" + segMeta.streamSuffix(), (_, v) => {
      Math.min(segMeta.txn().start(), Optional.ofNullable(v).orElse(Long.MaxValue))
    })
  }


  def logSegmentEventListener(): ElasticLogSegmentEventListener = {
    segmentEventListener
  }

  def logMeta(): ElasticLogMeta = {
    val elasticLogMeta = new ElasticLogMeta()
    val streamMap = new util.HashMap[String, java.lang.Long]()
    streamManager.streams.entrySet().forEach(entry => {
      streamMap.put(entry.getKey, entry.getValue.streamId())
    })
    elasticLogMeta.setStreamMap(streamMap)
    val segmentList: util.List[ElasticStreamSegmentMeta] = segments.values().stream().sorted().map(segment => segment.meta).collect(Collectors.toList())
    elasticLogMeta.setSegmentMetas(segmentList)
    elasticLogMeta
  }

  class EventListener extends ElasticLogSegmentEventListener {
    override def onEvent(segmentBaseOffset: Long, event: ElasticLogSegmentEvent): Unit = {
      event match {
        case ElasticLogSegmentEvent.SEGMENT_DELETE =>
          val deleted = remove(segmentBaseOffset) != null
          if (deleted) {
            // This may happen since kafka.log.LocalLog.deleteSegmentFiles schedules the delayed deletion task.
            if (metaStream.isFenced) {
              debug(s"${logIdent}meta stream is closed, skip persisting log meta")
            } else {
              asyncPersistLogMeta()
            }
          }
        case ElasticLogSegmentEvent.SEGMENT_UPDATE =>
          persistLogMeta()
        case _ =>
          throw new IllegalStateException(s"Unsupported event $event")
      }
    }
  }

}
