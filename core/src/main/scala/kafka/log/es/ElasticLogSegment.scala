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

import kafka.log._
import kafka.server.epoch.LeaderEpochFileCache
import kafka.server.{FetchDataInfo, LogOffsetMetadata}
import kafka.utils.{nonthreadsafe, threadsafe}
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.utils.Time

import java.io.File
import scala.math._


class ElasticLogSegment(val _meta: ElasticStreamSegmentMeta,
                        log: ElasticLogFileRecords,
                        val offsetIdx: ElasticOffsetIndex,
                        val timeIdx: ElasticTimeIndex,
                        txnIndex: ElasticTransactionIndex,
                        baseOffset: Long,
                        indexIntervalBytes: Int,
                        rollJitterMs: Long,
                        time: Time) extends LogSegmentKafka(log, null, null, txnIndex, baseOffset, indexIntervalBytes, rollJitterMs, time) with LogSegment {
  override def offsetIndex: OffsetIndex = offsetIdx

  override def timeIndex: TimeIndex = timeIdx

  override def resizeIndexes(size: Int): Unit = {
    // noop implementation.
  }

  override def sanityCheck(timeIndexFileNewlyCreated: Boolean): Unit = {
    // TODO: check LogLoader logic
  }

  @threadsafe
  override def read(startOffset: Long,
                    maxSize: Int,
                    maxPosition: Long = size,
                    minOneMessage: Boolean = false): FetchDataInfo = {
    if (maxSize < 0)
      throw new IllegalArgumentException(s"Invalid max size $maxSize for log read from segment $log")

    val startOffsetAndSize = translateOffset(startOffset)

    // if the start position is already off the end of the log, return null
    if (startOffsetAndSize == null)
      return null

    val startPosition = startOffsetAndSize.position
    val offsetMetadata = LogOffsetMetadata(startOffset, this.baseOffset, startPosition)

    val adjustedMaxSize =
      if (minOneMessage) math.max(maxSize, startOffsetAndSize.size)
      else maxSize

    // return a log segment but with zero size in the case below
    if (adjustedMaxSize == 0)
      return FetchDataInfo(offsetMetadata, MemoryRecords.EMPTY)

    // calculate the length of the message set to read based on whether or not they gave us a maxOffset
    val fetchSize: Int = min((maxPosition - startPosition).toInt, adjustedMaxSize)

    FetchDataInfo(offsetMetadata, log.read(startPosition, fetchSize),
      firstEntryIncomplete = adjustedMaxSize < startOffsetAndSize.size)
  }

  @nonthreadsafe
  override def recover(producerStateManager: ProducerStateManager, leaderEpochCache: Option[LeaderEpochFileCache] = None): Int = {
    // TODO: check LogLoader logic
    -1
  }


  @nonthreadsafe
  override def truncateTo(offset: Long): Int = {
    // TODO: check truncate logic
    -1
  }

  override def updateParentDir(dir: File): Unit = {
    // TODO: check
  }

  override def changeFileSuffixes(oldSuffix: String, newSuffix: String): Unit = {
    // TODO: check
  }

  override def hasSuffix(suffix: String): Boolean = {
    // TODO: check
    false
  }

  override def onBecomeInactiveSegment(): Unit = {
    timeIndex.maybeAppend(maxTimestampSoFar, offsetOfMaxTimestampSoFar, skipFullCheck = true)
    log.seal()
    _meta.setLogStreamEndOffset(log.streamSegment.endOffsetInStream)
    offsetIdx.seal()
    _meta.setOffsetStreamEndOffset(offsetIdx.streamSegment().endOffsetInStream)
    timeIdx.seal()
    _meta.setTimeStreamEndOffset(timeIdx.streamSegment().endOffsetInStream)
    txnIndex.seal()
    _meta.setTxnStreamEndOffset(txnIndex.streamSegment.endOffsetInStream)
  }

  def meta: ElasticStreamSegmentMeta = _meta

  /**
   * Close this log segment
   */
  override def close(): Unit = {
    // TODO:
  }

  override def closeHandlers(): Unit = {
    // TODO:
  }

  override def deleteIfExists(): Unit = {
    // TODO: remove segment from log meta and save
  }

  override def deleted(): Boolean = {
    // TODO:
    false
  }

  override def lastModified = log.getLastModifiedTimeMs

  override def lastModified_=(ms: Long) = {
    // TODO: check
    null
  }
}

object ElasticLogSegment {
  def apply(meta: ElasticStreamSegmentMeta, sm: ElasticStreamSegmentManager, logConfig: LogConfig,
            time: Time): ElasticLogSegment = {
    val baseOffset = meta.getSegmentBaseOffset
    val suffix = meta.getStreamSuffix
    val log = new ElasticLogFileRecords(sm.loadOrCreateSegment("log" + suffix, meta.getLogStreamStartOffset, meta.getLogStreamEndOffset))
    val offsetIndex = new ElasticOffsetIndex(new StreamSegmentSupplier(sm, "idx" + suffix, meta.getOffsetStreamEndOffset, meta.getOffsetStreamEndOffset), baseOffset, logConfig.maxIndexSize)
    val timeIndex = new ElasticTimeIndex(new StreamSegmentSupplier(sm, "tim" + suffix, meta.getTimeStreamStartOffset, meta.getTimeStreamEndOffset), baseOffset, logConfig.maxIndexSize)
    val txnIndex = new ElasticTransactionIndex(baseOffset, sm.loadOrCreateSegment("txn" + meta.getStreamSuffix, meta.getTxnStreamStartOffset, meta.getTxnStreamEndOffset))

    new ElasticLogSegment(meta, log, offsetIndex, timeIndex, txnIndex, baseOffset, logConfig.indexInterval, logConfig.segmentJitterMs, time)
  }
}
