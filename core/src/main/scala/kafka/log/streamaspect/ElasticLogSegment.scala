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

import kafka.log._
import kafka.log.streamaspect.cache.FileCache
import kafka.server.epoch.LeaderEpochFileCache
import kafka.server.{FetchDataInfo, LogOffsetMetadata}
import kafka.utils.{CoreUtils, nonthreadsafe, threadsafe}
import org.apache.kafka.common.InvalidRecordException
import org.apache.kafka.common.errors.CorruptRecordException
import org.apache.kafka.common.record.FileRecords.{LogOffsetPosition, TimestampAndOffset}
import org.apache.kafka.common.record.{FileRecords, MemoryRecords, RecordBatch}
import org.apache.kafka.common.utils.Time

import java.io.File
import java.nio.file.Path
import java.util.concurrent.CompletableFuture
import scala.jdk.CollectionConverters._


class ElasticLogSegment(val _meta: ElasticStreamSegmentMeta,
                        val _log: ElasticLogFileRecords,
                        val timeIdx: ElasticTimeIndex,
                        val txnIndex: ElasticTransactionIndex,
                        val baseOffset: Long,
                        val indexIntervalBytes: Int,
                        val rollJitterMs: Long,
                        val time: Time,
                        val logListener: ElasticLogSegmentEventListener,
                        _logIdent: String = "") extends LogSegment with Comparable[ElasticLogSegment]{

  logIdent = _logIdent

  def log: FileRecords = throw new UnsupportedOperationException()

  def offsetIndex: OffsetIndex = {
    throw new UnsupportedOperationException()
  }

  def timeIndex: TimeIndex = timeIdx

  def shouldRoll(rollParams: RollParams): Boolean = {
    val reachedRollMs = timeWaitedForRoll(rollParams.now, rollParams.maxTimestampInMessages) > rollParams.maxSegmentMs - rollJitterMs
    size > rollParams.maxSegmentBytes - rollParams.messagesSize ||
      (size > 0 && reachedRollMs) || timeIndex.isFull
  }

  def resizeIndexes(size: Int): Unit = {
    // noop implementation.
  }

  def sanityCheck(timeIndexFileNewlyCreated: Boolean): Unit = {
    // do nothing since it will not be called.
  }

  private val created = time.milliseconds

  protected var bytesSinceLastIndexEntry = 0

  @volatile protected var rollingBasedTimestamp: Option[Long] = if (_meta.firstBatchTimestamp() != 0) {
    Some(_meta.firstBatchTimestamp())
  } else {
    None
  }

  @volatile private var _maxTimestampAndOffsetSoFar: TimestampOffset = TimestampOffset.Unknown

  def maxTimestampAndOffsetSoFar_=(timestampOffset: TimestampOffset): Unit = _maxTimestampAndOffsetSoFar = timestampOffset

  def maxTimestampAndOffsetSoFar: TimestampOffset = {
    if (_maxTimestampAndOffsetSoFar == TimestampOffset.Unknown)
      _maxTimestampAndOffsetSoFar = timeIndex.lastEntry
    _maxTimestampAndOffsetSoFar
  }

  def maxTimestampSoFar: Long = {
    maxTimestampAndOffsetSoFar.timestamp
  }

  def offsetOfMaxTimestampSoFar: Long = {
    maxTimestampAndOffsetSoFar.offset
  }

  def size: Int = _log.sizeInBytes()

  def append(largestOffset: Long, largestTimestamp: Long, shallowOffsetOfMaxTimestamp: Long, records: MemoryRecords): Unit = {
    if (records.sizeInBytes > 0) {
      val physicalPosition = _log.sizeInBytes()
      if (physicalPosition == 0) {
        rollingBasedTimestamp = Some(largestTimestamp)
        _meta.firstBatchTimestamp(largestTimestamp)
      }
      // append the messages
      val appendedBytes = _log.append(records, largestOffset + 1)
      trace(s"Appended $appendedBytes at end offset $largestOffset")
      // Update the in memory max timestamp and corresponding offset.
      if (largestTimestamp > maxTimestampSoFar) {
        maxTimestampAndOffsetSoFar = TimestampOffset(largestTimestamp, shallowOffsetOfMaxTimestamp)
      }
      // append an entry to the index (if needed)
      if (bytesSinceLastIndexEntry > indexIntervalBytes) {
        timeIndex.maybeAppend(maxTimestampSoFar, offsetOfMaxTimestampSoFar)
        bytesSinceLastIndexEntry = 0
      }
      bytesSinceLastIndexEntry += records.sizeInBytes
    }
    _meta.lastModifiedTimestamp(System.currentTimeMillis())
  }

  def asyncLogFlush(): CompletableFuture[Void] = {
    _log.asyncFlush()
  }

  def appendFromFile(records: FileRecords, start: Int): Int = {
    throw new UnsupportedOperationException()
  }

  @nonthreadsafe
  def updateTxnIndex(completedTxn: CompletedTxn, lastStableOffset: Long): Unit = {
    if (completedTxn.isAborted) {
      trace(s"Writing aborted transaction $completedTxn to transaction index, last stable offset is $lastStableOffset")
      txnIndex.append(new AbortedTxn(completedTxn, lastStableOffset))
    }
  }

  protected def updateProducerState(producerStateManager: ProducerStateManager, batch: RecordBatch, txnIndexCheckpoint: Option[Long]): Unit = {
    if (batch.hasProducerId) {
      val producerId = batch.producerId
      val appendInfo = producerStateManager.prepareUpdate(producerId, origin = AppendOrigin.Replication)
      val maybeCompletedTxn = appendInfo.append(batch, firstOffsetMetadataOpt = None)
      producerStateManager.update(appendInfo)
      maybeCompletedTxn.foreach { completedTxn =>
        val lastStableOffset = producerStateManager.lastStableOffset(completedTxn)
        if (txnIndexCheckpoint.isEmpty || txnIndexCheckpoint.get < completedTxn.lastOffset) {
          updateTxnIndex(completedTxn, lastStableOffset)
        }
        producerStateManager.completeTxn(completedTxn)
      }
    }
    producerStateManager.updateMapEndOffset(batch.lastOffset + 1)
  }

  @threadsafe
  private[log] def translateOffset(offset: Long, startingFilePosition: Int = 0): LogOffsetPosition = {
    throw new UnsupportedOperationException()
  }

  @threadsafe
  override def read(startOffset: Long, maxSize: Int, maxPosition: Long, maxOffset: Long, minOneMessage: Boolean): FetchDataInfo = {
    readAsync(startOffset, maxSize, maxPosition, maxOffset, minOneMessage).get()
  }

  // Compared with LogSegment, null is impossible to be returned anymore.
  @threadsafe
  override def readAsync(startOffset: Long,
                    maxSize: Int,
                    maxPosition: Long = size,
                    maxOffset: Long = Long.MaxValue,
                         minOneMessage: Boolean = false): CompletableFuture[FetchDataInfo] = {
    // TODO: isolate the log clean read to another method
    if (maxSize < 0)
      return CompletableFuture.failedFuture(new IllegalArgumentException(s"Invalid max size $maxSize for log read from segment $log"))
    // Note that relativePositionInSegment here is a fake value. There are no 'position' in elastic streams.
    val offsetMetadata = LogOffsetMetadata(startOffset, this.baseOffset, (startOffset - this.baseOffset).toInt)
    if (maxSize == 0) {
      return CompletableFuture.completedFuture(FetchDataInfo(offsetMetadata, MemoryRecords.EMPTY, firstEntryIncomplete = true))
    }
    // Note that 'maxPosition' and 'minOneMessage' are not used here. 'maxOffset' is a better alternative to 'maxPosition'.
    // 'minOneMessage' is also not used because we always read at least one message ('maxSize' is just a hint in ES SDK).
    _log.read(startOffset, maxOffset, maxSize)
      .thenApply(records => {
        if (ReadHint.isReadAll() && records.sizeInBytes() == 0) {
          // After topic compact, the read request might be out of range. Segment should return null and log will retry read next segment.
          null
        } else {
          // We keep 'firstEntryIncomplete' false here since real size of records may exceed 'maxSize'. It is some kind of
          // hack but we don't want to return 'firstEntryIncomplete' as true in that case.
          FetchDataInfo(offsetMetadata, records)
        }
      })
  }

  def fetchUpperBoundOffset(startOffsetPosition: OffsetPosition, fetchSize: Int): Option[Long] = {
    throw new UnsupportedOperationException()
  }

  @nonthreadsafe
  override def recover(producerStateManager: ProducerStateManager, leaderEpochCache: Option[LeaderEpochFileCache] = None): Int = {
    logListener.onEvent(baseOffset, ElasticLogSegmentEvent.SEGMENT_UPDATE)
    recover0(producerStateManager, leaderEpochCache)
  }

  @nonthreadsafe
  protected def recover0(producerStateManager: ProducerStateManager, leaderEpochCache: Option[LeaderEpochFileCache] = None): Int = {
    var validBytes = 0
    var lastIndexEntry = 0
    maxTimestampAndOffsetSoFar = TimestampOffset.Unknown
    // exclusive recover from the checkpoint cause the offset the offset of record in batch
    val timeIndexCheckpoint = timeIndex.asInstanceOf[ElasticTimeIndex].loadLastEntry().offset
    // exclusive recover from the checkpoint
    val txnIndexCheckpoint = txnIndex.loadLastOffset()
    try {
      val recoverPoint = math.max(producerStateManager.mapEndOffset, baseOffset)
      info(s"[UNCLEAN_SHUTDOWN] recover range [$recoverPoint, ${_log.nextOffset()})")
      for (batch <- _log.batchesFrom(recoverPoint).asScala) {
        batch.ensureValid()
        // The max timestamp is exposed at the batch level, so no need to iterate the records
        if (batch.maxTimestamp > maxTimestampSoFar) {
          maxTimestampAndOffsetSoFar = TimestampOffset(batch.maxTimestamp, batch.lastOffset)
        }

        // Build offset index
        if (validBytes - lastIndexEntry > indexIntervalBytes && batch.baseOffset() > timeIndexCheckpoint) {
          timeIndex.maybeAppend(maxTimestampSoFar, offsetOfMaxTimestampSoFar)
          lastIndexEntry = validBytes
        }
        validBytes += batch.sizeInBytes()

        if (batch.magic >= RecordBatch.MAGIC_VALUE_V2) {
          leaderEpochCache.foreach { cache =>
            if (batch.partitionLeaderEpoch >= 0 && cache.latestEpoch.forall(batch.partitionLeaderEpoch > _))
              cache.assign(batch.partitionLeaderEpoch, batch.baseOffset)
          }
          updateProducerState(producerStateManager, batch, txnIndexCheckpoint)
        }
      }
    } catch {
      case e@(_: CorruptRecordException | _: InvalidRecordException) =>
        warn("Found invalid messages in log segment at byte offset %d: %s. %s"
          .format(validBytes, e.getMessage, e.getCause))
    }
    // won't have record corrupted cause truncate
    // A normally closed segment always appends the biggest timestamp ever seen into log segment, we do this as well.
    timeIndex.maybeAppend(maxTimestampSoFar, offsetOfMaxTimestampSoFar, skipFullCheck = true)
    0
  }

  protected def loadLargestTimestamp(): Unit = {
    // Get the last time index entry. If the time index is empty, it will return (-1, baseOffset)
    val lastTimeIndexEntry = timeIndex.lastEntry
    maxTimestampAndOffsetSoFar = lastTimeIndexEntry
    val maxTimestampOffsetAfterLastEntry = _log.largestTimestampAfter(maxTimestampAndOffsetSoFar.offset)
    if (maxTimestampOffsetAfterLastEntry.timestamp > lastTimeIndexEntry.timestamp) {
      maxTimestampAndOffsetSoFar = TimestampOffset(maxTimestampOffsetAfterLastEntry.timestamp, maxTimestampOffsetAfterLastEntry.offset)
    }
  }

  def collectAbortedTxns(fetchOffset: Long, upperBoundOffset: Long): TxnIndexSearchResult =
    txnIndex.collectAbortedTxns(fetchOffset, upperBoundOffset)

  override def toString: String = "ElasticLogSegment(baseOffset=" + baseOffset +
    ", size=" + size +
    ", lastModifiedTime=" + lastModified +
    ", largestRecordTimestamp=" + largestRecordTimestamp +
    ")"

  @nonthreadsafe
  override def truncateTo(offset: Long): Int = {
    throw new UnsupportedOperationException()
  }

  /**
   * get appended offset. It can be used to show whether the segment contains any valid data.
   *
   * @return appended offset
   */
  def appendedOffset: Long = _log.appendedOffset()


  @threadsafe
  override def readNextOffset: Long = {
    _log.nextOffset()
  }

  @threadsafe
  override def flush(): Unit = {
    LogFlushStats.logFlushTimer.time { () =>
      _log.flush()
      timeIndex.flush()
      txnIndex.flush()
    }
  }

  override def updateParentDir(dir: File): Unit = {
    timeIdx.updateParentDir(dir)
    txnIndex.updateParentDir(dir)
  }

  // Do nothing here.
  override def changeFileSuffixes(oldSuffix: String, newSuffix: String): Unit = {
  }

  // No need to support suffix in ElasticLogSegment. Just return false.
  override def hasSuffix(suffix: String): Boolean = {
    false
  }

  override def onBecomeInactiveSegment(): Unit = {
    timeIndex.maybeAppend(maxTimestampSoFar, offsetOfMaxTimestampSoFar, skipFullCheck = true)
    _log.seal()
    _meta.log(_log.streamSegment.sliceRange)
    timeIndex.trimToValidSize()
    timeIdx.seal()
    _meta.time(timeIdx.stream.sliceRange)
    _meta.timeIndexLastEntry(timeIndex.lastEntry)
    txnIndex.seal()
    _meta.txn(txnIndex.stream.sliceRange)
  }

  protected def loadFirstBatchTimestamp(): Unit = {
    if (rollingBasedTimestamp.isEmpty) {
      if (_log.sizeInBytes() > 0) {
        val records = _log.read(baseOffset, baseOffset + 1, 1).get()
        val iter = records.batches().iterator()
        if (iter.hasNext)
          rollingBasedTimestamp = Some(iter.next().maxTimestamp)
      }
    }
  }

  def timeWaitedForRoll(now: Long, messageTimestamp: Long): Long = {
    // Load the timestamp of the first message into memory
    loadFirstBatchTimestamp()
    rollingBasedTimestamp match {
      case Some(t) if t >= 0 => messageTimestamp - t
      case _ => now - created
    }
  }

  def getFirstBatchTimestamp(): Long = {
    loadFirstBatchTimestamp()
    rollingBasedTimestamp match {
      case Some(t) if t >= 0 => t
      case _ => Long.MaxValue
    }
  }

  override def findOffsetByTimestamp(timestamp: Long, startingOffset: Long = baseOffset): Option[TimestampAndOffset] = {
    // TODO: async the find to avoid blocking the thread
    // Get the index entry with a timestamp less than or equal to the target timestamp
    val timestampOffset = timeIndex.lookup(timestamp)
    // Search the timestamp
    val rst = Option(_log.searchForTimestamp(timestamp, timestampOffset.offset))
    rst
  }


  /**
   * Close this log segment
   */
  override def close(): Unit = {
    if (_maxTimestampAndOffsetSoFar != TimestampOffset.Unknown)
      CoreUtils.swallow(timeIndex.maybeAppend(maxTimestampSoFar, offsetOfMaxTimestampSoFar,
        skipFullCheck = true), this)
    CoreUtils.swallow(timeIdx.close(), this)
    CoreUtils.swallow(_log.close(), this)
    CoreUtils.swallow(txnIndex.close(), this)
  }

  override def closeHandlers(): Unit = {
    CoreUtils.swallow(timeIdx.closeHandler(), this)
    CoreUtils.swallow(_log.closeHandlers(), this)
    CoreUtils.swallow(txnIndex.close(), this)
  }

  override def deleteIfExists(): Unit = {
    logListener.onEvent(baseOffset, ElasticLogSegmentEvent.SEGMENT_DELETE)
  }

  // No need to support this method. If this instance can still be accessed, it is not deleted.
  override def deleted(): Boolean = {
    false
  }

  override def lastModified: Long = meta.lastModifiedTimestamp

  def largestRecordTimestamp: Option[Long] = if (maxTimestampSoFar >= 0) Some(maxTimestampSoFar) else None

  def largestTimestamp: Long = if (maxTimestampSoFar >= 0) maxTimestampSoFar else lastModified

  override def lastModified_=(ms: Long): Path = {
    meta.lastModifiedTimestamp(ms)
    null
  }

  def meta: ElasticStreamSegmentMeta = {
    _meta.log(_log.streamSegment.sliceRange)
    _meta.logSize(_log.sizeInBytes())
    _meta.time(timeIdx.stream.sliceRange)
    _meta.txn(txnIndex.stream.sliceRange)
    _meta.timeIndexLastEntry(timeIndex.lastEntry)
    _meta.streamSuffix(_meta.streamSuffix())
    _meta
  }

  override def lazyOffsetIndex: LazyIndex[OffsetIndex] = throw new UnsupportedOperationException()

  override def lazyTimeIndex: LazyIndex[TimeIndex] = throw new UnsupportedOperationException()

  override def canConvertToRelativeOffset(offset: Long): Boolean = throw new UnsupportedOperationException()

  override def hasOverflow: Boolean = throw new UnsupportedOperationException()

  override def compareTo(o: ElasticLogSegment): Int = {
    baseOffset.compareTo(o.baseOffset)
  }
}

object ElasticLogSegment {
  var TxnCache: FileCache = _
  var TimeCache: FileCache = _

  def apply(dir: File, meta: ElasticStreamSegmentMeta, sm: ElasticStreamSliceManager, logConfig: LogConfig,
            time: Time, segmentEventListener: ElasticLogSegmentEventListener, logIdent: String = ""): ElasticLogSegment = {
    val baseOffset = meta.baseOffset
    val suffix = meta.streamSuffix
    val log = new ElasticLogFileRecords(sm.loadOrCreateSlice("log" + suffix, meta.log), baseOffset, meta.logSize())
    val lastTimeIndexEntry = meta.timeIndexLastEntry().toTimestampOffset
    val timeIndex = new ElasticTimeIndex(UnifiedLog.timeIndexFile(dir, baseOffset, suffix), new DefaultStreamSliceSupplier(sm, "tim" + suffix, meta.time), baseOffset, logConfig.maxIndexSize, lastTimeIndexEntry, TimeCache)
    val txnIndex = new ElasticTransactionIndex(UnifiedLog.transactionIndexFile(dir, baseOffset, suffix), new DefaultStreamSliceSupplier(sm, "txn" + suffix, meta.txn), baseOffset, TxnCache)

    new ElasticLogSegment(meta, log, timeIndex, txnIndex, baseOffset, logConfig.indexInterval, logConfig.segmentJitterMs, time, segmentEventListener, logIdent)
  }
}
