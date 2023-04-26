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
import kafka.server.epoch.LeaderEpochFileCache
import kafka.server.{FetchDataInfo, LogOffsetMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.{FileRecords, MemoryRecords}
import org.apache.kafka.common.utils.Time

import java.io.File
import java.nio.file.Path
import scala.jdk.CollectionConverters.IterableHasAsScala

class ElasticLogSegment(val topicPartition: TopicPartition,
                        val dataStreamSegment: ElasticStreamSegment,
                        val timeStreamSegment: ElasticStreamSegment,
                        val elasticTxnIndex: ElasticTransactionIndex,
                        private var endOffset: Long,
                        baseOffset: Long,
                        indexIntervalBytes: Int,
                        rollJitterMs: Long,
                        time: Time) extends LogSegment(
  log = null, lazyOffsetIndex = null, lazyTimeIndex = null, txnIndex = elasticTxnIndex, baseOffset, indexIntervalBytes, rollJitterMs, time) {

  // TODO: check LogCleaner
  // override def offsetIndex: OffsetIndex = lazyOffsetIndex.get
  // override def timeIndex: TimeIndex = lazyTimeIndex.get

  var isDel: Boolean = false

  var lastModifedTime: Long = time.milliseconds()

  override def shouldRoll(rollParams: RollParams): Boolean = {
    val reachedRollMs = timeWaitedForRoll(rollParams.now, rollParams.maxTimestampInMessages) > rollParams.maxSegmentMs - rollJitterMs

    // roll based on segment record count
    (rollParams.maxOffsetInMessages - baseOffset > Int.MaxValue.toLong) ||
      // roll based on time
      (size > 0 && reachedRollMs)
  }


  override def resizeIndexes(size: Int): Unit = {
    // noop implementation, not expect invoked
    val ex = new UnsupportedOperationException()
    error("not expect invoked [resizeIndexes]", ex)
    throw ex
  }

  override def sanityCheck(timeIndexFileNewlyCreated: Boolean): Unit = {
    // noop implementation, don't expect invoked
    val ex = new UnsupportedOperationException()
    error("not expect invoked [sanityCheck]", ex)
    throw ex
  }

  // TODO: impl timeIndex


  override def size: Int = {
    // use logic offset diff to represent segment size cause of cannot cal segment bytes size without iterate all RecordBatch.
    //TODO: check all size usage
    // - LogCleaner#groupSegmentsBySize: use size to get a group cleanable segment => need optimize
    // - others...
    (endOffset - baseOffset).toInt
  }


  override def canConvertToRelativeOffset(offset: Long): Boolean = {
    val relativeOffset = offset - baseOffset
    if (relativeOffset < 0 || relativeOffset > Int.MaxValue)
      false
    else
      true
  }

  override def append(largestOffset: Long, largestTimestamp: Long, shallowOffsetOfMaxTimestamp: Long, records: MemoryRecords): Unit = {
    if (records.sizeInBytes > 0) {
      trace(s"Inserting ${records.sizeInBytes} bytes at end offset $largestOffset at position ${log.sizeInBytes} " +
        s"with largest timestamp $largestTimestamp at shallow offset $shallowOffsetOfMaxTimestamp")
      if (size == 0) {
        //TODO: check rollingBasedTimestamp usage
        rollingBasedTimestamp = Some(largestTimestamp)
      }

      ensureOffsetInRange(largestOffset)

      // append the messages
      for (batch <- records.batches.asScala) {
        // TODO: ack should wait append complete
        // when compact, the logic offset will have hollow, so there is no need to check offset successive.
        endOffset = largestOffset + 1
        dataStreamSegment.append(new RecordBatchWrapper(batch))
        // TODO: timeindex insert
        // TODO: txnindex insert
      }
      trace(s"Appended to ${log.file} at end offset $largestOffset")

      // Update the in memory max timestamp and corresponding offset.
      //      if (largestTimestamp > maxTimestampSoFar) {
      //        maxTimestampAndOffsetSoFar = TimestampOffset(largestTimestamp, shallowOffsetOfMaxTimestamp)
      //      }
      // append an entry to the index (if needed)
      if (bytesSinceLastIndexEntry > indexIntervalBytes) {
        // TODO: compacted segment append, offset is not successive, need index stream segment support
        //        timeIndex.maybeAppend(maxTimestampSoFar, offsetOfMaxTimestampSoFar)
        bytesSinceLastIndexEntry = 0
      }
      bytesSinceLastIndexEntry += records.sizeInBytes
    }
  }

  // LogSegment#appendChunkFromFile is only invoked by #appendFromFile

  override def appendFromFile(records: FileRecords, start: Int): Int = {
    // noop implementation, don't expect invoked
    val ex = new UnsupportedOperationException()
    error("not expect invoked [appendFromFile]", ex)
    throw ex
  }

  // LogSegment#updateTxnIndex no need to change

  // LogSegment#updateProducerState no need to change

  //TODO: check #translateOffset
  // - UnifiedLog#rebuildProducerState use it

  override def read(startOffset: Long, maxSize: Int, maxPosition: Long, minOneMessage: Boolean): FetchDataInfo = {
    if (maxSize < 0)
      throw new IllegalArgumentException(s"Invalid max size $maxSize for log read from segment $log")

    //TODO: check LogOffsetMetadata.relativePositionInSegment usage
    val offsetMetadata = LogOffsetMetadata(startOffset, this.baseOffset, (startOffset - baseOffset).toInt)

    if (maxSize == 0 && !minOneMessage) {
      return FetchDataInfo(offsetMetadata, MemoryRecords.EMPTY)
    }

    val rst = dataStreamSegment.fetch(startOffset, maxSize).get()
    val compositeByteBuf = Unpooled.compositeBuffer();
    // TODO: compacted segment read, offset is not successive
    rst.recordBatchList().forEach(record => {
      compositeByteBuf.addComponent(true, Unpooled.wrappedBuffer(record.rawPayload()))
    })
    new FetchDataInfo(offsetMetadata, MemoryRecords.readableRecords(compositeByteBuf.nioBuffer()))
  }

  // TODO: remove use of LogSegment#fetchUpperBoundOffset
  // - change addAbortedTransactions to get upperBoundOffset by another way

  override def recover(producerStateManager: ProducerStateManager, leaderEpochCache: Option[LeaderEpochFileCache]): Int = {
    // noop implementation, don't expect invoked
    val ex = new UnsupportedOperationException()
    error("not expect invoked [recover]", ex)
    throw ex
  }

  // LogSegment#loadLargestTimestamp is only invoked by #truncateTo

  // LogSegment#hasOverflow is only invoked by #truncateTo #splitOverflowedSegment

  override def truncateTo(offset: Long): Int = {
    // noop implementation, don't expect invoked
    val ex = new UnsupportedOperationException()
    error("not expect invoked [truncateTo]", ex)
    throw ex
  }

  override def flush(): Unit = {
    //TODO: await all stream segment async append complete
  }

  override def updateParentDir(dir: File): Unit = {
    // noop implementation, don't expect invoked
    val ex = new UnsupportedOperationException()
    error("not expect invoked [updateParentDir]", ex)
    throw ex
  }

  override def changeFileSuffixes(oldSuffix: String, newSuffix: String): Unit = {
    //TODO: impl, segment replace?
    // noop implementation, don't expect invoked
    val ex = new UnsupportedOperationException()
    error("not expect invoked [changeFileSuffixes]", ex)
    throw ex
  }

  override def hasSuffix(suffix: String): Boolean = {
    //TODO: impl
    false
  }

  override def onBecomeInactiveSegment(): Unit = {
    // TODO: insert timeIndex end; save meta?
  }

  override protected def loadFirstBatchTimestamp(): Unit = {
    val rst = dataStreamSegment.fetch(baseOffset, 1).get();
    if (rst.recordBatchList().size() > 0) {
      val firstBatch = rst.recordBatchList().get(0)
      rollingBasedTimestamp = Some(firstBatch.baseTimestamp())
    }
  }

  override def findOffsetByTimestamp(timestamp: Long, startingOffset: Long): Option[FileRecords.TimestampAndOffset] = {
    // TODO: impl by timeIndex or stream support?
    None
  }

  override def close(): Unit = {
    // TODO: recycle resource
  }

  override def closeHandlers(): Unit = {
    // empty implementation
  }

  override def deleteIfExists(): Unit = {
    // TODO: recycle resource, and remove segment meta from log meta
    isDel = true
  }

  override def deleted(): Boolean = {
    isDel
  }

  override def lastModified: Long = lastModifedTime

  // LogSegment#largestRecordTimestamp no need to change (rely on TimeIndex)

  // LogSegment#largestTimestamp no need to change (rely on TimeIndex)

  override def lastModified_=(ms: Long): Path = {
    lastModifedTime = ms
    // TODO: save meta?
    null
  }
}
