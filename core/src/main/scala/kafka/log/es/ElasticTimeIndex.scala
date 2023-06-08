/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.log.es

import kafka.Kafka.trace
import kafka.log.{IndexSearchType, TimeIndex, TimestampOffset}
import kafka.utils.CoreUtils.inLock
import org.apache.kafka.common.errors.InvalidOffsetException
import org.apache.kafka.common.record.RecordBatch

import java.io.File
import java.nio.ByteBuffer

class ElasticTimeIndex(_file: File, streamSegmentSupplier: StreamSliceSupplier, baseOffset: Long, maxIndexSize: Int = -1)
  extends AbstractStreamIndex(_file, streamSegmentSupplier, baseOffset, maxIndexSize) with TimeIndex {

  @volatile private var _lastEntry = lastEntryFromIndexFile

  protected def entrySize: Int = 12

  override def isFull: Boolean = entries >= maxEntries - 1

  override def lastEntry: TimestampOffset = _lastEntry

  private def lastEntryFromIndexFile: TimestampOffset = {
    inLock(lock) {
      _entries match {
        case 0 => TimestampOffset(RecordBatch.NO_TIMESTAMP, baseOffset)
        case s => parseEntry(s - 1)
      }
    }
  }

  def entry(n: Int): TimestampOffset = {
    maybeLock(lock) {
      if (n >= _entries)
        throw new IllegalArgumentException(s"Attempt to fetch the ${n}th entry from  time index ${file.getAbsolutePath} " +
          s"which has size ${_entries}.")
      parseEntry(n)
    }
  }

  def parseEntry(n: Int): TimestampOffset = {
    val startOffset = n * entrySize
    // try get from cache
    var timestampOffset = TimestampOffset(cache.getLong(startOffset), cache.getInt(startOffset + 8))
    if (timestampOffset.timestamp == 0 && timestampOffset.offset == 0) {
      // cache missing, try read from remote and put it to cache.
      val rst = stream.fetch(startOffset, Math.min(_entries * entrySize, startOffset + 16 * 1024))
      if (rst.recordBatchList().size() == 0) {
        throw new IllegalStateException(s"fetch empty from stream $stream at offset $startOffset")
      }
      rst.recordBatchList().forEach(record => {
        cache synchronized {
          cache.position(record.baseOffset().toInt)
          cache.put(record.rawPayload())
        }
      })
    } else {
      return timestampOffset
    }
    timestampOffset = TimestampOffset(cache.getLong(startOffset), cache.getInt(startOffset + 8))
    if (timestampOffset.timestamp == 0 && timestampOffset.offset == 0) {
      throw new IllegalStateException(s"expect offset $startOffset already in cache")
    }
    timestampOffset
  }

  def maybeAppend(timestamp: Long, offset: Long, skipFullCheck: Boolean = false): Unit = {
    inLock(lock) {
      if (!skipFullCheck)
        require(!isFull, "Attempt to append to a full time index (size = " + _entries + ").")
      // We do not throw exception when the offset equals to the offset of last entry. That means we are trying
      // to insert the same time index entry as the last entry.
      // If the timestamp index entry to be inserted is the same as the last entry, we simply ignore the insertion
      // because that could happen in the following two scenarios:
      // 1. A log segment is closed.
      // 2. LogSegment.onBecomeInactiveSegment() is called when an active log segment is rolled.
      if (_entries != 0 && offset < lastEntry.offset)
        throw new InvalidOffsetException(s"Attempt to append an offset ($offset) to slot ${_entries} no larger than" +
          s" the last offset appended (${lastEntry.offset}) to ${file.getAbsolutePath}.")
      if (_entries != 0 && timestamp < lastEntry.timestamp)
        throw new IllegalStateException(s"Attempt to append a timestamp ($timestamp) to slot ${_entries} no larger" +
          s" than the last timestamp appended (${lastEntry.timestamp}) to ${file.getAbsolutePath}.")
      // We only append to the time index when the timestamp is greater than the last inserted timestamp.
      // If all the messages are in message format v0, the timestamp will always be NoTimestamp. In that case, the time
      // index will be empty.
      if (timestamp > lastEntry.timestamp) {
        trace(s"Adding index entry $timestamp => $offset to ${file.getAbsolutePath}.")

        val buffer = ByteBuffer.allocate(entrySize)
        buffer.putLong(timestamp)
        val relatedOffset = relativeOffset(offset)
        buffer.putInt(relatedOffset)
        buffer.flip()
        stream.append(RawPayloadRecordBatch.of(buffer))

        // put time index to cache
        cache.putLong(_entries * entrySize, timestamp)
        cache.putInt(_entries * entrySize + 8, relatedOffset)

        _entries += 1
        _lastEntry = TimestampOffset(timestamp, offset)
      }
    }
  }

  def lookup(targetTimestamp: Long): TimestampOffset = {
    maybeLock(lock) {
      val slot = largestLowerBoundSlotFor(targetTimestamp, IndexSearchType.KEY)
      if (slot == -1)
        TimestampOffset(RecordBatch.NO_TIMESTAMP, baseOffset)
      else
        parseEntry(slot)
    }
  }

  override def reset(): Unit = {
    stream = streamSliceSupplier.reset()
    _entries = 0
    _lastEntry = lastEntryFromIndexFile
  }

  def truncate(): Unit = {
    //TODO:
  }

  def truncateTo(offset: Long): Unit = {
    // TODO:
  }

  def sanityCheck(): Unit = {
    // noop implementation.
  }

  override def flush(): Unit = {
    // TODO: wait all in-flight append complete
  }

  def seal(): Unit = {

  }
}
