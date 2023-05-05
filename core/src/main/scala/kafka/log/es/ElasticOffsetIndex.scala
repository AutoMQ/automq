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

import kafka.log.OffsetIndex.{debug, trace}
import kafka.log.{IndexSearchType, OffsetIndex, OffsetPosition}
import kafka.utils.CoreUtils.inLock
import org.apache.kafka.common.errors.InvalidOffsetException

import java.nio.ByteBuffer

class ElasticOffsetIndex(streamSegment: ElasticStreamSegment, baseOffset: Long, maxIndexSize: Int = -1) extends AbstractStreamIndex(streamSegment, baseOffset, maxIndexSize) with OffsetIndex {
  override def entrySize: Int = 8

  /* the last offset in the index */
  private[this] var _lastOffset = lastEntry.offset

  debug(s"Loaded index file ${file.getAbsolutePath} with maxEntries = $maxEntries, " +
    s"maxIndexSize = $maxIndexSize, entries = ${_entries}, lastOffset = ${_lastOffset}, file position = ${streamSegment.nextOffset()}")

  private def lastEntry: OffsetPosition = {
    inLock(lock) {
      _entries match {
        case 0 => OffsetPosition(baseOffset, 0)
        case s => parseEntry(s - 1)
      }
    }
  }

  def lastOffset: Long = _lastOffset

  def lookup(targetOffset: Long): OffsetPosition = {
    maybeLock(lock) {
      val slot = largestLowerBoundSlotFor(targetOffset, IndexSearchType.KEY)
      if (slot == -1)
        OffsetPosition(baseOffset, 0)
      else
        parseEntry(slot)
    }
  }

  def fetchUpperBoundOffset(fetchOffset: OffsetPosition, fetchSize: Int): Option[OffsetPosition] = {
    maybeLock(lock) {
      val slot = smallestUpperBoundSlotFor(fetchOffset.position + fetchSize, IndexSearchType.VALUE)
      if (slot == -1)
        None
      else
        Some(parseEntry(slot))
    }
  }

  def parseEntry(n: Int): OffsetPosition = {
    val rst = stream.fetch(n * entrySize, entrySize).get()
    if (rst.recordBatchList().size() == 0) {
      throw new IllegalStateException(s"fetch empty from stream ${streamSegment} at offset ${n * entrySize}")
    }
    val buffer = rst.recordBatchList().get(0).rawPayload()
    OffsetPosition(baseOffset + buffer.getInt(0), buffer.getInt(4))
  }

  def entry(n: Int): OffsetPosition = {
    maybeLock(lock) {
      if (n >= _entries)
        throw new IllegalArgumentException(s"Attempt to fetch the ${n}th entry from index ${file.getAbsolutePath}, " +
          s"which has size ${_entries}.")
      parseEntry(n)
    }
  }

  def append(offset: Long, position: Int): Unit = {
    inLock(lock) {
      require(!isFull, "Attempt to append to a full index (size = " + _entries + ").")
      if (_entries == 0 || offset > _lastOffset) {
        trace(s"Adding index entry $offset => $position to ${file.getAbsolutePath}")

        val buffer = ByteBuffer.allocate(8)
        buffer.putInt(relativeOffset(offset))
        buffer.putInt(position)
        buffer.flip()
        streamSegment.append(RawPayloadRecordBatch.of(buffer))

        _entries += 1
        _lastOffset = offset
      } else {
        throw new InvalidOffsetException(s"Attempt to append an offset ($offset) to position $entries no larger than" +
          s" the last offset appended (${_lastOffset}) to ${file.getAbsolutePath}.")
      }
    }
  }

  override def truncate(): Unit = {
    //TODO:
  }

  override def truncateTo(offset: Long): Unit = {
    //TODO:
  }

  override def sanityCheck(): Unit = {
    // noop implementation.
  }

  override def flush(): Unit = {
    // TODO: wait all in-flight append complete
  }
}
