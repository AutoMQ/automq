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

import java.io.File
import java.nio.ByteBuffer

class ElasticOffsetIndex(_file: File, streamSegmentSupplier: StreamSliceSupplier, baseOffset: Long, maxIndexSize: Int = -1)
  extends AbstractStreamIndex(_file, streamSegmentSupplier, baseOffset, maxIndexSize) with OffsetIndex {

  override def entrySize: Int = 8

  /* the last offset in the index */
  private[this] var _lastOffset = lastEntry.offset

  debug(s"Loaded index file ${file.getAbsolutePath} with maxEntries = $maxEntries, " +
    s"maxIndexSize = $maxIndexSize, entries = ${_entries}, lastOffset = ${_lastOffset}, file position = ${stream.nextOffset()}")

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
    val startOffset = n * entrySize
    // try get from cache
    var offsetPosition = OffsetPosition(baseOffset + cache.getInt(startOffset), cache.getInt(startOffset + 4))
    if (offsetPosition.offset == 0 && offsetPosition.position == 0) {
      // cache missing, usually the first offset index won't be record.
      // try read from remote and put it to cache.
      val rst = stream.fetch(startOffset, Math.min(_entries * entrySize, startOffset + 16 * 1024)).get()
      if (rst.recordBatchList().size() == 0) {
        throw new IllegalStateException(s"fetch empty from stream $stream at offset $startOffset")
      }
      rst.recordBatchList().forEach(record => {
        cache synchronized {
          // replace it with position put in higher JDK version.
          cache.position(record.baseOffset().toInt)
          cache.put(record.rawPayload())
        }
      })
    } else {
      return offsetPosition
    }
    // expect offset index already in cache.
    offsetPosition = OffsetPosition(baseOffset + cache.getInt(startOffset), cache.getInt(startOffset + 4))
    if (offsetPosition.offset == 0 && offsetPosition.position == 0) {
      throw new IllegalStateException(s"expect offset $startOffset already in cache")
    }
    offsetPosition
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
        val relatedOffset = relativeOffset(offset)
        buffer.putInt(relatedOffset)
        buffer.putInt(position)
        buffer.flip()
        stream.append(RawPayloadRecordBatch.of(buffer))

        // put offset index to cache
        cache.putInt(_entries * entrySize, relatedOffset)
        cache.putInt(_entries * entrySize + 4, position)

        _entries += 1
        _lastOffset = offset
      } else {
        throw new InvalidOffsetException(s"Attempt to append an offset ($offset) to position $entries no larger than" +
          s" the last offset appended (${_lastOffset}) to ${file.getAbsolutePath}.")
      }
    }
  }

  override def reset(): Unit = {
    stream = streamSliceSupplier.reset()
    _entries = 0
    _lastOffset = lastEntry.offset
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

  def seal(): Unit = {
    stream.seal()
  }

}
