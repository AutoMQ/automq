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

import kafka.common.IndexOffsetOverflowException
import kafka.log.AbstractIndex.error
import kafka.log.{Index, IndexEntry, IndexSearchType}
import kafka.utils.{CoreUtils, Logging}
import org.apache.kafka.common.utils.{ByteBufferUnmapper, OperatingSystem}

import java.io.{File, RandomAccessFile}
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.util.concurrent.locks.{Lock, ReentrantLock}

/**
 * Implementation ref. AbstractIndex
 */
abstract class AbstractStreamIndex(_file: File, val streamSliceSupplier: StreamSliceSupplier, val baseOffset: Long, val maxIndexSize: Int = -1) extends Index {

  var stream: ElasticStreamSlice = streamSliceSupplier.get()

  protected def entrySize: Int

  protected def _warmEntries: Int = 8192 / entrySize

  protected val lock = new ReentrantLock
  private val adjustedMaxIndexSize = roundDownToExactMultiple(maxIndexSize, entrySize)

  @volatile
  protected var _maxEntries: Int = adjustedMaxIndexSize / entrySize

  @volatile
  protected var _entries: Int = (stream.nextOffset() / entrySize).toInt

  @volatile
  protected var cache: MappedByteBuffer = {
    val newlyCreated = file.createNewFile()
    val raf = new RandomAccessFile(file, "rw")
    try {
      /* pre-allocate the file if necessary */
      if (newlyCreated) {
        if (maxIndexSize < entrySize)
          throw new IllegalArgumentException("Invalid max index size: " + maxIndexSize)
        raf.setLength(adjustedMaxIndexSize)
      }

      /* memory-map the file */
      val idx = raf.getChannel.map(FileChannel.MapMode.READ_WRITE, 0, adjustedMaxIndexSize)
      /* set the position in the index for the next entry */
      idx
    } finally {
      CoreUtils.swallow(raf.close(), AbstractStreamIndex)
    }
  }

  /**
   * True iff there are no more slots available in this index
   */
  def isFull: Boolean = _entries >= _maxEntries

  // TODO: check
  def file: File = _file

  def maxEntries: Int = _maxEntries

  def entries: Int = _entries

  def length: Long = adjustedMaxIndexSize

  // TODO:
  def updateParentDir(parentDir: File): Unit = {}

  def resize(newSize: Int): Boolean = {
    // noop implementation
    true
  }

  // TODO:
  def renameTo(f: File): Unit = {}

  // TODO:
  def deleteIfExists(): Boolean = {
    close()
    true
  }

  def trimToValidSize(): Unit = {
    // noop implementation
  }

  def sizeInBytes: Int = entrySize * _entries

  def close(): Unit = {
    if (cache != null) {
      try forceUnmap()
      catch {
        case t: Throwable => error(s"Error unmapping index $file", t)
      }
      Files.deleteIfExists(file.toPath)
    }
  }

  def closeHandler(): Unit = {
    // noop implementation.
  }

  def relativeOffset(offset: Long): Int = {
    val relativeOffset = toRelative(offset)
    if (relativeOffset.isEmpty)
      throw new IndexOffsetOverflowException(s"Integer overflow for offset: $offset (${file.getAbsoluteFile})")
    relativeOffset.get
  }

  private def toRelative(offset: Long): Option[Int] = {
    val relativeOffset = offset - baseOffset
    if (relativeOffset < 0 || relativeOffset > Int.MaxValue)
      None
    else
      Some(relativeOffset.toInt)
  }

  def canAppendOffset(offset: Long): Boolean = {
    toRelative(offset).isDefined
  }

  def parseEntry(n: Int): IndexEntry

  protected def largestLowerBoundSlotFor(target: Long, searchEntity: IndexSearchType): Int =
    indexSlotRangeFor(target, searchEntity)._1

  protected def smallestUpperBoundSlotFor(target: Long, searchEntity: IndexSearchType): Int =
    indexSlotRangeFor(target, searchEntity)._2

  private def indexSlotRangeFor(target: Long, searchEntity: IndexSearchType): (Int, Int) = {
    // check if the index is empty
    if (_entries == 0)
      return (-1, -1)

    def binarySearch(begin: Int, end: Int): (Int, Int) = {
      // binary search for the entry
      var lo = begin
      var hi = end
      while (lo < hi) {
        val mid = (lo + hi + 1) >>> 1
        val found = parseEntry(mid)
        val compareResult = compareIndexEntry(found, target, searchEntity)
        if (compareResult > 0)
          hi = mid - 1
        else if (compareResult < 0)
          lo = mid
        else
          return (mid, mid)
      }
      (lo, if (lo == _entries - 1) -1 else lo + 1)
    }

    val firstHotEntry = Math.max(0, _entries - 1 - _warmEntries)
    // check if the target offset is in the warm section of the index
    if (compareIndexEntry(parseEntry(firstHotEntry), target, searchEntity) < 0) {
      return binarySearch(firstHotEntry, _entries - 1)
    }

    // check if the target offset is smaller than the least offset
    if (compareIndexEntry(parseEntry(0), target, searchEntity) > 0)
      return (-1, 0)

    binarySearch(0, firstHotEntry)
  }

  private def compareIndexEntry(indexEntry: IndexEntry, target: Long, searchEntity: IndexSearchType): Int = {
    searchEntity match {
      case IndexSearchType.KEY => java.lang.Long.compare(indexEntry.indexKey, target)
      case IndexSearchType.VALUE => java.lang.Long.compare(indexEntry.indexValue, target)
    }
  }

  protected def maybeLock[T](lock: Lock)(fun: => T): T = {
    if (OperatingSystem.IS_WINDOWS || OperatingSystem.IS_ZOS)
      lock.lock()
    try fun
    finally {
      if (OperatingSystem.IS_WINDOWS || OperatingSystem.IS_ZOS)
        lock.unlock()
    }
  }

  def roundDownToExactMultiple(number: Int, factor: Int): Int = factor * (number / factor)

  protected[log] def forceUnmap(): Unit = {
    try ByteBufferUnmapper.unmap(file.getAbsolutePath, cache)
    finally cache = null // Accessing unmapped mmap crashes JVM by SEGV so we null it out to be safe
  }
}

object AbstractStreamIndex extends Logging {
  override val loggerName: String = classOf[AbstractStreamIndex].getName
}