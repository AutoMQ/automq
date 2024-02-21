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

import io.netty.buffer.Unpooled
import kafka.log.streamaspect.cache.FileCache
import kafka.log.{AbortedTxn, TransactionIndex}
import org.apache.kafka.common.KafkaException

import java.io.{File, IOException}
import java.nio.ByteBuffer
import java.util.concurrent.CompletableFuture
import scala.collection.mutable


class ElasticTransactionIndex(_file: File, streamSliceSupplier: StreamSliceSupplier, startOffset: Long, val cache: FileCache)
  extends TransactionIndex(startOffset, _file) {

  var stream: ElasticStreamSlice = streamSliceSupplier.get()
  @volatile private var lastAppend: (Long, CompletableFuture[_]) = (stream.nextOffset(), CompletableFuture.completedFuture(null))
  private var closed = false

  /**
   * In the case of unclean shutdown, the last entry needs to be recovered from the txn index.
   */
  def loadLastOffset(): Option[Long] = {
    if (stream.nextOffset() == 0) {
      None
    } else {
      val nextOffset = stream.nextOffset()
      val record = stream.fetch(nextOffset - 1, nextOffset, Int.MaxValue).get().recordBatchList().get(0)
      val readBuf = record.rawPayload()
      val abortedTxn = new AbortedTxn(readBuf)
      Some(abortedTxn.lastOffset)
    }
  }

  override def append(abortedTxn: AbortedTxn): Unit = {
    if (closed)
      throw new IOException(s"Attempt to append to closed transaction index $file")
    lastOffset.foreach { offset =>
      if (offset >= abortedTxn.lastOffset)
        throw new IllegalArgumentException(s"The last offset of appended transactions must increase sequentially, but " +
          s"${abortedTxn.lastOffset} is not greater than current last offset $offset of index ${file.getAbsolutePath}")
    }
    lastOffset = Some(abortedTxn.lastOffset)
    val position = stream.nextOffset()
    val cf = stream.append(RawPayloadRecordBatch.of(abortedTxn.buffer.duplicate()))
    lastAppend = (stream.nextOffset(), cf)
    cache.put(_file.getPath, position, Unpooled.wrappedBuffer(abortedTxn.buffer))
  }

  override def flush(): Unit = {
    lastAppend._2.get()
  }

  override def file: File = new File("mock")

  // Deleting index is actually implemented in ElasticLogSegment.deleteIfExists. We implement it here for tests.
  override def deleteIfExists(): Boolean = {
    close()
    true
  }

  override def reset(): Unit = {
    stream = streamSliceSupplier.reset()
    lastOffset = None
  }

  override def close(): Unit = {
    closed = true
  }

  override def renameTo(f: File): Unit = {
    throw new UnsupportedOperationException()
  }

  override def truncateTo(offset: Long): Unit = {
    throw new UnsupportedOperationException()
  }

  override protected def iterator(allocate: () => ByteBuffer = () => ByteBuffer.allocate(AbortedTxn.TotalSize)): Iterator[(AbortedTxn, Int)] = {
    // await last append complete, usually the abort transaction is not frequent, so it's ok to block here.
    val (endPosition, lastAppendCf) = this.lastAppend
    lastAppendCf.get()
    var position = 0
    val queue: mutable.Queue[(AbortedTxn, Int)] = mutable.Queue()
    new Iterator[(AbortedTxn, Int)] {
      /**
       * Note that nextOffset in stream here actually represents the physical size (or position).
       */
      override def hasNext: Boolean = queue.nonEmpty || endPosition - position >= AbortedTxn.TotalSize

      override def next(): (AbortedTxn, Int) = {
        if (queue.nonEmpty) {
          val item = queue.dequeue()
          if (item._1.version > AbortedTxn.CurrentVersion)
            throw new KafkaException(s"Unexpected aborted transaction version ${item._1.version} " +
              s"in transaction index ${file.getAbsolutePath}, current version is ${AbortedTxn.CurrentVersion}")
          return item
        }
        try {
          val getLength = math.min(position + AbortedTxn.TotalSize * 128, endPosition)
          val cacheDataOpt = cache.get(_file.getPath, position, getLength.toInt)
          val buf = if (cacheDataOpt.isPresent) {
            cacheDataOpt.get()
          } else {
            val records = stream.fetch(position, getLength).get()
            val txnListBuf = Unpooled.buffer(records.recordBatchList().size() * AbortedTxn.TotalSize)
            records.recordBatchList().forEach(r => {
              txnListBuf.writeBytes(r.rawPayload())
            })
            cache.put(_file.getPath, position, txnListBuf)
            records.free()
            txnListBuf
          }
          while (buf.readableBytes() > 0) {
            val abortedTxn = new AbortedTxn(buf.slice(buf.readerIndex(), AbortedTxn.TotalSize).nioBuffer())
            queue.enqueue((abortedTxn, position))
            position += AbortedTxn.TotalSize
            buf.skipBytes(AbortedTxn.TotalSize)
          }
          queue.dequeue()
        } catch {
          case e: IOException =>
            // We received an unexpected error reading from the index file. We propagate this as an
            // UNKNOWN error to the consumer, which will cause it to retry the fetch.
            throw new KafkaException(s"Failed to read from the transaction index ${file.getAbsolutePath}", e)
        }
      }
    }
  }

  override def sanityCheck(): Unit = {
    // do nothing
  }


  def seal(): Unit = {
    stream.seal()
  }
}
