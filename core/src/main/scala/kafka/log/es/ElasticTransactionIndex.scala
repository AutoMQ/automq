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
import kafka.log.{AbortedTxn, TransactionIndex}
import org.apache.kafka.common.KafkaException

import java.io.{File, IOException}
import java.nio.ByteBuffer
import scala.collection.mutable


class ElasticTransactionIndex(streamSliceSupplier: StreamSliceSupplier, startOffset: Long)
  extends TransactionIndex(startOffset, _file = new File("empty")) {

  var stream: ElasticStreamSlice = streamSliceSupplier.get()

  override def append(abortedTxn: AbortedTxn): Unit = {
    lastOffset.foreach { offset =>
      if (offset >= abortedTxn.lastOffset)
        throw new IllegalArgumentException(s"The last offset of appended transactions must increase sequentially, but " +
          s"${abortedTxn.lastOffset} is not greater than current last offset $offset of index ${file.getAbsolutePath}")
    }
    lastOffset = Some(abortedTxn.lastOffset)
    stream.append(RawPayloadRecordBatch.of(abortedTxn.buffer.duplicate()))
  }

  override def flush(): Unit = {
    // TODO: await all inflight
  }

  override def file: File = new File("mock")

  // TODO:
  override def updateParentDir(parentDir: File): Unit = {
    throw new UnsupportedOperationException()
  }

  override def deleteIfExists(): Boolean = {
    close()
    // TODO: delete
    true
  }

  override def reset(): Unit = {
    stream = streamSliceSupplier.reset()
  }

  override def close(): Unit = {
    // TODO: recycle resource
  }

  override def renameTo(f: File): Unit = {
    throw new UnsupportedOperationException();
  }

  override def truncateTo(offset: Long): Unit = {
    // TODO: check
    throw new UnsupportedOperationException();
  }

  override protected def iterator(allocate: () => ByteBuffer = () => ByteBuffer.allocate(AbortedTxn.TotalSize)): Iterator[(AbortedTxn, Int)] = {
    var position = 0
    val queue: mutable.Queue[(AbortedTxn, Int)] = mutable.Queue()
    new Iterator[(AbortedTxn, Int)] {
      override def hasNext: Boolean = queue.nonEmpty || stream.nextOffset() - position >= AbortedTxn.TotalSize

      override def next(): (AbortedTxn, Int) = {
        if (queue.nonEmpty) {
          return queue.dequeue()
        }
        try {
          val records = stream.fetch(position, 1024).get().recordBatchList()
          records.forEach(recordBatch => {
            val readBuf = Unpooled.wrappedBuffer(recordBatch.rawPayload())
            val size = readBuf.readableBytes()
            while (readBuf.readableBytes() != 0) {
              val buffer = allocate()
              readBuf.readBytes(buffer)
              buffer.flip()
              val abortedTxn = new AbortedTxn(buffer)
              if (abortedTxn.version > AbortedTxn.CurrentVersion)
                throw new KafkaException(s"Unexpected aborted transaction version ${abortedTxn.version} " +
                  s"in transaction index ${file.getAbsolutePath}, current version is ${AbortedTxn.CurrentVersion}")
              val nextEntry = (abortedTxn, position)
              queue.enqueue(nextEntry)
            }
            position += size
          })
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
