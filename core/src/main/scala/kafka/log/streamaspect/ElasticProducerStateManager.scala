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

package kafka.log.streamaspect

import kafka.log._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.types.SchemaException
import org.apache.kafka.common.utils.Time

import java.io.File
import java.nio.ByteBuffer
import java.util
import java.util.concurrent.{CompletableFuture, ConcurrentSkipListMap}
import scala.collection.mutable
import scala.jdk.CollectionConverters.CollectionHasAsScala

/**
 * ElasticProducerStateManager. Temporarily, we only persist the last snapshot.
 *
 * @param snapshotsMap All valid snapshots.
 */
class ElasticProducerStateManager(
                                   override val topicPartition: TopicPartition,
                                   __logDir: File,
                                   override val maxTransactionTimeoutMs: Int,
                                   override val producerStateManagerConfig: ProducerStateManagerConfig,
                                   override val time: Time,
                                   val snapshotsMap: util.NavigableMap[java.lang.Long, ByteBuffer],
                                   val persistFun: MetaKeyValue => CompletableFuture[Void]
                                 ) extends ProducerStateManager(topicPartition, __logDir, maxTransactionTimeoutMs, producerStateManagerConfig, time) {

  this.logIdent = s"[ElasticProducerStateManager partition=$topicPartition] "

  override protected def loadSnapshots(): ConcurrentSkipListMap[java.lang.Long, SnapshotFile] = {
    val tm = new ConcurrentSkipListMap[java.lang.Long, SnapshotFile]()
    snapshotsMap.forEach { case (offset, meta) =>
      tm.put(offset, SnapshotFile(new File(s"$offset.snapshot")))
    }
    tm
  }

  override protected def loadFromSnapshot(logStartOffset: Long, currentTime: Long): Unit = {
    while (true) {
      latestSnapshotFile match {
        case Some(snapshot) =>
          try {
            info(s"Loading producer state from snapshot file '$snapshot'")
            val loadedProducers = readSnapshot(snapshot.file).filter { producerEntry => !isProducerExpired(currentTime, producerEntry) }
            loadedProducers.foreach(loadProducerEntry)
            lastSnapOffset = snapshot.offset
            lastMapOffset = lastSnapOffset
            updateOldestTxnTimestamp()
            return
          } catch {
            case e: CorruptSnapshotException =>
              warn(s"Failed to load producer snapshot from '${snapshot.file}': ${e.getMessage}")
              removeAndDeleteSnapshot(snapshot.offset)
          }
        case None =>
          lastSnapOffset = logStartOffset
          lastMapOffset = logStartOffset
          return
      }
    }
  }


  override def truncateAndReload(logStartOffset: Long, logEndOffset: Long, currentTimeMs: Long): Unit = {
    // remove all out of range snapshots
    snapshots.values().asScala.foreach { snapshot =>
      if (snapshot.offset > logEndOffset || snapshot.offset <= logStartOffset) {
        removeAndDeleteSnapshot(snapshot.offset)
      }
    }

    if (logEndOffset != mapEndOffset) {
      producers.clear()
      ongoingTxns.clear()
      updateOldestTxnTimestamp()

      // since we assume that the offset is less than or equal to the high watermark, it is
      // safe to clear the unreplicated transactions
      unreplicatedTxns.clear()
    }
    loadFromSnapshot(logStartOffset, currentTimeMs)
  }

  override def takeSnapshot(): Unit = {
    takeSnapshot0()
  }

  def takeSnapshot0(): CompletableFuture[Void] = {
    // If not a new offset, then it is not worth taking another snapshot
    if (lastMapOffset > lastSnapOffset) {
      val snapshotFile = SnapshotFile(UnifiedLog.producerSnapshotFile(_logDir, lastMapOffset))
      val start = time.hiResClockMs()
      val rst = writeSnapshot(snapshotFile.offset, producers)
      snapshots.put(snapshotFile.offset, snapshotFile)
      info(s"Wrote producer snapshot at offset $lastMapOffset with ${producers.size} producer ids in ${time.hiResClockMs() - start} ms.")


      // Update the last snap offset according to the serialized map
      lastSnapOffset = lastMapOffset
      rst
    } else {
      CompletableFuture.completedFuture(null)
    }
  }

  /**
   * Only keep the last snapshot which offset less than or equals to the recoveryPointCheckpoint
   */
  def takeSnapshotAndRemoveExpired(recoveryPointCheckpoint: Long): CompletableFuture[Void] = {
    val lastSnapshotOffset = snapshotsMap.floorKey(recoveryPointCheckpoint)
    if (lastSnapshotOffset != null) {
      val expiredSnapshotOffsets = new util.ArrayList[java.lang.Long](snapshotsMap.headMap(lastSnapshotOffset, false).keySet())
      expiredSnapshotOffsets.forEach(offset => {
        snapshotsMap.remove(offset)
        snapshots.remove(offset)
      })
    }
    takeSnapshot0()
  }

  private def writeSnapshot(offset: Long, entries: mutable.Map[Long, ProducerStateEntry]): CompletableFuture[Void] = {
    val buffer = ProducerStateManager.writeSnapshotToBuffer(entries)
    val rawSnapshot: Array[Byte] = new Array[Byte](buffer.remaining())
    buffer.get(rawSnapshot)

    snapshotsMap.put(offset, ByteBuffer.wrap(rawSnapshot))
    val meta = new ElasticPartitionProducerSnapshotsMeta(snapshotsMap)
    persistFun(MetaKeyValue.of(MetaStream.PRODUCER_SNAPSHOTS_META_KEY, meta.encode()))
  }

  private def readSnapshot(file: File): Iterable[ProducerStateEntry] = {
    val offset = LocalLog.offsetFromFile(file)
    if (!snapshotsMap.containsKey(offset)) {
      throw new CorruptSnapshotException(s"Snapshot not found")
    }

    try {
      ProducerStateManager.readSnapshotFromBuffer(snapshotsMap.get(offset).array())
    } catch {
      case e: SchemaException =>
        throw new CorruptSnapshotException(s"Snapshot failed schema validation: ${e.getMessage}")
    }
  }

  // do nothing
  override def updateParentDir(parentDir: File): Unit = {}

  override protected def removeAndDeleteSnapshot(snapshotOffset: Long): Unit = {
    deleteSnapshot(snapshotOffset)
  }

  override private[log] def removeAndMarkSnapshotForDeletion(snapshotOffset: Long): Option[SnapshotFile] = {
    deleteSnapshot(snapshotOffset)
    None
  }

  override def deleteSnapshotFile(snapshotFile: SnapshotFile): Unit = {
    deleteSnapshot(snapshotFile.offset)
  }

  private def deleteSnapshot(snapshotOffset: Long): Unit = {
    // the real deletion will happens after meta compaction.
    snapshots.remove(snapshotOffset)
    val deleted = snapshotsMap.remove(snapshotOffset)
    if (deleted != null && isDebugEnabled) {
      debug(s"Deleted producer snapshot file '$snapshotOffset' for partition $topicPartition")
    }
  }
}

object ElasticProducerStateManager {
  def apply(
             topicPartition: TopicPartition,
             logDir: File,
             maxTransactionTimeoutMs: Int,
             producerStateManagerConfig: ProducerStateManagerConfig,
             time: Time,
             snapshotMap: util.Map[java.lang.Long, ByteBuffer],
             persistFun: MetaKeyValue => CompletableFuture[Void]
           ): ElasticProducerStateManager = {
    val stateManager = new ElasticProducerStateManager(
      topicPartition,
      logDir,
      maxTransactionTimeoutMs,
      producerStateManagerConfig,
      time,
      new util.TreeMap[java.lang.Long, ByteBuffer](snapshotMap),
      persistFun
    )
    stateManager
  }
}
