/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.types.SchemaException
import org.apache.kafka.common.utils.Time

import java.io.File
import java.util.concurrent.ConcurrentSkipListMap
import scala.collection.mutable

/**
 * ElasticProducerStateManager. Temporarily, we only persist the last snapshot.
 * @param snapshotsMap All valid snapshots.
 */
class ElasticProducerStateManager(
    override val topicPartition: TopicPartition,
    var logDir: File,
    override val maxTransactionTimeoutMs: Int,
    override val producerStateManagerConfig: ProducerStateManagerConfig,
    override val time: Time,
    val snapshotsMap: mutable.Map[Long, ElasticPartitionProducerSnapshotMeta],
    val persistFun: ElasticPartitionProducerSnapshotMeta => Unit
)  extends ProducerStateManager(topicPartition, logDir, maxTransactionTimeoutMs, producerStateManagerConfig, time) {

    this.logIdent = s"[ElasticProducerStateManager partition=$topicPartition] "

    override protected def loadSnapshots(): ConcurrentSkipListMap[java.lang.Long, SnapshotFile] = {
        val tm = new ConcurrentSkipListMap[java.lang.Long, SnapshotFile]()
        snapshotsMap.foreach { case (offset, meta) =>
            tm.put(offset, SnapshotFile(new File(meta.fileName())))
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

    override def takeSnapshot(): Unit = {
        // If not a new offset, then it is not worth taking another snapshot
        if (lastMapOffset > lastSnapOffset) {
            val snapshotFile = SnapshotFile(UnifiedLog.producerSnapshotFile(_logDir, lastMapOffset))
            val start = time.hiResClockMs()
            writeSnapshot(snapshotFile.offset, producers)
            info(s"Wrote producer snapshot at offset $lastMapOffset with ${producers.size} producer ids in ${time.hiResClockMs() - start} ms.")

            snapshots.put(snapshotFile.offset, snapshotFile)

            // Update the last snap offset according to the serialized map
            lastSnapOffset = lastMapOffset
        }
    }

    private def writeSnapshot(offset: Long, entries: mutable.Map[Long, ProducerStateEntry]): Unit = {
        val buffer = ProducerStateManager.writeSnapshotToBuffer(entries)
        val rawSnapshot: Array[Byte] = new Array[Byte](buffer.remaining())
        buffer.get(rawSnapshot)

        val meta = new ElasticPartitionProducerSnapshotMeta(offset, rawSnapshot)
        snapshotsMap.put(offset, meta)
        persistFun(meta)
    }

    private def readSnapshot(file: File): Iterable[ProducerStateEntry] = {
        val offset = LocalLog.offsetFromFile(file)
        if (!snapshotsMap.contains(offset)) {
            throw new CorruptSnapshotException(s"Snapshot not found")
        }

        try {
            ProducerStateManager.readSnapshotFromBuffer(snapshotsMap(offset).getRawSnapshotData)
        } catch {
            case e: SchemaException =>
                throw new CorruptSnapshotException(s"Snapshot failed schema validation: ${e.getMessage}")
        }
    }

    // TODO: check. maybe need to implement this method
//    override def updateParentDir(parentDir: File): Unit = {}

    override protected def removeAndDeleteSnapshot(snapshotOffset: Long): Unit = {
        deleteSnapshot(snapshotOffset)
    }

    override private[log] def removeAndMarkSnapshotForDeletion(snapshotOffset: Long): Option[SnapshotFile] = {
        deleteSnapshot(snapshotOffset)
        None
    }

    private def deleteSnapshot(snapshotOffset: Long): Unit = {
        snapshots.remove(snapshotOffset)
        snapshotsMap.remove(snapshotOffset).foreach( snapshot => {
            snapshot.setRawSnapshotData(null)
            persistFun(snapshot)
            info(s"Deleted producer snapshot file '$snapshotOffset' for partition $topicPartition")
        })
    }
}

object ElasticProducerStateManager {
    def apply(
        topicPartition: TopicPartition,
        logDir: File,
        maxTransactionTimeoutMs: Int,
        producerStateManagerConfig: ProducerStateManagerConfig,
        time: Time,
        snapshotMap: mutable.Map[Long, ElasticPartitionProducerSnapshotMeta],
        persistFun: ElasticPartitionProducerSnapshotMeta => Unit
    ): ElasticProducerStateManager = {
        val stateManager = new ElasticProducerStateManager(
            topicPartition,
            logDir,
            maxTransactionTimeoutMs,
            producerStateManagerConfig,
            time,
            snapshotMap,
            persistFun
        )
        stateManager
    }
}
