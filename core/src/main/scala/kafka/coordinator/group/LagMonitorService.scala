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

// AutoMQ for Kafka inject start
package kafka.coordinator.group

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.ConcurrentHashMap
import kafka.server.{KafkaConfig, MetadataCache, NetworkUtils, ReplicaManager}
import kafka.utils.Logging
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.{LogContext, Time}
import org.apache.kafka.server.metrics.KafkaMetricsGroup
import org.apache.kafka.server.util.{AsyncSender, InterBrokerAsyncSender, KafkaScheduler}

import scala.jdk.CollectionConverters._

/**
 * LagMonitorService runs on GroupCoordinator to calculate consumer lag.
 *
 * For local partitions, lag is computed synchronously in computeLag().
 * For remote partitions, commitOffset snapshot is used with AsyncSender
 * to compute lag directly in the ListOffsets response callback,
 * minimizing time skew between LEO and commitOffset.
 *
 * Use LagMonitorService.create() for production, or the constructor directly for testing.
 */
class LagMonitorService private[group] (
  val brokerId: Int,
  val groupMetadataManager: GroupMetadataManager,
  val replicaManager: ReplicaManager,
  val lagComputeIntervalMs: Long,
  remoteLeoFetcher: RemoteLeoFetcher,
  asyncSender: AsyncSender
) extends Logging {

  require(brokerId >= 0, s"brokerId must be non-negative, got: $brokerId")
  require(lagComputeIntervalMs > 0, s"lagComputeIntervalMs must be positive, got: $lagComputeIntervalMs")
  require(groupMetadataManager != null, "groupMetadataManager cannot be null")
  require(replicaManager != null, "replicaManager cannot be null")
  require(remoteLeoFetcher != null, "remoteLeoFetcher cannot be null")
  require(asyncSender != null, "asyncSender cannot be null")

  private val running = new AtomicBoolean(false)
  private val scheduler = new KafkaScheduler(1, true, "lag-monitor-")

  private val lagCache = new ConcurrentHashMap[(String, TopicPartition), Long]()

  private val metricsGroup = new KafkaMetricsGroup(this.getClass)
  private val registeredMetrics = ConcurrentHashMap.newKeySet[(String, TopicPartition)]()

  this.logIdent = s"[LagMonitorService brokerId=$brokerId] "

  def startup(): Unit = {
    if (running.compareAndSet(false, true)) {
      info("Starting up lag monitor service")
      scheduler.startup()

      scheduler.schedule(
        "compute-consumer-lag",
        () => {
          try { computeLag() }
          catch { case e: Exception => error("Error computing consumer lag", e) }
        },
        0L,
        lagComputeIntervalMs
      )
      info("Lag monitor service started")
    }
  }

  def shutdown(): Unit = {
    if (running.compareAndSet(true, false)) {
      info("Shutting down lag monitor service")

      registeredMetrics.asScala.toSet.foreach { key: (String, TopicPartition) =>
        removeMetric(key._1, key._2)
      }

      try { asyncSender.close() }
      catch {
        case e: Exception => warn("Failed to close AsyncSender", e)
      }

      scheduler.shutdown()
      info("Lag monitor service shut down")
    }
  }

  def isRunning: Boolean = running.get()

  def collectCommitOffsets(): Map[(String, TopicPartition), Long] = {
    groupMetadataManager.currentGroups.flatMap { group =>
      group.inLock {
        val state = group.currentState
        if (state != Dead && !(state == Empty && group.allOffsets.isEmpty)) {
          group.allOffsets.map { case (tp, offsetAndMetadata) =>
            (group.groupId, tp) -> offsetAndMetadata.offset
          }
        } else {
          Map.empty[(String, TopicPartition), Long]
        }
      }
    }.toMap
  }

  def fetchLocalLeos(partitions: Set[TopicPartition]): (Map[TopicPartition, Long], Set[TopicPartition]) = {
    val localLeos = scala.collection.mutable.Map[TopicPartition, Long]()
    val remotePartitions = scala.collection.mutable.Set[TopicPartition]()

    partitions.foreach { tp =>
      try {
        replicaManager.getLogEndOffset(tp) match {
          case Some(leo) => localLeos.put(tp, leo)
          case None => remotePartitions.add(tp)
        }
      } catch {
        case e: Exception =>
          warn(s"Failed to get LEO for $tp", e)
          remotePartitions.add(tp)
      }
    }

    (localLeos.toMap, remotePartitions.toSet)
  }

  def getLagCache: Map[(String, TopicPartition), Long] = lagCache.asScala.toMap

  def getRegisteredMetrics: Set[(String, TopicPartition)] = registeredMetrics.asScala.toSet

  private def ensureMetricRegistered(groupId: String, tp: TopicPartition): Unit = {
    val key = (groupId, tp)
    if (registeredMetrics.add(key)) {
      val tags = java.util.Map.of(
        "group", groupId,
        "topic", tp.topic,
        "partition", tp.partition.toString
      )
      metricsGroup.newGauge(
        "ConsumerLag",
        new java.util.function.Supplier[Long] {
          override def get(): Long = lagCache.getOrDefault(key, -1L)
        },
        tags
      )
    }
  }

  private def removeMetric(groupId: String, tp: TopicPartition): Unit = {
    val key = (groupId, tp)
    if (registeredMetrics.remove(key)) {
      lagCache.remove(key)
      val tags = java.util.Map.of(
        "group", groupId,
        "topic", tp.topic,
        "partition", tp.partition.toString
      )
      metricsGroup.removeMetric("ConsumerLag", tags)
    }
  }

  def computeLag(): Unit = {
    val commitOffsets = collectCommitOffsets()
    if (commitOffsets.isEmpty) return

    val allPartitions = commitOffsets.keys.map(_._2).toSet
    val (localLeos, remotePartitions) = fetchLocalLeos(allPartitions)

    val activeKeys = scala.collection.mutable.Set[(String, TopicPartition)]()
    commitOffsets.keys.foreach(activeKeys.add)

    computeLocalLag(commitOffsets, localLeos)

    if (remotePartitions.nonEmpty) {
      fetchRemoteLagAsync(commitOffsets, remotePartitions)
    }

    cleanupStaleMetrics(activeKeys.toSet)
  }

  private def computeLocalLag(commitOffsets: Map[(String, TopicPartition), Long],
                               localLeos: Map[TopicPartition, Long]): Unit = {
    commitOffsets.foreach { case ((groupId, tp), commitOffset) =>
      localLeos.get(tp).foreach { leo =>
        val lag = math.max(0, leo - commitOffset)
        lagCache.put((groupId, tp), lag)
        ensureMetricRegistered(groupId, tp)
      }
    }
  }

  private def fetchRemoteLagAsync(commitOffsets: Map[(String, TopicPartition), Long],
                                    remotePartitions: Set[TopicPartition]): Unit = {
    val remoteCommitOffsets = commitOffsets.filter { case ((_, tp), _) => remotePartitions.contains(tp) }
    remoteLeoFetcher.fetchLeos(remotePartitions).thenAccept { leos =>
      leos.foreach { case (tp, leo) =>
        remoteCommitOffsets.filter(_._1._2 == tp).foreach { case ((groupId, _), commitOffset) =>
          val lag = math.max(0, leo - commitOffset)
          lagCache.put((groupId, tp), lag)
          ensureMetricRegistered(groupId, tp)
        }
      }
      val failedPartitions = remotePartitions -- leos.keySet
      failedPartitions.foreach { tp =>
        remoteCommitOffsets.filter(_._1._2 == tp).foreach { case ((groupId, _), _) =>
          lagCache.remove((groupId, tp))
        }
      }
    }.exceptionally { ex =>
      warn(s"Failed to fetch remote LEOs: ${ex.getMessage}")
      null
    }
  }

  private def cleanupStaleMetrics(activeKeys: Set[(String, TopicPartition)]): Unit = {
    val staleKeys = lagCache.keySet.asScala.toSet -- activeKeys
    staleKeys.foreach { key => removeMetric(key._1, key._2) }
  }
}

object LagMonitorService {
  /**
   * Create a LagMonitorService with all dependencies.
   * Use this for production code.
   */
  def create(
    brokerId: Int,
    config: KafkaConfig,
    groupMetadataManager: GroupMetadataManager,
    replicaManager: ReplicaManager,
    metadataCache: MetadataCache,
    metrics: Metrics,
    time: Time,
    lagComputeIntervalMs: Long
  ): LagMonitorService = {
    val logContext = new LogContext(s"[AsyncSender brokerId=$brokerId] ")
    val client = NetworkUtils.buildNetworkClient("lag-fetcher", config, metrics, time, logContext)
    val sender = InterBrokerAsyncSender.create("lag-fetcher-sender", client, config.requestTimeoutMs, time)
    val fetcher = new RemoteLeoFetcher(brokerId, metadataCache, config.interBrokerListenerName, sender)
    new LagMonitorService(brokerId, groupMetadataManager, replicaManager, lagComputeIntervalMs, fetcher, sender)
  }
}
// AutoMQ for Kafka inject end
