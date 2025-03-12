/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.log.streamaspect

import com.automq.stream.api.Client
import com.automq.stream.s3.Constants
import com.automq.stream.s3.metadata.ObjectUtils
import kafka.automq.AutoMQConfig
import kafka.log.UnifiedLog
import kafka.log.streamaspect.ElasticLogManager.NAMESPACE
import kafka.log.streamaspect.cache.FileCache
import kafka.log.streamaspect.client.{ClientFactoryProxy, Context}
import kafka.server.{BrokerServer, BrokerTopicStats, KafkaConfig}
import kafka.utils.Logging
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.server.util.Scheduler
import org.apache.kafka.storage.internals.log.{LogConfig, LogDirFailureChannel, LogOffsetsListener, ProducerStateManagerConfig}
import org.slf4j.LoggerFactory

import java.io.File
import java.util.concurrent.{CompletableFuture, ConcurrentHashMap}
import scala.jdk.CollectionConverters.ConcurrentMapHasAsScala

class ElasticLogManager(val client: Client, val openStreamChecker: OpenStreamChecker) extends Logging {
  this.logIdent = s"[ElasticLogManager] "
  private val elasticLogs = new ConcurrentHashMap[TopicPartition, ElasticUnifiedLog]()

  def createLog(dir: File,
    config: LogConfig,
    scheduler: Scheduler,
    time: Time,
    maxTransactionTimeoutMs: Int,
    producerStateManagerConfig: ProducerStateManagerConfig,
    brokerTopicStats: BrokerTopicStats,
    producerIdExpirationCheckIntervalMs: Int,
    logDirFailureChannel: LogDirFailureChannel,
    topicId: Option[Uuid],
    leaderEpoch: Long = 0,
  ): ElasticUnifiedLog = {
    val snapshotRead = OpenHint.isSnapshotRead
    val topicPartition = UnifiedLog.parseTopicPartitionName(dir)
    if (!snapshotRead && elasticLogs.containsKey(topicPartition)) {
      return elasticLogs.get(topicPartition)
    }
    var elasticLog: ElasticUnifiedLog = null
    // Only Partition#makeLeader will create a new log, the ReplicaManager#asyncApplyDelta will ensure the same partition
    // operate sequentially. So it's safe without lock
    // ElasticLog new is a time cost operation.
    elasticLog = ElasticUnifiedLog(
      dir,
      config,
      scheduler,
      time,
      maxTransactionTimeoutMs,
      producerStateManagerConfig,
      brokerTopicStats,
      producerIdExpirationCheckIntervalMs,
      logDirFailureChannel,
      topicId,
      leaderEpoch,
      logOffsetsListener = LogOffsetsListener.NO_OP_OFFSETS_LISTENER,
      client,
      NAMESPACE,
      openStreamChecker,
      OpenHint.isSnapshotRead
    )
    if (!snapshotRead) {
      elasticLogs.put(topicPartition, elasticLog)
    }
    elasticLog
  }

  /**
   * Delete elastic log by topic partition. Note that this method may not be called by the broker holding the partition.
   *
   * @param topicPartition topic partition
   * @param epoch          epoch of the partition
   */
  def destroyLog(topicPartition: TopicPartition, topicId: Uuid, epoch: Long): Unit = {
    // Log is removed from elasticLogs when partition closed.
    try {
      ElasticLog.destroy(client, NAMESPACE, topicPartition, topicId, epoch)
    } catch {
      case e: Throwable =>
        // Even though the elastic log failed to be destroyed, the metadata has been deleted in controllers.
        warn(s"Failed to destroy elastic log for $topicPartition. But we will ignore this exception. ", e)
    }
  }

  /**
   * Remove elastic log in the map.
   */
  def removeLog(topicPartition: TopicPartition, log: ElasticUnifiedLog): Unit = {
    elasticLogs.remove(topicPartition, log)
  }

  def startup(): Unit = {
    client.start()
  }

  def shutdownNow(): Unit = {
    client.shutdown()
  }

}

object ElasticLogManager {
  val LOGGER = LoggerFactory.getLogger(ElasticLogManager.getClass)
  var INSTANCE: Option[ElasticLogManager] = None
  var NAMESPACE = ""
  val INIT_FUTURE: CompletableFuture[Void] = new CompletableFuture[Void]()
  private var isEnabled = false

  def init(config: KafkaConfig, clusterId: String, broker: BrokerServer = null) = {
    NAMESPACE = Constants.DEFAULT_NAMESPACE + clusterId
    ObjectUtils.setNamespace(NAMESPACE)

    val endpoint = config.elasticStreamEndpoint
    if (endpoint == null) {
      throw new IllegalArgumentException("The " + AutoMQConfig.ELASTIC_STREAM_ENDPOINT_CONFIG + " must be set")
    }
    val context = new Context()
    context.config = config
    context.brokerServer = broker
    val openStreamChecker = if (broker != null) {
      new DefaultOpenStreamChecker(broker.metadataCache)
    } else {
      OpenStreamChecker.NOOP
    }
    INSTANCE = Some(new ElasticLogManager(ClientFactoryProxy.get(context), openStreamChecker))
    INSTANCE.foreach(_.startup())
    ElasticLogSegment.txnCache = new FileCache(config.logDirs.head + "/" + "txnindex-cache", 100 * 1024 * 1024)
    ElasticLogSegment.timeCache = new FileCache(config.logDirs.head + "/" + "timeindex-cache", 100 * 1024 * 1024)
    INIT_FUTURE.complete(null)
  }

  def instance(): Option[ElasticLogManager] = {
    INIT_FUTURE.get()
    INSTANCE
  }

  def enable(shouldEnable: Boolean): Unit = {
    isEnabled = shouldEnable
  }

  def enabled(): Boolean = isEnabled

  def removeLog(topicPartition: TopicPartition, log: ElasticUnifiedLog): Unit = {
    instance().get.removeLog(topicPartition, log)
  }

  def destroyLog(topicPartition: TopicPartition, topicId: Uuid, epoch: Long): Unit = {
    instance().get.destroyLog(topicPartition, topicId, epoch)
  }

  def getElasticLog(topicPartition: TopicPartition): ElasticUnifiedLog = instance().get.elasticLogs.get(topicPartition)

  def getAllElasticLogs: Iterable[ElasticUnifiedLog] = {
    if (INSTANCE.isDefined) {
      instance().get.elasticLogs.asScala.values
    } else {
      List.empty
    }
  }

  // visible for testing
  def createLog(dir: File,
    config: LogConfig,
    scheduler: Scheduler,
    time: Time,
    maxTransactionTimeoutMs: Int,
    producerStateManagerConfig: ProducerStateManagerConfig,
    brokerTopicStats: BrokerTopicStats,
    producerIdExpirationCheckIntervalMs: Int,
    logDirFailureChannel: LogDirFailureChannel,
    topicId: Option[Uuid],
    leaderEpoch: Long = 0): ElasticUnifiedLog = {
    instance().get.createLog(
      dir,
      config,
      scheduler,
      time,
      maxTransactionTimeoutMs,
      producerStateManagerConfig,
      brokerTopicStats,
      producerIdExpirationCheckIntervalMs,
      logDirFailureChannel,
      topicId,
      leaderEpoch
    )
  }

  def shutdown(): Unit = {
    INIT_FUTURE.completeExceptionally(new IllegalStateException("ElasticLogManager is shutting down"))
    INSTANCE.foreach(_.shutdownNow())
  }
}
