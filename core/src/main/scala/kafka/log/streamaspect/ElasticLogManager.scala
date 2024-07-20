/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.log.streamaspect

import com.automq.stream.api.Client
import com.automq.stream.s3.metadata.ObjectUtils
import kafka.log.UnifiedLog
import kafka.log.streamaspect.ElasticLogManager.NAMESPACE
import kafka.log.streamaspect.cache.FileCache
import kafka.log.streamaspect.client.{ClientFactoryProxy, Context}
import kafka.server.{BrokerServer, BrokerTopicStats, KafkaConfig}
import kafka.utils.Logging
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.server.util.Scheduler
import org.apache.kafka.storage.internals.log.{LogConfig, LogDirFailureChannel, LogOffsetsListener, ProducerStateManagerConfig}

import java.io.File
import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters.ConcurrentMapHasAsScala

class ElasticLogManager(val client: Client, val openStreamChecker: OpenStreamChecker) extends Logging {
    this.logIdent = s"[ElasticLogManager] "
    private val elasticLogs = new ConcurrentHashMap[TopicPartition, ElasticUnifiedLog]()

    def getOrCreateLog(dir: File,
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
        val topicPartition = UnifiedLog.parseTopicPartitionName(dir)
        val log = elasticLogs.get(topicPartition)
        if (log != null) {
            return log
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
            openStreamChecker
        )
        elasticLogs.put(topicPartition, elasticLog)
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
     *
     * @param topicPartition topic partition
     */
    def removeLog(topicPartition: TopicPartition): Unit = {
        elasticLogs.remove(topicPartition)
    }

    def startup(): Unit = {
        client.start()
    }

    def shutdownNow(): Unit = {
        client.shutdown()
    }

}

object ElasticLogManager {
    var INSTANCE: Option[ElasticLogManager] = None
    var NAMESPACE = ""
    private var isEnabled = false

    def init(config: KafkaConfig, clusterId: String, broker: BrokerServer = null): Boolean = {
        if (!config.elasticStreamEnabled) {
            return false
        }

        val namespace = config.elasticStreamNamespace
        NAMESPACE = if (namespace == null || namespace.isEmpty) {
            "_kafka_" + clusterId
        } else {
            namespace
        }
        ObjectUtils.setNamespace(NAMESPACE)

        val endpoint = config.elasticStreamEndpoint
        if (endpoint == null) {
            return false
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
        true
    }

    def enable(shouldEnable: Boolean): Unit = {
        isEnabled = shouldEnable
    }

    def enabled(): Boolean = isEnabled

    def removeLog(topicPartition: TopicPartition): Unit = {
        INSTANCE.get.removeLog(topicPartition)
    }

    def destroyLog(topicPartition: TopicPartition, topicId: Uuid, epoch: Long): Unit = {
        INSTANCE.get.destroyLog(topicPartition, topicId, epoch)
    }

    def getElasticLog(topicPartition: TopicPartition): ElasticUnifiedLog = INSTANCE.get.elasticLogs.get(topicPartition)

    def getAllElasticLogs: Iterable[ElasticUnifiedLog] = INSTANCE.get.elasticLogs.asScala.values

    // visible for testing
    def getOrCreateLog(dir: File,
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
        INSTANCE.get.getOrCreateLog(
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
        INSTANCE.foreach(_.shutdownNow())
    }
}
