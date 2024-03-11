/*
 * Copyright 2024, AutoMQ CO.,LTD.
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
import kafka.log.streamaspect.ElasticLogManager.NAMESPACE
import kafka.log.streamaspect.cache.FileCache
import kafka.log.streamaspect.client.{ClientFactoryProxy, Context}
import kafka.log.streamaspect.utils.ExceptionUtil
import kafka.server.{BrokerServer, KafkaConfig}
import kafka.utils.Logging
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.server.util.Scheduler
import org.apache.kafka.storage.internals.log.{LogConfig, LogDirFailureChannel, ProducerStateManagerConfig}

import java.io.File
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}
import scala.jdk.CollectionConverters.ConcurrentMapHasAsScala

class ElasticLogManager(val client: Client) extends Logging {
    this.logIdent = s"[ElasticLogManager] "
    private val elasticLogs = new ConcurrentHashMap[TopicPartition, ElasticLog]()

    def getOrCreateLog(dir: File,
        config: LogConfig,
        scheduler: Scheduler,
        time: Time,
        topicPartition: TopicPartition,
        logDirFailureChannel: LogDirFailureChannel,
        numRemainingSegments: ConcurrentMap[String, Int] = new ConcurrentHashMap[String, Int],
        maxTransactionTimeoutMs: Int,
        producerStateManagerConfig: ProducerStateManagerConfig,
        topicId: Uuid,
        leaderEpoch: Long): ElasticLog = {
        val log = elasticLogs.get(topicPartition)
        if (log != null) {
            return log
        }
        var elasticLog: ElasticLog = null
        // Only Partition#makeLeader will create a new log, the ReplicaManager#asyncApplyDelta will ensure the same partition
        // operate sequentially. So it's safe without lock
        ExceptionUtil.maybeRecordThrowableAndRethrow(new Runnable {
            override def run(): Unit = {
                // ElasticLog new is a time cost operation.
                elasticLog = ElasticLog(client, NAMESPACE, dir, config, scheduler, time, topicPartition, logDirFailureChannel,
                    numRemainingSegments, maxTransactionTimeoutMs, producerStateManagerConfig, topicId, leaderEpoch)
            }
        }, s"Failed to create elastic log for $topicPartition", this)
        elasticLogs.putIfAbsent(topicPartition, elasticLog)
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

    /**
     * New elastic log segment.
     */
    def newSegment(topicPartition: TopicPartition, baseOffset: Long, time: Time, suffix: String): ElasticLogSegment = {
        val elasticLog = elasticLogs.get(topicPartition)
        if (elasticLog == null) {
            throw new IllegalStateException(s"Cannot find elastic log for $topicPartition")
        }

        var segment: ElasticLogSegment = null
        ExceptionUtil.maybeRecordThrowableAndRethrow(new Runnable {
            override def run(): Unit = {
                segment = elasticLog.newSegment(baseOffset, time, suffix)
            }
        }, s"Failed to create new segment for $topicPartition", this)
        segment
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
        INSTANCE = Some(new ElasticLogManager(ClientFactoryProxy.get(context)))
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

    def getElasticLog(topicPartition: TopicPartition): ElasticLog = INSTANCE.get.elasticLogs.get(topicPartition)

    def getAllElasticLogs: Iterable[ElasticLog] = INSTANCE.get.elasticLogs.asScala.values

    // visible for testing
    def getOrCreateLog(dir: File,
        config: LogConfig,
        scheduler: Scheduler,
        time: Time,
        topicPartition: TopicPartition,
        logDirFailureChannel: LogDirFailureChannel,
        maxTransactionTimeoutMs: Int,
        producerStateManagerConfig: ProducerStateManagerConfig,
        numRemainingSegments: ConcurrentMap[String, Int] = new ConcurrentHashMap[String, Int],
        topicId: Uuid,
        leaderEpoch: Long): ElasticLog = {
        INSTANCE.get.getOrCreateLog(dir, config, scheduler, time, topicPartition, logDirFailureChannel, numRemainingSegments,
            maxTransactionTimeoutMs, producerStateManagerConfig, topicId, leaderEpoch)
    }

    def newSegment(topicPartition: TopicPartition, baseOffset: Long, time: Time, fileSuffix: String): ElasticLogSegment = {
        INSTANCE.get.newSegment(topicPartition, baseOffset, time, fileSuffix)
    }

    def shutdown(): Unit = {
        INSTANCE.foreach(_.shutdownNow())
    }
}
