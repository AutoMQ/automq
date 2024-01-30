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

import ElasticLogManager.NAMESPACE
import com.automq.stream.api.Client
import com.automq.stream.s3.metadata.ObjectUtils
import kafka.log.streamaspect.cache.FileCache
import kafka.log.streamaspect.client.{ClientFactoryProxy, Context}
import kafka.log.streamaspect.utils.ExceptionUtil
import kafka.log.{LogConfig, ProducerStateManagerConfig}
import kafka.server.{BrokerServer, KafkaConfig, LogDirFailureChannel}
import kafka.utils.{Logging, Scheduler}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.{ThreadUtils, Time}
import org.apache.kafka.common.Uuid

import java.io.File
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap, ThreadPoolExecutor}
import scala.jdk.CollectionConverters.ConcurrentMapHasAsScala

class ElasticLogManager(val client: Client) extends Logging {
  this.logIdent = s"[ElasticLogManager] "
  private val elasticLogs = new ConcurrentHashMap[TopicPartition, ElasticLog]()
  private val executorService = new ThreadPoolExecutor(1, 8, 0L, java.util.concurrent.TimeUnit.MILLISECONDS,
    new java.util.concurrent.LinkedBlockingQueue[Runnable](), ThreadUtils.createThreadFactory("elastic-log-manager-%d", true))

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
          numRemainingSegments, maxTransactionTimeoutMs, producerStateManagerConfig, topicId, leaderEpoch, executorService)
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
    ElasticLogSegment.TxnCache = new FileCache(config.logDirs.head + "/" + "txnindex-cache", 100 * 1024 * 1024)
    ElasticLogSegment.TimeCache = new FileCache(config.logDirs.head + "/" + "timeindex-cache", 100 * 1024 * 1024)
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
