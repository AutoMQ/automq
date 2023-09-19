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
import kafka.log.streamaspect.client.{ClientFactoryProxy, Context}
import kafka.log.streamaspect.utils.ExceptionUtil
import kafka.log.{LogConfig, ProducerStateManagerConfig}
import kafka.server.{BrokerServer, KafkaConfig, LogDirFailureChannel}
import kafka.utils.{Logging, Scheduler}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Time

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
                     leaderEpoch: Long): ElasticLog = {
    elasticLogs.computeIfAbsent(topicPartition, _ => {
      var elasticLog: ElasticLog = null
      ExceptionUtil.maybeRecordThrowableAndRethrow(new Runnable {
        override def run(): Unit = {
          elasticLog = ElasticLog(client, NAMESPACE, dir, config, scheduler, time, topicPartition, logDirFailureChannel,
            numRemainingSegments, maxTransactionTimeoutMs, producerStateManagerConfig, leaderEpoch)
        }
      }, s"Failed to create elastic log for $topicPartition", this)
      elasticLog
    })
  }

  /**
   * Delete elastic log by topic partition. Note that this method may not be called by the broker holding the partition.
   *
   * @param topicPartition topic partition
   * @param epoch          epoch of the partition
   */
  def destroyLog(topicPartition: TopicPartition, epoch: Long): Unit = {
    // Removal may have happened in partition's closure. This is a defensive work.
    elasticLogs.remove(topicPartition)
    try {
      ElasticLog.destroy(client, NAMESPACE, topicPartition, epoch)
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

  def shutdownNow(): Unit = {
    client match {
      case alwaysSuccessClient: AlwaysSuccessClient =>
        alwaysSuccessClient.shutdownNow()
      case _ =>
    }
  }

}

object ElasticLogManager {
  var INSTANCE: Option[ElasticLogManager] = None
  var NAMESPACE = ""

  def init(config: KafkaConfig, clusterId: String, broker: BrokerServer = null, appendWithAsyncCallbacks: Boolean = true): Boolean = {
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
    context.appendWithAsyncCallbacks = appendWithAsyncCallbacks
    INSTANCE = Some(new ElasticLogManager(ClientFactoryProxy.get(context)))
    true
  }

  def enabled(): Boolean = INSTANCE.isDefined

  def removeLog(topicPartition: TopicPartition): Unit = {
    INSTANCE.get.removeLog(topicPartition)
  }

  def destroyLog(topicPartition: TopicPartition, epoch: Long): Unit = {
    INSTANCE.get.destroyLog(topicPartition, epoch)
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
                     leaderEpoch: Long): ElasticLog = {
    INSTANCE.get.getOrCreateLog(dir, config, scheduler, time, topicPartition, logDirFailureChannel, numRemainingSegments,
      maxTransactionTimeoutMs, producerStateManagerConfig, leaderEpoch)
  }

  def newSegment(topicPartition: TopicPartition, baseOffset: Long, time: Time, fileSuffix: String): ElasticLogSegment = {
    INSTANCE.get.newSegment(topicPartition, baseOffset, time, fileSuffix)
  }

  def shutdownNow(): Unit = {
    INSTANCE.foreach(_.shutdownNow())
  }
}
