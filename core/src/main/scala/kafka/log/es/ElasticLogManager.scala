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

import com.automq.elasticstream.client.DefaultClientBuilder
import kafka.log._
import kafka.log.es.ElasticLogManager.NAMESPACE
import kafka.server.{KafkaConfig, LogDirFailureChannel}
import kafka.utils.Scheduler
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Time
import com.automq.elasticstream.client.api.Client
import kafka.log.s3.S3Client

import java.io.File
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}
import scala.jdk.CollectionConverters.ConcurrentMapHasAsScala

class ElasticLogManager(val client: Client) {
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
    elasticLogs.computeIfAbsent(topicPartition, _ => ElasticLog(client, NAMESPACE, dir, config, scheduler, time, topicPartition, logDirFailureChannel,
      numRemainingSegments, maxTransactionTimeoutMs, producerStateManagerConfig, leaderEpoch))
  }

  /**
   * Delete elastic log by topic partition. Note that this method may not be called by the broker holding the partition.
   * @param topicPartition topic partition
   * @param epoch epoch of the partition
   */
  def destroyLog(topicPartition: TopicPartition, epoch: Long): Unit = {
    // Removal may have happened in partition's closure. This is a defensive work.
    elasticLogs.remove(topicPartition)
    ElasticLog.destroy(client, NAMESPACE, topicPartition, epoch)
  }

  /**
   * Remove elastic log in the map.
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
    elasticLog.newSegment(baseOffset, time, suffix)
  }

}

object ElasticLogManager {
  private val ES_ENDPOINT_PREFIX = "es://"
  private val MEMORY_ENDPOINT_PREFIX = "memory://"
  private val REDIS_ENDPOINT_PREFIX = "redis://"

  var INSTANCE: Option[ElasticLogManager] = None
  var NAMESPACE = ""

  def init(config: KafkaConfig, clusterId: String): Boolean = {
    if (!config.elasticStreamEnabled) {
      return false
    }

    // TODO: modify kafka on es repo to support SPI
    if (true) {
      val streamClient = new AlwaysSuccessClient(new S3Client());
      INSTANCE = Some(new ElasticLogManager(streamClient))
      return true
    }


    val endpoint = config.elasticStreamEndpoint
    if (endpoint == null) {
      return false
    }
    if (endpoint.startsWith(ES_ENDPOINT_PREFIX)) {
      val kvEndpoint = config.elasticStreamKvEndpoint;
      if (!kvEndpoint.startsWith(ES_ENDPOINT_PREFIX)) {
        throw new IllegalArgumentException(s"Elastic stream endpoint and kvEndpoint must be the same protocol: $endpoint $kvEndpoint")
      }
      val streamClient = new AlwaysSuccessClient(new DefaultClientBuilder()
          .endpoint(endpoint.substring(ES_ENDPOINT_PREFIX.length))
          .kvEndpoint(kvEndpoint.substring(ES_ENDPOINT_PREFIX.length))
          .build())
      INSTANCE = Some(new ElasticLogManager(streamClient))
    } else if (endpoint.startsWith(MEMORY_ENDPOINT_PREFIX)) {
      val streamClient = new AlwaysSuccessClient(new MemoryClient())
      INSTANCE = Some(new ElasticLogManager(streamClient))
    } else if (endpoint.startsWith(REDIS_ENDPOINT_PREFIX)) {
      INSTANCE = Some(new ElasticLogManager(new ElasticRedisClient(endpoint.substring(REDIS_ENDPOINT_PREFIX.length))))
    } else {
      return false
    }

    val namespace = config.elasticStreamNamespace
    NAMESPACE = if (namespace == null || namespace.isEmpty) {
      "_kafka_" + clusterId
    } else {
      namespace
    }
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
}
