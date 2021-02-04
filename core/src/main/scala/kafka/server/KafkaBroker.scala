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

package kafka.server

import java.util
import java.util.concurrent.atomic.AtomicReference

import com.yammer.metrics.core.MetricName
import kafka.log.LogManager
import kafka.metrics.{KafkaMetricsGroup, KafkaYammerMetrics, LinuxIoMetricsCollector}
import kafka.network.SocketServer
import kafka.utils.KafkaScheduler
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.ClusterResource
import org.apache.kafka.common.internals.ClusterResourceListeners
import org.apache.kafka.common.metrics.{KafkaMetricsContext, Metrics, MetricsReporter}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.metadata.BrokerState
import org.apache.kafka.server.authorizer.Authorizer

import scala.collection.Seq
import scala.jdk.CollectionConverters._

object KafkaBroker {
  //properties for MetricsContext
  val metricsPrefix: String = "kafka.server"
  val metricsTypeName: String = "KafkaServer"
  private val KAFKA_CLUSTER_ID: String = "kafka.cluster.id"
  private val KAFKA_BROKER_ID: String = "kafka.broker.id"

  private[server] def createKafkaMetricsContext(clusterId: String, config: KafkaConfig): KafkaMetricsContext = {
    val contextLabels = new util.HashMap[String, Object]
    contextLabels.put(KAFKA_CLUSTER_ID, clusterId)
    contextLabels.put(KAFKA_BROKER_ID, config.brokerId.toString)
    contextLabels.putAll(config.originalsWithPrefix(CommonClientConfigs.METRICS_CONTEXT_PREFIX))
    val metricsContext = new KafkaMetricsContext(metricsPrefix, contextLabels)
    metricsContext
  }

  private[server] def notifyClusterListeners(clusterId: String,
                                             clusterListeners: Seq[AnyRef]): Unit = {
    val clusterResourceListeners = new ClusterResourceListeners
    clusterResourceListeners.maybeAddAll(clusterListeners.asJava)
    clusterResourceListeners.onUpdate(new ClusterResource(clusterId))
  }

  private[server] def notifyMetricsReporters(clusterId: String,
                                             config: KafkaConfig,
                                             metricsReporters: Seq[AnyRef]): Unit = {
    val metricsContext = createKafkaMetricsContext(clusterId, config)
    metricsReporters.foreach {
      case x: MetricsReporter => x.contextChange(metricsContext)
      case _ => //do nothing
    }
  }
}

trait KafkaBroker extends KafkaMetricsGroup {
  def authorizer: Option[Authorizer]
  val brokerState = new AtomicReference[BrokerState](BrokerState.NOT_RUNNING)
  def clusterId: String
  def config: KafkaConfig
  def dataPlaneRequestHandlerPool: KafkaRequestHandlerPool
  def kafkaScheduler: KafkaScheduler
  def kafkaYammerMetrics: KafkaYammerMetrics
  def logManager: LogManager
  def metrics: Metrics
  def quotaManagers: QuotaFactory.QuotaManagers
  def replicaManager: ReplicaManager
  def socketServer: SocketServer

  // For backwards compatibility, we need to keep older metrics tied
  // to their original name when this class was named `KafkaServer`
  override def metricName(name: String, metricTags: scala.collection.Map[String, String]): MetricName = {
    explicitMetricName(KafkaBroker.metricsPrefix, KafkaBroker.metricsTypeName, name, metricTags)
  }

  newGauge("BrokerState", () => brokerState.get.value())
  newGauge("ClusterId", () => clusterId)
  newGauge("yammer-metrics-count", () =>  KafkaYammerMetrics.defaultRegistry.allMetrics.size)

  private val linuxIoMetricsCollector = new LinuxIoMetricsCollector("/proc", Time.SYSTEM, logger.underlying)

  if (linuxIoMetricsCollector.usable()) {
    newGauge("linux-disk-read-bytes", () => linuxIoMetricsCollector.readBytes())
    newGauge("linux-disk-write-bytes", () => linuxIoMetricsCollector.writeBytes())
  }
}
