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

package kafka.server.metadata

<<<<<<< HEAD
import kafka.metrics.KafkaMetricsGroup

=======
>>>>>>> trunk
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.metrics.Gauge
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.metrics.MetricConfig
import org.apache.kafka.image.MetadataProvenance
<<<<<<< HEAD
import org.apache.kafka.image.loader.MetadataLoaderMetrics
import org.apache.kafka.server.metrics.KafkaYammerMetrics

=======
import org.apache.kafka.server.metrics.{KafkaMetricsGroup, KafkaYammerMetrics}

import java.util.Collections
>>>>>>> trunk
import java.util.concurrent.TimeUnit.NANOSECONDS

final class BrokerServerMetrics private (
  metrics: Metrics
<<<<<<< HEAD
) extends MetadataLoaderMetrics with KafkaMetricsGroup {
  import BrokerServerMetrics._

  private val batchProcessingTimeHistName = explicitMetricName("kafka.server",
    "BrokerMetadataListener",
    "MetadataBatchProcessingTimeUs",
    Map.empty)
=======
) extends AutoCloseable {
  import BrokerServerMetrics._

  private val batchProcessingTimeHistName = KafkaMetricsGroup.explicitMetricName("kafka.server",
    "BrokerMetadataListener",
    "MetadataBatchProcessingTimeUs",
    Collections.emptyMap())
>>>>>>> trunk

  /**
   * A histogram tracking the time in microseconds it took to process batches of events.
   */
  private val batchProcessingTimeHist =
    KafkaYammerMetrics.defaultRegistry().newHistogram(batchProcessingTimeHistName, true)

<<<<<<< HEAD
  private val batchSizeHistName = explicitMetricName("kafka.server",
    "BrokerMetadataListener",
    "MetadataBatchSizes",
    Map.empty)
=======
  private val batchSizeHistName = KafkaMetricsGroup.explicitMetricName("kafka.server",
    "BrokerMetadataListener",
    "MetadataBatchSizes",
    Collections.emptyMap())
>>>>>>> trunk

  /**
   * A histogram tracking the sizes of batches that we have processed.
   */
  private val batchSizeHist =
    KafkaYammerMetrics.defaultRegistry().newHistogram(batchSizeHistName, true)

  val lastAppliedImageProvenance: AtomicReference[MetadataProvenance] =
    new AtomicReference[MetadataProvenance](MetadataProvenance.EMPTY)
  val metadataLoadErrorCount: AtomicLong = new AtomicLong(0)
  val metadataApplyErrorCount: AtomicLong = new AtomicLong(0)

  val lastAppliedRecordOffsetName: MetricName = metrics.metricName(
    "last-applied-record-offset",
    metricGroupName,
    "The offset of the last record from the cluster metadata partition that was applied by the broker"
  )

  val lastAppliedRecordTimestampName: MetricName = metrics.metricName(
    "last-applied-record-timestamp",
    metricGroupName,
    "The timestamp of the last record from the cluster metadata partition that was applied by the broker"
  )

  val lastAppliedRecordLagMsName: MetricName = metrics.metricName(
    "last-applied-record-lag-ms",
    metricGroupName,
    "The difference between now and the timestamp of the last record from the cluster metadata partition that was applied by the broker"
  )

  val metadataLoadErrorCountName: MetricName = metrics.metricName(
    "metadata-load-error-count",
    metricGroupName,
    "The number of errors encountered by the BrokerMetadataListener while loading the metadata log and generating a new MetadataDelta based on it."
  )

  val metadataApplyErrorCountName: MetricName = metrics.metricName(
    "metadata-apply-error-count",
    metricGroupName,
    "The number of errors encountered by the BrokerMetadataPublisher while applying a new MetadataImage based on the latest MetadataDelta."
  )

  addMetric(metrics, lastAppliedRecordOffsetName) { _ =>
    lastAppliedImageProvenance.get.lastContainedOffset()
  }

  addMetric(metrics, lastAppliedRecordTimestampName) { _ =>
    lastAppliedImageProvenance.get.lastContainedLogTimeMs()
  }

  addMetric(metrics, lastAppliedRecordLagMsName) { now =>
    now - lastAppliedImageProvenance.get.lastContainedLogTimeMs()
  }

  addMetric(metrics, metadataLoadErrorCountName) { _ =>
    metadataLoadErrorCount.get
  }

  addMetric(metrics, metadataApplyErrorCountName) { _ =>
    metadataApplyErrorCount.get
  }

  override def close(): Unit = {
    KafkaYammerMetrics.defaultRegistry().removeMetric(batchProcessingTimeHistName)
    KafkaYammerMetrics.defaultRegistry().removeMetric(batchSizeHistName)
    List(
      lastAppliedRecordOffsetName,
      lastAppliedRecordTimestampName,
      lastAppliedRecordLagMsName,
      metadataLoadErrorCountName,
      metadataApplyErrorCountName
    ).foreach(metrics.removeMetric)
  }

<<<<<<< HEAD
  override def updateBatchProcessingTime(elapsedNs: Long): Unit =
    batchProcessingTimeHist.update(NANOSECONDS.toMicros(elapsedNs))

  override def updateBatchSize(size: Int): Unit = batchSizeHist.update(size)

  override def updateLastAppliedImageProvenance(provenance: MetadataProvenance): Unit =
    lastAppliedImageProvenance.set(provenance)

  override def lastAppliedOffset(): Long = lastAppliedImageProvenance.get().lastContainedOffset()
=======
  def updateBatchProcessingTime(elapsedNs: Long): Unit =
    batchProcessingTimeHist.update(NANOSECONDS.toMicros(elapsedNs))

  def updateBatchSize(size: Int): Unit = batchSizeHist.update(size)

  def updateLastAppliedImageProvenance(provenance: MetadataProvenance): Unit =
    lastAppliedImageProvenance.set(provenance)

  def lastAppliedOffset(): Long = lastAppliedImageProvenance.get().lastContainedOffset()
>>>>>>> trunk

  def lastAppliedTimestamp(): Long = lastAppliedImageProvenance.get().lastContainedLogTimeMs()
}


object BrokerServerMetrics {
  private val metricGroupName = "broker-metadata-metrics"

  private def addMetric[T](metrics: Metrics, name: MetricName)(func: Long => T): Unit = {
    metrics.addMetric(name, new FuncGauge(func))
  }

  private final class FuncGauge[T](func: Long => T) extends Gauge[T] {
    override def value(config: MetricConfig, now: Long): T = {
      func(now)
    }
  }

  def apply(metrics: Metrics): BrokerServerMetrics = {
    new BrokerServerMetrics(metrics)
  }
}
