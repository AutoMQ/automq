/**
 *
 *
 *
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

package kafka.metrics

import kafka.utils.VerifiableProperties
import org.apache.kafka.server.metrics.MetricConfigs
import org.apache.kafka.server.util.Csv

import scala.collection.Seq
import scala.jdk.CollectionConverters._

class KafkaMetricsConfig(props: VerifiableProperties) {

  /**
   * Comma-separated list of reporter types. These classes should be on the
   * classpath and will be instantiated at run-time.
   */
  val reporters: Seq[String] = Csv.parseCsvList(props.getString(MetricConfigs.KAFKA_METRICS_REPORTER_CLASSES_CONFIG,
    MetricConfigs.KAFKA_METRIC_REPORTER_CLASSES_DEFAULT)).asScala

  /**
   * The metrics polling interval (in seconds).
   */
  val pollingIntervalSecs: Int = props.getInt(MetricConfigs.KAFKA_METRICS_POLLING_INTERVAL_SECONDS_CONFIG,
    MetricConfigs.KAFKA_METRICS_POLLING_INTERVAL_SECONDS_DEFAULT)
}
